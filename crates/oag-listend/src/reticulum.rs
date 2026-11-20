//! The Reticulum carrier: the same audio surface over a Reticulum TCP interface.
//!
//! This is the imagination `Transport` seam's last carrier, and the only one that needs an
//! external crate. It is feature-gated (`--features reticulum`) because `reticulum` brings a
//! gRPC/protobuf build and a crypto stack -- far heavier than this daemon's other dependencies.
//! The default build and the default gate do not pay for it; a separate CI job does.
//!
//! No radio is involved. Reticulum's own `TcpServer` interface carries the transport, exactly as
//! `oag-imagination-reticulum`'s loopback test does, so the carrier can be exercised end to end
//! on one machine. The audio rules are not touched: an incoming frame goes through the same
//! `crate::server::handle` every other carrier uses, so `AudioStore::accept` is the inbound rule
//! here too, and a fetch returns the same bytes and BLAKE3.

#![cfg(feature = "reticulum")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reticulum::destination::link::{LinkEvent, LinkEventData};
use reticulum::destination::{DestinationName, SingleInputDestination};
use reticulum::hash::AddressHash;
use reticulum::identity::PrivateIdentity;
use reticulum::iface::tcp_server::TcpServer;
use reticulum::transport::{Transport as RnTransport, TransportConfig};
use tokio::sync::{broadcast, watch, Mutex};

use crate::protocol;
use crate::server::handle;
use crate::AudioStore;

/// The Reticulum application name this daemon serves as.
pub const APP_NAME: &str = "oag-listend";
/// The destination name within [`APP_NAME`].
pub const DESTINATION_NAME: &str = "audio";
/// The identity seed used when the operator does not choose one. A stable identity means a stable
/// address, so peers that have been given the address can still reach the daemon after a restart.
pub const DEFAULT_IDENTITY_SEED: &str = "oag-listend";
/// How often the destination announces itself. A client that subscribes between announcements
/// would otherwise wait a full interval before the daemon is reachable.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);
/// Comfortably under a Reticulum packet's plaintext capacity once link encryption is accounted
/// for. Matches the imagination layer's own Reticulum transport.
const CHUNK_SIZE: usize = 1400;

/// Serves the audio surface over a Reticulum TCP interface at `addr`.
pub struct ReticulumCarrier {
    store: Arc<Mutex<AudioStore>>,
    transport: Arc<RnTransport>,
    destination: Arc<Mutex<SingleInputDestination>>,
    /// The address this carrier is reachable at, for logs.
    local_addr: String,
    /// This destination's Reticulum address, for logs. Computed before the identity is moved.
    address: AddressHash,
    shutdown_tx: watch::Sender<bool>,
}

impl ReticulumCarrier {
    /// Bind a Reticulum TCP interface at `addr` and register the audio destination.
    ///
    /// `identity_seed` must be stable across restarts: it derives this node's identity and
    /// therefore its address.
    pub async fn bind(
        addr: impl Into<String>,
        store: Arc<Mutex<AudioStore>>,
        identity_seed: &str,
    ) -> Result<Self, String> {
        let addr = addr.into();
        let identity = PrivateIdentity::new_from_name(identity_seed);
        let mut transport = RnTransport::new(TransportConfig::new(APP_NAME, &identity, false));
        transport.iface_manager().lock().await.spawn(
            TcpServer::new(addr.clone(), transport.iface_manager()),
            TcpServer::spawn,
        );
        let address = SingleInputDestination::new(
            PrivateIdentity::new_from_name(identity_seed),
            DestinationName::new(APP_NAME, DESTINATION_NAME),
        )
        .desc
        .address_hash;
        let destination = transport
            .add_destination(identity, DestinationName::new(APP_NAME, DESTINATION_NAME))
            .await;
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            store,
            transport: Arc::new(transport),
            destination,
            local_addr: addr,
            address,
            shutdown_tx,
        })
    }

    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }

    /// This destination's Reticulum address, as a hex string.
    pub fn address(&self) -> String {
        self.address.to_hex_string()
    }

    pub fn shutdown_handle(&self) -> crate::server::ShutdownHandle {
        crate::server::ShutdownHandle::new(self.shutdown_tx.clone())
    }

    /// Serve until the transport closes. Announces periodically, then answers every link.
    pub async fn run(&self) -> Result<(), String> {
        {
            let transport = Arc::clone(&self.transport);
            let destination = Arc::clone(&self.destination);
            tokio::spawn(async move {
                loop {
                    transport.send_announce(&destination, None).await;
                    tokio::time::sleep(ANNOUNCE_INTERVAL).await;
                }
            });
        }

        // One reassembly buffer per link. A Reticulum packet carries ~2 KiB once encrypted, so a
        // frame larger than that arrives in pieces; a frame that declares more than the frame cap
        // is dropped rather than accumulated.
        let mut buffers: HashMap<AddressHash, Vec<u8>> = HashMap::new();
        let mut events = self.transport.in_link_events();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return Ok(()),
                received = events.recv() => {
                    let LinkEventData { id, event, .. } = match received {
                        Ok(event) => event,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    };
                    self.on_link_event(id, event, &mut buffers).await;
                }
            }
        }
    }

    /// One link event: maintain the per-link buffer, answer a complete frame.
    async fn on_link_event(
        &self,
        id: AddressHash,
        event: LinkEvent,
        buffers: &mut HashMap<AddressHash, Vec<u8>>,
    ) {
        match event {
            LinkEvent::Activated => {
                buffers.insert(id, Vec::new());
            }
            LinkEvent::Closed => {
                buffers.remove(&id);
            }
            LinkEvent::Data(payload) => {
                let buffer = buffers.entry(id).or_default();
                buffer.extend_from_slice(payload.as_slice());
                if buffer.len() > protocol::MAX_FRAME_BYTES + CHUNK_SIZE {
                    // A peer declaring more than a frame can be, or dribbling without ever
                    // completing one: drop the buffer rather than grow it without bound.
                    buffers.remove(&id);
                    return;
                }
                // Answer every complete frame in the buffer, in order, and keep the remainder:
                // a peer may pack several frames into one packet, exactly as it may pipeline them
                // on a stream carrier.
                loop {
                    if buffer.len() < 4 {
                        return;
                    }
                    let len =
                        u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
                    if len > protocol::MAX_FRAME_BYTES || buffer.len() < 4 + len {
                        // Too long for the cap, or not all here yet.
                        if len > protocol::MAX_FRAME_BYTES {
                            buffers.remove(&id);
                        }
                        return;
                    }
                    let body = buffer[4..4 + len].to_vec();
                    buffer.drain(..4 + len);

                    let request = match protocol::decode_request(&body) {
                        Ok(request) => request,
                        Err(e) => {
                            // A peer that is not speaking the audio protocol gets a typed
                            // rejection rather than silence, exactly as on the stream carriers.
                            self.reply(
                                &id,
                                &protocol::encode_frame(&protocol::encode_response(
                                    &protocol::Response::Rejected {
                                        reason: e.to_string(),
                                    },
                                )),
                            )
                            .await;
                            continue;
                        }
                    };
                    let response = handle(request, &self.store).await;
                    self.reply(
                        &id,
                        &protocol::encode_frame(&protocol::encode_response(&response)),
                    )
                    .await;
                }
            }
        }
    }

    /// Send one frame back over the link that asked, chunked to a packet's capacity.
    async fn reply(&self, link_id: &AddressHash, frame: &[u8]) {
        let Some(link) = self.transport.find_in_link(link_id).await else {
            return;
        };
        for chunk in frame.chunks(CHUNK_SIZE) {
            match link.lock().await.data_packet(chunk) {
                Ok(packet) => self.transport.send_packet(packet).await,
                Err(_) => break,
            }
        }
    }
}
