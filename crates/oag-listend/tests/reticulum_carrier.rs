//! The Reticulum carrier, end to end over a real link.
//!
//! Runs only with `--features reticulum`: the carrier is opt-in because the `reticulum` crate is
//! far heavier than this daemon's other dependencies. The default build and the default gate do
//! not pay for it; a separate CI job does.
//!
//! No radio is involved. The carrier speaks Reticulum over its own `TcpServer` interface, exactly
//! as the imagination layer's loopback test does, so a client and a server on one machine exercise
//! the whole path. The client here is deliberately minimal: it is not a shipped artefact, it
//! exists to prove the carrier serves the same audio surface as every other carrier.

#![cfg(feature = "reticulum")]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::reticulum::{ReticulumCarrier, APP_NAME, DEFAULT_IDENTITY_SEED, DESTINATION_NAME};
use oag_listend::{wav, AudioItem, AudioStore};
use reticulum::destination::link::{Link, LinkEvent, LinkEventData};
use reticulum::destination::{DestinationName, SingleInputDestination};
use reticulum::hash::AddressHash;
use reticulum::identity::PrivateIdentity;
use reticulum::iface::tcp_client::TcpClient;
use reticulum::transport::{AnnounceEvent, Transport as RnTransport, TransportConfig};
use tokio::sync::{broadcast, Mutex};

const ANNOUNCE_WAIT: Duration = Duration::from_secs(30);
const LINK_WAIT: Duration = Duration::from_secs(15);
const REPLY_WAIT: Duration = Duration::from_secs(15);
/// Comfortably under a Reticulum packet's plaintext capacity.
const CHUNK_SIZE: usize = 1400;

/// The destination address a server with `seed` announces.
fn server_address(seed: &str) -> AddressHash {
    SingleInputDestination::new(
        PrivateIdentity::new_from_name(seed),
        DestinationName::new(APP_NAME, DESTINATION_NAME),
    )
    .desc
    .address_hash
}

/// A minimal Reticulum client: one transport, one link, many exchanges.
struct Client {
    transport: Arc<RnTransport>,
    link: Arc<Mutex<Link>>,
    link_id: AddressHash,
    events: Mutex<broadcast::Receiver<LinkEventData>>,
}

impl Client {
    async fn connect(uplink: &str, seed: &str, target: AddressHash) -> Self {
        let identity = PrivateIdentity::new_from_name(seed);
        let transport =
            RnTransport::new(TransportConfig::new("oag-listend-test", &identity, false));
        transport
            .iface_manager()
            .lock()
            .await
            .spawn(TcpClient::new(uplink.to_string()), TcpClient::spawn);
        let announces = transport.recv_announces().await;
        // Subscribe before the link exists, or its activation event is missed.
        let events = transport.out_link_events();

        let desc = Self::wait_for_announce(&announces, target).await;
        let link = transport.link(desc).await;
        let link_id = *link.lock().await.id();

        let client = Self {
            transport: Arc::new(transport),
            link,
            link_id,
            events: Mutex::new(events),
        };
        client.wait_for_activation().await;
        client
    }

    async fn wait_for_announce(
        announces: &broadcast::Receiver<AnnounceEvent>,
        target: AddressHash,
    ) -> reticulum::destination::DestinationDesc {
        let mut announces = announces.resubscribe();
        tokio::time::timeout(ANNOUNCE_WAIT, async {
            loop {
                match announces.recv().await {
                    Ok(event) => {
                        let desc = event.destination.lock().await.desc;
                        if desc.address_hash == target {
                            return desc;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("the announce channel closed before an announce arrived")
                    }
                }
            }
        })
        .await
        .expect("the daemon must announce its destination within the bound")
    }

    /// Wait for the link to activate. A Reticulum link carries requests only once active.
    async fn wait_for_activation(&self) {
        let mut events = self.events.lock().await;
        tokio::time::timeout(LINK_WAIT, async {
            loop {
                match events.recv().await {
                    Ok(event)
                        if event.id == self.link_id
                            && matches!(event.event, LinkEvent::Activated) =>
                    {
                        return;
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        })
        .await
        .expect("the link must activate within the bound");
    }

    /// Send one frame and return the reply frame, prefix included, exactly as a TCP peer would
    /// receive it.
    async fn exchange(&self, frame: &[u8]) -> Vec<u8> {
        for chunk in frame.chunks(CHUNK_SIZE) {
            let packet = self
                .link
                .lock()
                .await
                .data_packet(chunk)
                .expect("a chunk must fit a packet");
            self.transport.send_packet(packet).await;
        }

        let mut events = self.events.lock().await;
        tokio::time::timeout(REPLY_WAIT, async {
            let mut buffer = Vec::new();
            loop {
                match events.recv().await {
                    Ok(event) if event.id == self.link_id => match event.event {
                        LinkEvent::Data(payload) => {
                            buffer.extend_from_slice(payload.as_slice());
                            if protocol::decode_frame(&buffer).is_ok() {
                                return buffer;
                            }
                        }
                        LinkEvent::Closed => panic!("the link closed before a reply arrived"),
                        LinkEvent::Activated => {}
                    },
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("link events closed before a reply arrived")
                    }
                }
            }
        })
        .await
        .expect("a reply must arrive within the bound")
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve a port")
        .local_addr()
        .expect("the reserved address")
        .port()
}

/// A Reticulum client publishes audio, fetches it back whole and verifies it, answers a ping with
/// audio, and is refused when it sends something that is not audio -- the same surface as every
/// other carrier.
#[tokio::test]
async fn reticulum_serves_the_same_audio_surface() {
    let addr = format!("127.0.0.1:{}", free_port());
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let carrier = ReticulumCarrier::bind(addr.clone(), Arc::clone(&store), DEFAULT_IDENTITY_SEED)
        .await
        .expect("bind the reticulum carrier");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let target = server_address(DEFAULT_IDENTITY_SEED);
    let client = Client::connect(&addr, "oag-listend-test-client", target).await;

    // Publish a generated tone.
    let tone = wav::generate(9, wav::Sound::Tone);
    let request = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: tone.clone(),
    }));
    let reply = client.exchange(&request).await;
    let hash = match protocol::decode_response(protocol::decode_frame(&reply).expect("a frame"))
        .expect("a response")
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(
        AudioItem::verify(&hash, &tone),
        "the hash must name the bytes"
    );

    // Fetch it back.
    let request = protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
        content_hash: hash.clone(),
    }));
    let reply = client.exchange(&request).await;
    match protocol::decode_response(protocol::decode_frame(&reply).expect("a frame"))
        .expect("a response")
    {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert_eq!(content_hash, hash);
            assert_eq!(bytes, tone, "the audio must come back whole");
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    // The musical ping answers with audio, as on every carrier.
    let request = protocol::encode_frame(&protocol::encode_request(&Request::Ping));
    let reply = client.exchange(&request).await;
    match protocol::decode_response(protocol::decode_frame(&reply).expect("a frame"))
        .expect("a response")
    {
        Response::Audio { bytes, .. } => {
            assert!(!bytes.is_empty(), "a ping must never be an empty pong");
        }
        other => panic!("expected Audio for a ping, got {other:?}"),
    }

    // Non-audio is refused.
    let request = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: b"<html>not audio</html>".to_vec(),
    }));
    let reply = client.exchange(&request).await;
    match protocol::decode_response(protocol::decode_frame(&reply).expect("a frame"))
        .expect("a response")
    {
        Response::Rejected { reason } => {
            assert!(!reason.is_empty());
            assert!(
                !reason.contains("html"),
                "a refusal must not echo the bytes"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the carrier must stop within the bound")
        .expect("join")
        .expect("run");
}
