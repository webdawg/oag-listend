//! The UDP carrier: the imagination layer's datagram seam, serving the same audio.
//!
//! One datagram is one request; the reply goes back to the sender. There is no connection, no
//! retransmission, and no ordering -- which is exactly why the frame codec is reused rather than
//! reinvented: a datagram already delimits the message, and the length prefix is checked against
//! what actually arrived.
//!
//! The honest limitation: a datagram is at most [`MAX_DATAGRAM_BYTES`], so a fetch whose response
//! does not fit is answered with a rejection rather than silently truncated. Small exchanges --
//! hello, list, a generated tone, a short recording -- are this carrier's job.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{watch, Mutex, Semaphore};

use crate::protocol::{self, Request, Response};
use crate::server::ShutdownHandle;
use crate::{AudioStore, ListendError};

/// The largest UDP payload IPv4 can carry. Borrowed from the imagination layer's seam.
pub const MAX_DATAGRAM_BYTES: usize = 65_507;
/// Leave room for the 4-byte frame prefix.
pub const MAX_FRAME_BYTES: usize = MAX_DATAGRAM_BYTES - 4;
/// The largest listing page a datagram can carry: 900 hashes is about 59 KiB of body.
pub const MAX_DATAGRAM_LIST_LIMIT: usize = 900;
/// A datagram exchange is bounded like every other network read and write.
pub const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrent datagrams being handled. The same session limit every other carrier applies, so a
/// flood applies backpressure rather than spawning without bound.
pub const MAX_CONNECTIONS: usize = 64;

pub struct UdpCarrier {
    store: Arc<Mutex<AudioStore>>,
    socket: Arc<UdpSocket>,
    local_addr: std::net::SocketAddr,
    /// Requests in flight, for a bounded shutdown grace period. A datagram is handled in its own
    /// task, so this counts those tasks.
    active: Arc<AtomicUsize>,
    shutdown_tx: watch::Sender<bool>,
}

impl UdpCarrier {
    pub async fn bind_shared(
        addr: &str,
        store: Arc<Mutex<AudioStore>>,
    ) -> std::io::Result<(Self, std::net::SocketAddr)> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let local_addr = socket.local_addr()?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok((
            Self {
                store,
                socket,
                local_addr,
                active: Arc::new(AtomicUsize::new(0)),
                shutdown_tx,
            },
            local_addr,
        ))
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle::new(self.shutdown_tx.clone())
    }

    /// Requests in flight right now.
    pub fn active_sessions(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// The live request counter, so a caller can wait for in-flight datagrams to drain.
    pub fn active_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active)
    }

    /// Serve datagrams until stopped.
    ///
    /// Each datagram is handled in its own task, like every other carrier handles each connection
    /// or link. Handling inline made this the one carrier with head-of-line blocking: a publish
    /// whose mirror hung held the whole loop, and every other peer's datagram waited out the add
    /// timeout -- measured at 9.70 s before the change. The semaphore caps concurrency at the same
    /// session limit the other carriers use, so a flood applies backpressure instead of spawning
    /// without bound.
    pub async fn run(&self) -> std::io::Result<()> {
        let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                received = self.socket.recv_from(&mut buf) => {
                    let (len, peer) = match received {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    };
                    let Ok(permit) = semaphore.clone().acquire_owned().await else { break };
                    let store = Arc::clone(&self.store);
                    let socket = Arc::clone(&self.socket);
                    let active = Arc::clone(&self.active);
                    let data = buf[..len].to_vec();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _guard = crate::server::ActiveGuard::raise(&active);
                        if let Some(frame) = handle(&data, &store).await {
                            // A datagram either carries the whole response or it does not; a silent
                            // truncation would be a lie about the audio.
                            if frame.len() <= MAX_DATAGRAM_BYTES {
                                let _ = tokio::time::timeout(IO_TIMEOUT, socket.send_to(&frame, peer))
                                    .await;
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// Decode one datagram and encode the reply. `None` is never returned today -- a peer that is not
/// speaking the protocol still gets a typed rejection rather than silence.
async fn handle(datagram: &[u8], store: &Arc<Mutex<AudioStore>>) -> Option<Vec<u8>> {
    let body = match protocol::decode_frame(datagram) {
        Ok(body) => body,
        Err(e) => return Some(encode_rejection(&e.to_string())),
    };
    let request = match protocol::decode_request(body) {
        Ok(request) => request,
        Err(e) => return Some(encode_rejection(&e.to_string())),
    };

    let response = match request {
        Request::Hello => Response::Hello {
            name: crate::protocol::DAEMON_NAME.to_string(),
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            item_count: store.lock().await.len(),
        },
        Request::Ping => {
            let bytes = crate::wav::twinkle();
            let content_hash = crate::AudioItem::new(bytes.clone(), None).content_hash;
            Response::Audio {
                content_hash,
                bytes,
                cid: None,
            }
        }
        Request::List { offset, limit } => {
            let store = store.lock().await;
            let hashes = store.hashes();
            let total = hashes.len();
            let limit = crate::protocol::normalize_limit(limit, MAX_DATAGRAM_LIST_LIMIT);
            let offset = offset.min(total);
            let end = (offset + limit).min(total);
            Response::Listing {
                content_hashes: hashes[offset..end].to_vec(),
                total,
                offset,
            }
        }
        Request::Fetch { content_hash } => {
            let item = store.lock().await.serve(&content_hash).ok();
            match item {
                Some(item) => {
                    let cid = store
                        .lock()
                        .await
                        .content_id(&content_hash)
                        .map(str::to_string);
                    Response::Audio {
                        content_hash,
                        bytes: item.bytes.clone(),
                        cid,
                    }
                }
                None => Response::NotFound { content_hash },
            }
        }
        Request::Publish {
            declared_media_type,
            bytes,
        } => match crate::accept_and_mirror(store, bytes, declared_media_type).await {
            Ok(hash) => Response::Published { content_hash: hash },
            Err(ListendError::NotAudio) => Response::Rejected {
                reason: ListendError::NotAudio.to_string(),
            },
            Err(e) => Response::Rejected {
                reason: e.to_string(),
            },
        },
    };

    let frame = protocol::encode_frame(&protocol::encode_response(&response));
    if frame.len() > MAX_DATAGRAM_BYTES {
        let reason = format!(
            "response is {} bytes, larger than a {} byte datagram",
            frame.len(),
            MAX_DATAGRAM_BYTES
        );
        return Some(encode_rejection(&reason));
    }
    Some(frame)
}

fn encode_rejection(reason: &str) -> Vec<u8> {
    protocol::encode_frame(&protocol::encode_response(&Response::Rejected {
        reason: reason.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wav::{self, Sound};

    #[test]
    fn the_frame_bound_is_the_datagram_bound_minus_the_prefix() {
        // 65,507 is the largest IPv4 UDP payload; four bytes of that are the frame prefix.
        assert_eq!(MAX_FRAME_BYTES + 4, MAX_DATAGRAM_BYTES);
    }

    #[tokio::test]
    async fn a_tone_fits_and_a_long_recording_does_not() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let tone = wav::generate(1, Sound::Tone);
        let published = match handle(
            &protocol::encode_frame(&protocol::encode_request(&Request::Publish {
                declared_media_type: None,
                bytes: tone.clone(),
            })),
            &store,
        )
        .await
        .as_deref()
        {
            Some(frame) => {
                protocol::decode_response(protocol::decode_frame(frame).unwrap()).unwrap()
            }
            None => panic!("expected a reply"),
        };
        let hash = match published {
            Response::Published { content_hash } => content_hash,
            other => panic!("expected Published, got {other:?}"),
        };

        let fetched = match handle(
            &protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
                content_hash: hash.clone(),
            })),
            &store,
        )
        .await
        .as_deref()
        {
            Some(frame) => {
                protocol::decode_response(protocol::decode_frame(frame).unwrap()).unwrap()
            }
            None => panic!("expected a reply"),
        };
        assert_eq!(
            fetched,
            Response::Audio {
                content_hash: hash,
                bytes: tone,
                cid: None,
            },
            "a tone must round-trip over the datagram carrier"
        );

        // A recording too large for a datagram is refused, not truncated.
        let long = wav::generate_n(2, Sound::Tone, 40_000);
        let published = match handle(
            &protocol::encode_frame(&protocol::encode_request(&Request::Publish {
                declared_media_type: None,
                bytes: long.clone(),
            })),
            &store,
        )
        .await
        .as_deref()
        {
            Some(frame) => {
                protocol::decode_response(protocol::decode_frame(frame).unwrap()).unwrap()
            }
            None => panic!("expected a reply"),
        };
        let hash = match published {
            Response::Published { content_hash } => content_hash,
            other => panic!("expected Published, got {other:?}"),
        };
        let refused = match handle(
            &protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
                content_hash: hash,
            })),
            &store,
        )
        .await
        .as_deref()
        {
            Some(frame) => {
                protocol::decode_response(protocol::decode_frame(frame).unwrap()).unwrap()
            }
            None => panic!("expected a reply"),
        };
        match refused {
            Response::Rejected { reason } => {
                assert!(reason.contains("datagram"), "{reason}");
                assert!(
                    !reason.contains(&long.len().to_string()),
                    "no byte counts of the audio"
                );
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_audio_is_refused_over_the_datagram_carrier_too() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let reply = handle(
            &protocol::encode_frame(&protocol::encode_request(&Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: b"<html>".to_vec(),
            })),
            &store,
        )
        .await
        .unwrap();
        match protocol::decode_response(protocol::decode_frame(&reply).unwrap()).unwrap() {
            Response::Rejected { reason } => assert!(reason.contains("not audio")),
            other => panic!("expected a rejection, got {other:?}"),
        }
        assert!(store.lock().await.is_empty());
    }

    #[tokio::test]
    async fn a_datagram_that_is_not_the_protocol_gets_a_typed_rejection() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let reply = handle(b"hello, not a frame", &store).await.unwrap();
        match protocol::decode_response(protocol::decode_frame(&reply).unwrap()).unwrap() {
            Response::Rejected { .. } => {}
            other => panic!("expected a rejection, got {other:?}"),
        }
    }
}
