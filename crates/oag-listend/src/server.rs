//! The TCP carrier: a listener, a session loop, and the audio-only frame protocol on the wire.
//!
//! This is the first carrier. It is deliberately the least interesting part of the daemon: it
//! binds, accepts, reads a frame, asks the audio surface, and writes a frame. Every audio rule
//! lives in [`crate::AudioStore`], so the next carrier (HTTP, a datagram seam, a future network)
//! is a new file and not a new set of rules.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, watch};

use crate::protocol::{
    self, Request, Response, WireError, MAX_FRAME_BYTES,
};
use crate::{AudioStore, ListendError};

/// How long a client may take to send a frame, and how long the daemon may take to answer. A
/// carrier that can block must bound the block.
pub const READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrent sessions. A listener that accepts without bound is a memory-exhaustion vector.
pub const MAX_CONNECTIONS: usize = 64;
/// How many generated sounds a fresh daemon publishes at startup.
pub const STARTUP_SOUNDS: usize = 4;

/// A cloneable way to ask a running daemon to stop. Held by whoever started it, so the server
/// itself can be moved into its accept task without losing the off switch.
#[derive(Clone)]
pub struct ShutdownHandle(watch::Sender<bool>);

impl ShutdownHandle {
    pub(crate) fn new(sender: watch::Sender<bool>) -> Self {
        Self(sender)
    }

    /// Ask every session to stop and the accept loop to return. Idempotent.
    pub fn shutdown(&self) {
        let _ = self.0.send(true);
    }
}

/// The daemon. The store is shared by `Arc`, and shutdown is a watch channel.
pub struct ListendServer {
    store: Arc<Mutex<AudioStore>>,
    listener: TcpListener,
    local_addr: std::net::SocketAddr,
    read_timeout: Duration,
    write_timeout: Duration,
    max_frame: usize,
    max_connections: usize,
    shutdown_tx: watch::Sender<bool>,
}

impl ListendServer {
    /// Bind `addr` and return the server plus the address actually bound (useful for port 0 in
    /// tests). The socket is already listening when this returns, so a caller that drops the
    /// server immediately does not race a client.
    pub async fn bind(
        addr: &str,
        store: AudioStore,
    ) -> std::io::Result<(Self, std::net::SocketAddr)> {
        Self::bind_shared(addr, Arc::new(Mutex::new(store))).await
    }

    /// Bind around an already-shared store, so two carriers (TCP and HTTP) serve the same audio
    /// rather than two private stores.
    pub async fn bind_shared(
        addr: &str,
        store: Arc<Mutex<AudioStore>>,
    ) -> std::io::Result<(Self, std::net::SocketAddr)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, _) = watch::channel(false);
        let server = Self {
            store,
            listener,
            local_addr,
            read_timeout: READ_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
            max_frame: MAX_FRAME_BYTES,
            max_connections: MAX_CONNECTIONS,
            shutdown_tx,
        };
        Ok((server, local_addr))
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// Publish a small seeded set of generated sounds, through [`crate::AudioStore::accept`].
    ///
    /// Generated sound is not trusted audio: it goes through the same inbound rule a carrier's
    /// bytes do, so the startup path cannot become a way around the rule. Returns the hashes
    /// accepted.
    pub async fn publish_generated(&self, seed: u64, count: usize) -> Vec<String> {
        let mut store = self.store.lock().await;
        crate::wav::sounds(seed, count)
            .into_iter()
            .filter_map(|bytes| store.accept(bytes, Some("audio/wav".to_string())).ok())
            .collect()
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown_tx.clone())
    }

    /// Ask every session to stop and the accept loop to return. Idempotent.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Accept until [`Self::shutdown`] is called. Returns `Ok(())` on a clean shutdown.
    pub async fn run(&self) -> std::io::Result<()> {
        let semaphore = Arc::new(Semaphore::new(self.max_connections));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                accepted = self.listener.accept() => {
                    let (stream, _peer) = match accepted {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    };
                    let Ok(permit) = semaphore.clone().acquire_owned().await else { break };
                    let store = Arc::clone(&self.store);
                    let read_timeout = self.read_timeout;
                    let write_timeout = self.write_timeout;
                    let max_frame = self.max_frame;
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = serve_session(stream, store, read_timeout, write_timeout, max_frame).await;
                    });
                }
            }
        }
        Ok(())
    }
}

/// Serve one session over any byte stream. TCP and the Unix domain socket share this loop, so a
/// new stream carrier cannot quietly get a different protocol, a different bound, or a different
/// inbound rule.
pub(crate) async fn serve_session<S>(
    mut stream: S,
    store: Arc<Mutex<AudioStore>>,
    read_timeout: Duration,
    write_timeout: Duration,
    max_frame: usize,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let body = match read_frame(&mut stream, max_frame, read_timeout).await {
            Ok(Some(body)) => body,
            Ok(None) => return Ok(()), // the client closed cleanly
            Err(e) => {
                // A malformed frame or a timeout ends the session. Nothing is written back: a
                // peer that is not speaking the audio protocol gets no channel.
                let _ = stream.shutdown().await;
                return Err(e);
            }
        };

        let request = match protocol::decode_request(&body) {
            Ok(request) => request,
            Err(WireError::BadHash) => {
                // A hash-shaped field with the wrong shape is still a protocol error; answer it
                // rather than dropping the connection, so a buggy client can be fixed.
                write_response(&mut stream, &Response::Rejected {
                    reason: WireError::BadHash.to_string(),
                }, write_timeout).await?;
                continue;
            }
            Err(e) => {
                let _ = stream.shutdown().await;
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
        };

        let response = handle(request, &store).await;
        write_response(&mut stream, &response, write_timeout).await?;
    }
}

pub(crate) async fn handle(request: Request, store: &Arc<Mutex<AudioStore>>) -> Response {
    match request {
        Request::Hello => {
            let count = store.lock().await.len();
            Response::Hello { item_count: count }
        }
        Request::List => {
            let hashes = store.lock().await.hashes();
            Response::Listing { content_hashes: hashes }
        }
        Request::Publish { declared_media_type, bytes } => {
            let mut store = store.lock().await;
            match store.accept(bytes, declared_media_type) {
                Ok(content_hash) => Response::Published { content_hash },
                // The rejection carries the rule, never the bytes that were refused.
                Err(ListendError::NotAudio) => Response::Rejected {
                    reason: ListendError::NotAudio.to_string(),
                },
                Err(e) => Response::Rejected { reason: e.to_string() },
            }
        }
        Request::Fetch { content_hash } => {
            let store = store.lock().await;
            match store.serve(&content_hash) {
                Ok(item) => Response::Audio {
                    content_hash,
                    bytes: item.bytes.clone(),
                },
                Err(_) => Response::NotFound { content_hash },
            }
        }
    }
}

/// Read one length-prefixed frame. The length is checked against `max_frame` before the body is
/// allocated, so a hostile length cannot drive an allocation.
async fn read_frame<S>(
    stream: &mut S,
    max_frame: usize,
    timeout: Duration,
) -> std::io::Result<Option<Vec<u8>>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    let first = match tokio::time::timeout(timeout, stream.read(&mut prefix[..1])).await {
        Err(_) => return Err(timeout_error("read")),
        Ok(Ok(0)) => return Ok(None),
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(e),
    };
    tokio::time::timeout(timeout, stream.read_exact(&mut prefix[first..]))
        .await
        .map_err(|_| timeout_error("read"))??;

    let len = u32::from_be_bytes(prefix) as usize;
    if len > max_frame {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            WireError::TooLong(len),
        ));
    }
    let mut body = vec![0u8; len];
    if len > 0 {
        tokio::time::timeout(timeout, stream.read_exact(&mut body))
            .await
            .map_err(|_| timeout_error("read"))??;
    }
    Ok(Some(body))
}

async fn write_response<S>(
    stream: &mut S,
    response: &Response,
    timeout: Duration,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let frame = protocol::encode_frame(&protocol::encode_response(response));
    tokio::time::timeout(timeout, stream.write_all(&frame))
        .await
        .map_err(|_| timeout_error("write"))??;
    tokio::time::timeout(timeout, stream.flush())
        .await
        .map_err(|_| timeout_error("write"))??;
    Ok(())
}

fn timeout_error(kind: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, format!("{kind} timed out"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AudioItem;

    #[tokio::test]
    async fn hello_reports_how_much_audio_is_held() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        store.lock().await.publish(AudioItem::new(vec![1], None));
        let response = handle(Request::Hello, &store).await;
        assert_eq!(response, Response::Hello { item_count: 1 });
    }

    #[tokio::test]
    async fn a_publish_is_answered_with_the_hash_of_what_was_accepted() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let wav = crate::wav::generate(1, crate::wav::Sound::Tone);
        let response = handle(
            Request::Publish { declared_media_type: Some("audio/wav".into()), bytes: wav.clone() },
            &store,
        )
        .await;
        let Response::Published { content_hash } = response else {
            panic!("expected Published, got {response:?}");
        };
        assert!(crate::AudioItem::verify(&content_hash, &wav));
        assert_eq!(store.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn a_non_audio_publish_is_rejected_and_nothing_is_stored() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let response = handle(
            Request::Publish { declared_media_type: Some("audio/wav".into()), bytes: b"<html>".to_vec() },
            &store,
        )
        .await;
        assert!(matches!(response, Response::Rejected { .. }));
        assert!(store.lock().await.is_empty());
    }

    #[tokio::test]
    async fn a_fetch_round_trips_and_an_unknown_hash_is_not_found() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let wav = crate::wav::generate(2, crate::wav::Sound::Noise);
        let Response::Published { content_hash } = handle(
            Request::Publish { declared_media_type: None, bytes: wav.clone() },
            &store,
        )
        .await else {
            panic!("expected Published");
        };
        let fetched = handle(Request::Fetch { content_hash: content_hash.clone() }, &store).await;
        match fetched {
            Response::Audio { content_hash: got, bytes } => {
                assert_eq!(got, content_hash);
                assert_eq!(bytes, wav);
            }
            other => panic!("expected Audio, got {other:?}"),
        }
        let missing = handle(Request::Fetch { content_hash: "00".repeat(32) }, &store).await;
        assert_eq!(missing, Response::NotFound { content_hash: "00".repeat(32) });
    }

    #[tokio::test]
    async fn a_listing_is_sorted_so_it_survives_a_restart() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let a = crate::wav::generate(10, crate::wav::Sound::Tone);
        let b = crate::wav::generate(11, crate::wav::Sound::Noise);
        let ha = handle(Request::Publish { declared_media_type: None, bytes: a }, &store)
            .await
            .published_hash();
        let hb = handle(Request::Publish { declared_media_type: None, bytes: b }, &store)
            .await
            .published_hash();
        let listing = handle(Request::List, &store).await;
        let mut expected = vec![ha.clone(), hb.clone()];
        expected.sort();
        assert_eq!(
            listing,
            Response::Listing { content_hashes: expected },
            "a listing must be sorted, so the walk is the same across a restart"
        );
    }

    impl Response {
        fn published_hash(&self) -> String {
            match self {
                Response::Published { content_hash } => content_hash.clone(),
                other => panic!("expected Published, got {other:?}"),
            }
        }
    }
}
