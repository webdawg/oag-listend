//! The TCP carrier: a listener, a session loop, and the audio-only frame protocol on the wire.
//!
//! This is the first carrier. It is deliberately the least interesting part of the daemon: it
//! binds, accepts, reads a frame, asks the audio surface, and writes a frame. Every audio rule
//! lives in [`crate::AudioStore`], so the next carrier (HTTP, a datagram seam, a future network)
//! is a new file and not a new set of rules.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex, Semaphore};

use crate::protocol::{self, Request, Response, WireError, MAX_FRAME_BYTES};
use crate::{AudioStore, ListendError};

/// How long a client may take to send a frame, and how long the daemon may take to answer. A
/// carrier that can block must bound the block.
pub const READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrent sessions. A listener that accepts without bound is a memory-exhaustion vector.
pub const MAX_CONNECTIONS: usize = 64;
/// A total bound on one session, not just on each read. A client that dribbles a byte every few
/// seconds would otherwise hold a session open indefinitely, and 64 of them would exhaust the
/// session cap. A session idle or slow past this is closed. Generous, because a legitimate client
/// may keep a connection for occasional fetches; still finite.
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(1800);
/// Raises an active-session counter when created and lowers it when dropped -- including during a
/// panic's unwind. Every carrier counts its sessions this way so a stop can wait for them, and a
/// hand-written `fetch_sub` after the await is skipped by a panic: the counter stays permanently
/// raised and every later shutdown waits the whole grace period for a session that no longer
/// exists. A reachable panic makes that more than theoretical.
pub(crate) struct ActiveGuard(Arc<AtomicUsize>);

impl ActiveGuard {
    pub(crate) fn raise(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(counter))
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How many seeded sounds a fresh daemon publishes at startup: one of each kind -- silence, tone,
/// noise. The canonical ping is appended separately.
pub const STARTUP_SOUNDS: usize = 3;

/// What startup generation did. `rejected` is reported rather than dropped: a generated sound
/// that the store refuses is a fact the operator should see, not a silent no-op.
#[derive(Debug, Default)]
pub struct GeneratedReport {
    pub accepted: Vec<String>,
    pub rejected: Vec<String>,
    pub skipped_non_empty: bool,
}

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
    /// Sessions currently being served, so a stop can give in-flight exchanges a brief
    /// grace period instead of cutting them off mid-transfer.
    active: Arc<AtomicUsize>,
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
            active: Arc::new(AtomicUsize::new(0)),
            shutdown_tx,
        };
        Ok((server, local_addr))
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// Sessions in flight right now.
    pub fn active_sessions(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// The live counter itself, so a caller can wait for sessions to drain.
    pub fn active_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active)
    }

    /// Publish a small seeded set of generated sounds, through [`crate::AudioStore::accept`].
    ///
    /// Generated sound is not trusted audio: it goes through the same inbound rule a carrier's
    /// bytes do, so the startup path cannot become a way around the rule.
    ///
    /// The set is a *seed for an empty store*, not a recurring addition. A restart with a
    /// persistent store would otherwise add four more sounds every time -- and with the default
    /// clock seed they would be new hashes, so the generated set would slowly evict the operator's
    /// own audio. A non-empty store is left alone.
    pub async fn publish_generated(&self, seed: u64, count: usize) -> GeneratedReport {
        {
            let store = self.store.lock().await;
            if !store.is_empty() {
                return GeneratedReport {
                    accepted: Vec::new(),
                    rejected: Vec::new(),
                    skipped_non_empty: true,
                };
            }
        }
        let mut report = GeneratedReport::default();
        let mut items = crate::wav::sounds(seed, count);
        // The canonical ping sound is part of the seed set, so it is durable and fetchable by hash
        // as well as answerable with a ping.
        items.push(crate::wav::twinkle());
        for bytes in items {
            match crate::accept_and_mirror(&self.store, bytes, Some("audio/wav".to_string())).await
            {
                Ok(hash) => report.accepted.push(hash),
                Err(e) => report.rejected.push(e.to_string()),
            }
        }
        report
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
                    let active = Arc::clone(&self.active);
                    let store = Arc::clone(&self.store);
                    let read_timeout = self.read_timeout;
                    let write_timeout = self.write_timeout;
                    let max_frame = self.max_frame;
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _active = ActiveGuard::raise(&active);
                        let _ = serve_session(
                            stream,
                            store,
                            read_timeout,
                            write_timeout,
                            max_frame,
                            SESSION_TIMEOUT,
                        )
                        .await;
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
    stream: S,
    store: Arc<Mutex<AudioStore>>,
    read_timeout: Duration,
    write_timeout: Duration,
    max_frame: usize,
    session_timeout: Duration,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(
        session_timeout,
        serve_session_inner(stream, store, read_timeout, write_timeout, max_frame),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "session timed out"))?
}

async fn serve_session_inner<S>(
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
                write_response(
                    &mut stream,
                    &Response::Rejected {
                        reason: WireError::BadHash.to_string(),
                    },
                    write_timeout,
                )
                .await?;
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
            Response::Hello {
                name: crate::protocol::DAEMON_NAME.to_string(),
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                item_count: count,
            }
        }
        Request::Ping => {
            // The ping answer is the canonical audio item, generated deterministically, so every
            // daemon answers with the same bytes and the same hash.
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
            // The page is bounded so the response always fits the frame cap.
            let limit = crate::protocol::normalize_limit(limit, crate::protocol::MAX_LIST_LIMIT);
            let offset = offset.min(total);
            let end = (offset + limit).min(total);
            Response::Listing {
                content_hashes: hashes[offset..end].to_vec(),
                total,
                offset,
            }
        }
        Request::Publish {
            declared_media_type,
            bytes,
        } => {
            // Accept under the lock, mirror after releasing it: a slow IPFS node must not stall
            // every other client behind this publish.
            match crate::accept_and_mirror(store, bytes, declared_media_type).await {
                Ok(content_hash) => Response::Published { content_hash },
                // The rejection carries the rule, never the bytes that were refused.
                Err(ListendError::NotAudio) => Response::Rejected {
                    reason: ListendError::NotAudio.to_string(),
                },
                Err(e) => Response::Rejected {
                    reason: e.to_string(),
                },
            }
        }
        Request::Fetch { content_hash } => {
            // Clone the handle under the lock, then copy the bytes after releasing it.
            let item = store.lock().await.serve(&content_hash).ok();
            match item {
                Some(item) => {
                    // Where else these bytes live, if this daemon mirrors to IPFS.
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
    let first = match tokio::time::timeout(timeout, stream.read(&mut prefix)).await {
        Err(_) => return Err(timeout_error("read")),
        Ok(Ok(0)) => return Ok(None),
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(e),
    };
    read_exact_bounded(stream, &mut prefix[first..], timeout).await?;

    let len = u32::from_be_bytes(prefix) as usize;
    if len > max_frame {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            WireError::TooLong(len),
        ));
    }
    let mut body = vec![0u8; len];
    if len > 0 {
        read_exact_bounded(stream, &mut body, timeout).await?;
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
    write_all_bounded(stream, &frame, timeout).await?;
    tokio::time::timeout(timeout, stream.flush())
        .await
        .map_err(|_| timeout_error("write"))??;
    Ok(())
}

/// Read exactly `buf.len()` bytes, bounding each read syscall rather than the whole transfer.
///
/// A single timeout around `read_exact` would cut off a legitimate large upload from a slow client
/// mid-transfer. Per-read, a stalled client is dropped after the timeout but a slow one is not.
pub(crate) async fn read_exact_bounded<S>(
    stream: &mut S,
    buf: &mut [u8],
    timeout: Duration,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut read = 0;
    while read < buf.len() {
        let n = tokio::time::timeout(timeout, stream.read(&mut buf[read..]))
            .await
            .map_err(|_| timeout_error("read"))??;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the connection closed mid-message",
            ));
        }
        read += n;
    }
    Ok(())
}

/// Write the whole buffer, bounding each write syscall rather than the whole transfer. See
/// [`read_exact_bounded`] for why.
pub(crate) async fn write_all_bounded<S>(
    stream: &mut S,
    mut buf: &[u8],
    timeout: Duration,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    while !buf.is_empty() {
        let n = tokio::time::timeout(timeout, stream.write(buf))
            .await
            .map_err(|_| timeout_error("write"))??;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "the connection accepted no more bytes",
            ));
        }
        buf = &buf[n..];
    }
    Ok(())
}

fn timeout_error(kind: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, format!("{kind} timed out"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AudioItem;

    /// A session that panics must still release its slot. Every carrier counts sessions so a stop
    /// can wait for them; a hand-written decrement after the await is skipped by a panic's unwind,
    /// which leaves the counter raised forever and makes every later shutdown wait the whole grace
    /// period for a session that no longer exists.
    #[test]
    fn a_panicking_session_still_releases_its_slot() {
        let counter = Arc::new(AtomicUsize::new(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ActiveGuard::raise(&counter);
            panic!("a session that dies badly");
        }));
        assert!(result.is_err(), "the panic must propagate");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "the slot is released during the unwind, not only on a clean return"
        );
    }

    /// The read bound is per syscall, not per transfer: 100 one-byte reads at 10 ms each take
    /// about a second, far longer than the 50 ms bound, and must still succeed.
    /// The total bound on one session is real, and it is the total that fires -- not the per-read
    /// bound underneath it. A client that connects and then says nothing must be cut off rather than
    /// hold one of the 64 slots indefinitely. The production bound is 30 minutes, so the test drives
    /// the same function with a short one and a deliberately longer read bound, then asserts the
    /// short one won: without the total bound this takes the read bound's eight seconds instead.
    #[tokio::test]
    async fn a_client_that_says_nothing_is_cut_off_at_the_session_bound() {
        let (client, server) = tokio::io::duplex(64);
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let started = std::time::Instant::now();
        let result = serve_session(
            server,
            store,
            Duration::from_secs(8),
            Duration::from_secs(8),
            crate::protocol::MAX_FRAME_BYTES,
            Duration::from_millis(50),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            result.is_err(),
            "a silent client must be cut off, not hold the slot"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the session bound (50 ms) must be what ends it, not the 8 s read bound: {elapsed:?}"
        );
        drop(client);
    }

    #[tokio::test]
    async fn a_slow_but_progressing_read_is_not_cut_off() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            for _ in 0..100 {
                client.write_all(&[7]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = client.shutdown().await;
        });
        let mut buf = vec![0u8; 100];
        super::read_exact_bounded(&mut server, &mut buf, Duration::from_millis(50))
            .await
            .expect("a slow but progressing read must succeed");
        assert!(buf.iter().all(|&b| b == 7));
        writer.await.unwrap();
    }

    /// The write bound is per syscall too.
    #[tokio::test]
    async fn a_slow_but_progressing_write_is_not_cut_off() {
        let (mut client, mut server) = tokio::io::duplex(1);
        let reader = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut b = [0u8];
            for _ in 0..100 {
                server.read_exact(&mut b).await.unwrap();
                got.push(b[0]);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            got
        });
        super::write_all_bounded(&mut client, &[9u8; 100], Duration::from_millis(50))
            .await
            .expect("a slow but progressing write must succeed");
        let got = reader.await.unwrap();
        assert!(got.iter().all(|&b| b == 9));
    }

    #[tokio::test]
    async fn hello_reports_how_much_audio_is_held() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        store.lock().await.publish(AudioItem::new(vec![1], None));
        let response = handle(Request::Hello, &store).await;
        assert_eq!(
            response,
            Response::Hello {
                name: crate::protocol::DAEMON_NAME.to_string(),
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                item_count: 1,
            }
        );
    }

    #[tokio::test]
    async fn a_publish_is_answered_with_the_hash_of_what_was_accepted() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let wav = crate::wav::generate(1, crate::wav::Sound::Tone);
        let response = handle(
            Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: wav.clone(),
            },
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
    async fn a_listing_is_a_bounded_page_with_a_total() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let mut hashes = Vec::new();
        for seed in 0..5u64 {
            let wav = crate::wav::generate(seed, crate::wav::Sound::Tone);
            hashes.push(
                handle(
                    Request::Publish {
                        declared_media_type: None,
                        bytes: wav,
                    },
                    &store,
                )
                .await
                .published_hash(),
            );
        }
        hashes.sort();

        let page = handle(
            Request::List {
                offset: 0,
                limit: 2,
            },
            &store,
        )
        .await;
        assert_eq!(
            page,
            Response::Listing {
                content_hashes: hashes[0..2].to_vec(),
                total: 5,
                offset: 0
            }
        );
        let page = handle(
            Request::List {
                offset: 4,
                limit: 2,
            },
            &store,
        )
        .await;
        assert_eq!(
            page,
            Response::Listing {
                content_hashes: hashes[4..5].to_vec(),
                total: 5,
                offset: 4
            }
        );
        let page = handle(
            Request::List {
                offset: 5,
                limit: 2,
            },
            &store,
        )
        .await;
        assert_eq!(
            page,
            Response::Listing {
                content_hashes: vec![],
                total: 5,
                offset: 5
            }
        );
        // A limit above the carrier's cap is clamped, not honoured.
        let page = handle(
            Request::List {
                offset: 0,
                limit: 100_000,
            },
            &store,
        )
        .await;
        match page {
            Response::Listing { content_hashes, .. } => {
                assert!(content_hashes.len() <= crate::protocol::MAX_LIST_LIMIT)
            }
            other => panic!("expected a listing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_non_audio_publish_is_rejected_and_nothing_is_stored() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let response = handle(
            Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: b"<html>".to_vec(),
            },
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
            Request::Publish {
                declared_media_type: None,
                bytes: wav.clone(),
            },
            &store,
        )
        .await
        else {
            panic!("expected Published");
        };
        let fetched = handle(
            Request::Fetch {
                content_hash: content_hash.clone(),
            },
            &store,
        )
        .await;
        match fetched {
            Response::Audio {
                content_hash: got,
                bytes,
                ..
            } => {
                assert_eq!(got, content_hash);
                assert_eq!(bytes, wav);
            }
            other => panic!("expected Audio, got {other:?}"),
        }
        let missing = handle(
            Request::Fetch {
                content_hash: "00".repeat(32),
            },
            &store,
        )
        .await;
        assert_eq!(
            missing,
            Response::NotFound {
                content_hash: "00".repeat(32)
            }
        );
    }

    #[tokio::test]
    async fn generation_seeds_only_an_empty_store() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let (server, _addr) = super::ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
            .await
            .unwrap();
        // Three seeded sounds (silence, tone, noise) plus the canonical ping.
        let first = server.publish_generated(1, 3).await;
        assert_eq!(first.accepted.len(), 4);
        assert!(!first.skipped_non_empty);
        assert_eq!(store.lock().await.len(), 4);

        let second = server.publish_generated(2, 4).await;
        assert!(second.accepted.is_empty());
        assert!(
            second.skipped_non_empty,
            "a non-empty store must not be added to"
        );
        assert_eq!(
            store.lock().await.len(),
            4,
            "a restart must not grow the store"
        );
    }

    #[tokio::test]
    async fn a_generated_sound_the_store_refuses_is_reported() {
        let store = Arc::new(Mutex::new(AudioStore::with_capacity(1)));
        let (server, _addr) = super::ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
            .await
            .unwrap();
        let report = server.publish_generated(1, 3).await;
        assert!(report.accepted.is_empty());
        assert_eq!(report.rejected.len(), 4, "every refusal must be reported");
        assert!(report.rejected.iter().all(|r| r.contains("larger than")));
    }

    #[tokio::test]
    async fn a_listing_is_sorted_so_it_survives_a_restart() {
        let store = Arc::new(Mutex::new(AudioStore::default()));
        let a = crate::wav::generate(10, crate::wav::Sound::Tone);
        let b = crate::wav::generate(11, crate::wav::Sound::Noise);
        let ha = handle(
            Request::Publish {
                declared_media_type: None,
                bytes: a,
            },
            &store,
        )
        .await
        .published_hash();
        let hb = handle(
            Request::Publish {
                declared_media_type: None,
                bytes: b,
            },
            &store,
        )
        .await
        .published_hash();
        let listing = handle(
            Request::List {
                offset: 0,
                limit: 100,
            },
            &store,
        )
        .await;
        let mut expected = vec![ha.clone(), hb.clone()];
        expected.sort();
        assert_eq!(
            listing,
            Response::Listing {
                content_hashes: expected,
                total: 2,
                offset: 0
            },
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
