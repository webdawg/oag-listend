//! The IPFS mirror at the surface: accepted audio is offered to IPFS, and a mirror failure is
//! recorded rather than allowed to lose the audio. The stub here mimics Kubo's `/api/v0/add`; this
//! is explicitly not a real-IPFS test.

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;

use oag_listend::http::HttpCarrier;
use oag_listend::ipfs::IpfsPublisher;
use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioFormat, AudioStore, ListendServer, MirrorOutcome};

const BOUND: Duration = Duration::from_secs(20);

/// Accept through the same entry point the carriers use, so the mirror is exercised too.
async fn accept(
    store: &Arc<TokioMutex<AudioStore>>,
    bytes: Vec<u8>,
    declared: Option<String>,
) -> String {
    oag_listend::accept_and_mirror(store, bytes, declared)
        .await
        .expect("the audio must be accepted")
}

/// A stub that answers like Kubo to exactly `expected` requests, and records every request body.
async fn stub(response: String, expected: usize) -> (String, Arc<Mutex<Vec<u8>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("stub addr");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&seen);
    tokio::spawn(async move {
        for _ in 0..expected {
            let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
            else {
                break;
            };
            let mut buf = vec![0u8; 64 * 1024];
            let mut got = Vec::new();
            loop {
                match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        got.extend_from_slice(&buf[..n]);
                        if got.ends_with(b"--\r\n") {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            captured.lock().unwrap().extend_from_slice(&got);
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        }
    });
    (format!("127.0.0.1:{port}", port = addr.port()), seen)
}

#[tokio::test]
async fn accepted_audio_is_offered_to_ipfs_and_the_cid_is_recorded() {
    let outcome = tokio::time::timeout(BOUND, mirror_ok()).await;
    outcome.expect("the mirror exchange must complete within the bound")
}

async fn mirror_ok() {
    let (addr, seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 58\r\n\r\n{\"Hash\":\"bafyaccepted\",\"Name\":\"audio\",\"Size\":\"8\"}\r\n".into(),
        1,
    )
    .await;
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));

    let tone = wav::generate(5, wav::Sound::Tone);
    let hash = accept(&store, tone.clone(), Some("audio/wav".into())).await;

    assert_eq!(
        store.lock().await.mirror_summary(),
        (1, 0),
        "one mirror, no failures"
    );
    assert_eq!(
        store.lock().await.mirror_log().back().cloned(),
        Some(MirrorOutcome::Mirrored {
            content_hash: hash.clone(),
            cid: "bafyaccepted".into(),
        })
    );

    // The stub really was asked, with these bytes, as a multipart file part.
    let sent_bytes = seen.lock().unwrap().clone();
    let sent = String::from_utf8_lossy(&sent_bytes).into_owned();
    assert!(sent.starts_with("POST /api/v0/add HTTP/1.1"), "{sent}");
    assert!(sent.contains("name=\"file\""), "{sent}");
    assert!(
        sent_bytes.windows(tone.len()).any(|w| w == tone.as_slice()),
        "the posted body must carry the audio"
    );
    assert_eq!(
        store.lock().await.len(),
        1,
        "the audio is held locally regardless"
    );
}

#[tokio::test]
async fn a_mirror_failure_is_recorded_and_does_not_lose_the_audio() {
    let outcome = tokio::time::timeout(BOUND, mirror_fails()).await;
    outcome.expect("the failed-mirror exchange must complete within the bound")
}

async fn mirror_fails() {
    let (addr, _seen) = stub(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".into(),
        1,
    )
    .await;
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));

    let noise = wav::generate(6, wav::Sound::Noise);
    let hash = accept(&store, noise.clone(), None).await;

    assert_eq!(
        store.lock().await.len(),
        1,
        "the audio is stored despite the mirror failing"
    );
    assert!(
        store.lock().await.serve(&hash).is_ok(),
        "and it is served, byte for byte"
    );
    assert_eq!(store.lock().await.mirror_summary(), (0, 1));
    let outcome = store.lock().await.mirror_log().back().cloned();
    match outcome {
        Some(MirrorOutcome::Failed {
            content_hash,
            reason,
        }) => {
            assert_eq!(content_hash, hash);
            assert!(reason.contains("500"), "{reason}");
        }
        other => panic!("expected a recorded failure, got {other:?}"),
    }
}

/// A chunk size near `usize::MAX` must be refused as bad framing, not panic the process. The
/// pre-fix parser wrapped both sums, passed both bounds checks, and panicked on the slice; the
/// mirror is cleartext HTTP, so that reply comes from the network.
#[tokio::test]
async fn a_hostile_chunk_size_is_a_recorded_failure_not_a_crash() {
    tokio::time::timeout(BOUND, hostile_chunk_size())
        .await
        .expect("the hostile reply exchange must complete within the bound")
}

async fn hostile_chunk_size() {
    // A chunked 200 whose second chunk declares usize::MAX - 5, after ten real bytes: both
    // `pos + size` and `out.len() + size` wrap, so both bounds checks would pass.
    let mut reply = String::from("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    reply.push_str("a\r\naaaaaaaaaa\r\n");
    reply.push_str(&format!("{:x}\r\n", usize::MAX - 5));
    reply.push_str("xx\r\n0\r\n\r\n");
    let (addr, _seen) = stub(reply, 1).await;

    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));
    let tone = wav::generate(8, wav::Sound::Tone);
    let hash = accept(&store, tone, None).await;
    assert_eq!(store.lock().await.len(), 1, "the audio is still stored");
    assert!(store.lock().await.serve(&hash).is_ok(), "and still served");
    assert_eq!(
        store.lock().await.mirror_summary(),
        (0, 1),
        "the hostile reply is a recorded mirror failure, not a crash"
    );
}

/// An unreachable API behaves the same way: reported, never fatal.
#[tokio::test]
async fn an_unreachable_api_is_reported_not_fatal() {
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store
        .lock()
        .await
        .set_ipfs(Some(IpfsPublisher::new("127.0.0.1:1")));
    let tone = wav::generate(7, wav::Sound::Silence);
    let hash = accept(&store, tone.clone(), None).await;
    assert_eq!(AudioFormat::sniff(&tone), Some(AudioFormat::Wav));
    assert_eq!(store.lock().await.mirror_summary(), (0, 1));
    assert!(store.lock().await.serve(&hash).is_ok());
}

/// A restart with `--ipfs-api` must offer what is already stored, not only what is published later.
#[tokio::test]
async fn a_restart_offers_the_stored_library_to_ipfs() {
    let outcome = tokio::time::timeout(BOUND, mirror_existing()).await;
    outcome.expect("the startup mirror must complete within the bound")
}

async fn mirror_existing() {
    let (addr, seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafystart\",\"Name\":\"audio\"}\r\n".into(),
        2,
    )
    .await;

    // Build a store as a previous run would have left it, then attach a mirror.
    let mut store = AudioStore::default();
    for seed in [11u64, 12] {
        store
            .accept(wav::generate(seed, wav::Sound::Noise), None)
            .await
            .expect("seed");
    }
    assert_eq!(
        store.mirror_summary(),
        (0, 0),
        "no mirror before one is configured"
    );

    store.set_ipfs(Some(IpfsPublisher::new(addr)));
    let offered = store.mirror_existing(Duration::from_secs(10)).await;
    assert_eq!(offered, 2, "every held item must be offered");
    assert_eq!(store.mirror_summary(), (2, 0));
    assert!(
        !seen.lock().unwrap().is_empty(),
        "the stub must have received the audio"
    );
}

/// The pass is bounded as a whole, so a store must not hold startup open forever behind a mirror
/// that answers nothing.
#[tokio::test]
async fn the_startup_mirror_is_bounded_as_a_whole() {
    let outcome = tokio::time::timeout(BOUND, bounded_mirror()).await;
    outcome.expect("the bounded mirror must complete within the bound")
}

async fn bounded_mirror() {
    // A listener that accepts and then never replies.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a silent stub");
    let addr = listener.local_addr().expect("stub addr").port();
    tokio::spawn(async move {
        // Hold the accepted stream open without replying. Dropping it would close the connection,
        // and the client would see EOF rather than a stall.
        if let Ok(Ok((stream, _))) =
            tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        {
            let _stream = stream;
            std::future::pending::<()>().await;
        }
    });

    let mut store = AudioStore::default();
    store
        .accept(wav::generate(13, wav::Sound::Tone), None)
        .await
        .expect("seed");
    store.set_ipfs(Some(IpfsPublisher::new(format!("127.0.0.1:{addr}"))));

    let started = std::time::Instant::now();
    let offered = store.mirror_existing(Duration::from_millis(200)).await;
    assert_eq!(
        offered, 0,
        "a mirror that never answers must not be counted as offered"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the whole pass must stop at its budget, not at each call's timeout"
    );
}

/// A client that fetches audio is told where else it lives: the CID rides on the native reply and
/// on an HTTP header, so the same bytes can be fetched from IPFS later without asking the operator.
#[tokio::test]
async fn a_client_is_told_where_else_the_audio_lives() {
    let outcome = tokio::time::timeout(BOUND, cid_on_the_wire()).await;
    outcome.expect("the CID-on-the-wire exchange must complete within the bound")
}

async fn cid_on_the_wire() {
    let (addr, _seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafywire\",\"Name\":\"audio\"}\r\n".into(),
        1,
    )
    .await;

    let mut store = AudioStore::default();
    store.set_ipfs(Some(IpfsPublisher::new(addr)));
    let shared = Arc::new(TokioMutex::new(store));

    let (native, native_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&shared))
        .await
        .expect("bind native");
    let (http, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&shared))
        .await
        .expect("bind http");
    let native_shutdown = native.shutdown_handle();
    let http_shutdown = http.shutdown_handle();
    let native_run = tokio::spawn(async move { native.run().await });
    let http_run = tokio::spawn(async move { http.run().await });

    let tone = wav::generate(21, wav::Sound::Tone);
    let hash = match exchange(
        native_addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };

    eprintln!("DEBUG mirror log: {:?}", shared.lock().await.mirror_log());
    match exchange(
        native_addr,
        &Request::Fetch {
            content_hash: hash.clone(),
        },
    )
    .await
    {
        Response::Audio { cid, bytes, .. } => {
            assert_eq!(bytes, tone, "the bytes are unchanged");
            assert_eq!(cid.as_deref(), Some("bafywire"), "the CID must ride along");
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    let response = http_get(http_addr, &format!("/audio/{hash}")).await;
    let head = String::from_utf8_lossy(&response).to_ascii_lowercase();
    assert!(
        head.contains("x-ipfs-cid: bafywire"),
        "HTTP must expose the CID too: {head}"
    );

    native_shutdown.shutdown();
    http_shutdown.shutdown();
    for run in [native_run, http_run] {
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("shutdown bound")
            .expect("join")
            .expect("run");
    }
}

/// A mirror reply whose `Hash` is not a plausible CID must never reach a response header. The
/// mirror is cleartext HTTP by design, so that reply comes from the network; an unvalidated value
/// would be written into `X-IPFS-CID` and `ETag` on every later fetch of that audio.
#[tokio::test]
async fn an_implausible_cid_from_the_node_is_refused_not_served() {
    let outcome = tokio::time::timeout(BOUND, bad_cid()).await;
    outcome.expect("the bad-CID exchange must complete within the bound")
}

async fn bad_cid() {
    let (addr, _seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 62\r\n\r\n{\"Hash\":\"evil\r\nX-Injected: yes\",\"Size\":\"8\"}\r\n"
            .into(),
        1,
    )
    .await;

    let mut store = AudioStore::default();
    store.set_ipfs(Some(IpfsPublisher::new(addr)));
    let shared = Arc::new(TokioMutex::new(store));

    let (native, native_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&shared))
        .await
        .expect("bind native");
    let (http, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&shared))
        .await
        .expect("bind http");
    let native_shutdown = native.shutdown_handle();
    let http_shutdown = http.shutdown_handle();
    let native_run = tokio::spawn(async move { native.run().await });
    let http_run = tokio::spawn(async move { http.run().await });

    let tone = wav::generate(22, wav::Sound::Tone);
    let hash = match exchange(
        native_addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };

    // The audio survived the failed mirror: a mirror must never lose audio.
    assert!(
        shared.lock().await.serve(&hash).is_ok(),
        "the audio must still be held locally"
    );
    // And the failure is reported rather than swallowed.
    assert!(
        shared
            .lock()
            .await
            .mirror_log()
            .iter()
            .any(|outcome| matches!(outcome, MirrorOutcome::Failed { .. })),
        "a refused CID must be recorded as a mirror failure"
    );

    match exchange(
        native_addr,
        &Request::Fetch {
            content_hash: hash.clone(),
        },
    )
    .await
    {
        Response::Audio { cid, bytes, .. } => {
            assert_eq!(bytes, tone, "the bytes are unchanged");
            assert_eq!(cid, None, "a refused CID must not ride along");
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    let response = http_get(http_addr, &format!("/audio/{hash}")).await;
    let head = String::from_utf8_lossy(&response).to_ascii_lowercase();
    assert!(!head.contains("x-injected"), "no injected header: {head}");
    assert!(!head.contains("x-ipfs-cid"), "no CID header: {head}");

    native_shutdown.shutdown();
    http_shutdown.shutdown();
    for run in [native_run, http_run] {
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("shutdown bound")
            .expect("join")
            .expect("run");
    }
}

async fn exchange(addr: std::net::SocketAddr, request: &Request) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    stream.write_all(&frame).await.expect("write");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    protocol::decode_response(&body).expect("decode")
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let request = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    stream.flush().await.expect("flush");
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
    out
}

/// A Kubo node that accepts and then says nothing must not stall the daemon. The mirror happens
/// after the store lock is released, so one slow add cannot block another client's request.
#[tokio::test]
async fn a_hung_mirror_does_not_stall_other_clients() {
    let outcome = tokio::time::timeout(BOUND, hung_mirror()).await;
    outcome.expect("the hung-mirror exchange must complete within the bound")
}

async fn hung_mirror() {
    // Accept one connection and never reply.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a silent stub");
    let api_port = listener.local_addr().expect("stub addr").port();
    tokio::spawn(async move {
        if let Ok(Ok((stream, _))) =
            tokio::time::timeout(Duration::from_secs(20), listener.accept()).await
        {
            let _held = stream;
            std::future::pending::<()>().await;
        }
    });

    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store
        .lock()
        .await
        .set_ipfs(Some(IpfsPublisher::new(format!("127.0.0.1:{api_port}"))));
    let (server, addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind native");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    // A publish whose mirror will hang.
    let publisher = tokio::spawn(async move {
        let tone = wav::generate(31, wav::Sound::Tone);
        let _ = exchange(
            addr,
            &Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: tone,
            },
        )
        .await;
    });
    // Let it reach the hanging mirror before measuring anything else.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = std::time::Instant::now();
    let answered = tokio::time::timeout(Duration::from_secs(3), exchange(addr, &Request::Hello))
        .await
        .expect("another client must be served while the mirror hangs");
    assert!(
        matches!(answered, Response::Hello { .. }),
        "expected Hello, got {answered:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "a slow mirror must not queue behind the store lock"
    );

    publisher.abort();
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A stub for the read half: reads one request head, then replies with `status` and `body`.
async fn cat_stub(status: &'static str, body: Vec<u8>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind cat stub");
    let addr = listener.local_addr().expect("cat stub addr").port();
    tokio::spawn(async move {
        let Ok(Ok((mut stream, _))) =
            tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        else {
            return;
        };
        let mut buf = vec![0u8; 8192];
        let mut got = Vec::new();
        while !got.windows(4).any(|w| w == b"\r\n\r\n") {
            match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }
        let head = format!(
            "{status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.write_all(&body).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    });
    format!("127.0.0.1:{addr}")
}

/// `GET /ipfs/{cid}` reaches IPFS and applies the same audio rule as every other inbound path.
#[tokio::test]
async fn the_daemon_serves_audio_fetched_by_cid() {
    let outcome = tokio::time::timeout(BOUND, serve_by_cid()).await;
    outcome.expect("the by-CID exchange must complete within the bound")
}

async fn serve_by_cid() {
    let tone = wav::generate(41, wav::Sound::Tone);
    let api = cat_stub("HTTP/1.1 200 OK", tone.clone()).await;

    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(api)));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let head = http_get(addr, "/ipfs/bafyabc").await;
    let text = String::from_utf8_lossy(&head);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(
        text.to_ascii_lowercase()
            .contains("content-type: audio/wav"),
        "{text}"
    );
    assert!(text.contains("ETag: \"bafyabc\""), "{text}");
    assert!(
        text.to_ascii_lowercase()
            .contains("cache-control: public, max-age=31536000, immutable"),
        "CID content is immutable and may be cached: {text}"
    );
    // The receiver can prove the bytes are the bytes.
    let expected = oag_listend::AudioItem::new(tone.clone(), None).content_hash;
    assert!(
        text.to_ascii_lowercase()
            .contains(&format!("x-content-hash: {expected}")),
        "the served BLAKE3 must be published so the client can verify: {text}"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Eviction must take the recorded CID with the audio, or a later restart would skip re-adding
/// something that is no longer held.
#[tokio::test]
async fn eviction_removes_the_recorded_cid() {
    let outcome = tokio::time::timeout(BOUND, evict_cid()).await;
    outcome.expect("the eviction exchange must complete within the bound")
}

async fn evict_cid() {
    let dir = std::env::temp_dir().join(format!(
        "oag-listend-cid-evict-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("make the store directory");

    // A bound that holds one sound but not two, so the second publish evicts the first.
    let (addr, _seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafyold\",\"Name\":\"a\"}\r\n"
            .into(),
        2,
    )
    .await;
    let (store, _) = AudioStore::with_directory(&dir, 1_500).expect("open a small store");
    let shared = Arc::new(TokioMutex::new(store));
    shared.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));

    let first = wav::generate(81, wav::Sound::Tone);
    let first_hash = accept(&shared, first.clone(), None).await;
    assert!(dir.join(format!("{first_hash}.cid")).exists());

    let second = wav::generate(82, wav::Sound::Noise);
    let second_hash = accept(&shared, second.clone(), None).await;

    assert_ne!(first_hash, second_hash);
    assert!(!dir.join(&first_hash).exists(), "the evicted audio is gone");
    assert!(
        !dir.join(format!("{first_hash}.cid")).exists(),
        "its recorded CID must be evicted with it"
    );
    assert!(
        dir.join(format!("{second_hash}.cid")).exists(),
        "the survivor keeps its CID"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A restart must not re-add what the previous run recorded. The CID association is persisted beside
/// the audio, so a fresh process reconstructs it instead of rewriting the library to IPFS.
#[tokio::test]
async fn a_restart_does_not_readd_what_the_store_recorded() {
    let outcome = tokio::time::timeout(BOUND, restart_skip()).await;
    outcome.expect("the restart exchange must complete within the bound")
}

async fn restart_skip() {
    let dir = std::env::temp_dir().join(format!(
        "oag-listend-cid-persist-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("make the store directory");

    // First run: one publish, mirrored, into a persistent store.
    let tone = wav::generate(71, wav::Sound::Tone);
    let hash = {
        let (addr, _seen) = stub(
            "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafykeep\",\"Name\":\"a\"}\r\n".into(),
            1,
        )
        .await;
        let (store, _) = AudioStore::with_directory(&dir, 1024 * 1024).expect("open store");
        let shared = Arc::new(TokioMutex::new(store));
        shared.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));
        let hash = accept(&shared, tone.clone(), None).await;
        assert_eq!(
            shared.lock().await.mirror_summary(),
            (1, 0),
            "the first run mirrors once"
        );
        hash
    };
    assert!(
        dir.join(format!("{hash}.cid")).exists(),
        "the CID must be recorded beside the audio"
    );

    // Second run: reload the same directory and offer everything held. Nothing should be added --
    // proved two ways, because a redundant add could fail to connect and still count as an attempt.
    let (addr, seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafylate\",\"Name\":\"a\"}\r\n"
            .into(),
        1,
    )
    .await;
    let (store, report) = AudioStore::with_directory(&dir, 1024 * 1024).expect("reopen store");
    assert_eq!(report.loaded, 1, "the audio reloads");
    assert_eq!(
        store.content_id(&hash),
        Some("bafykeep"),
        "the CID association reloads from its sidecar"
    );
    assert_eq!(
        store.hash_for_cid("bafykeep"),
        Some(hash.as_str()),
        "and the reverse lookup too"
    );
    let shared = Arc::new(TokioMutex::new(store));
    shared.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));
    let offered = shared
        .lock()
        .await
        .mirror_existing(Duration::from_secs(5))
        .await;
    assert_eq!(
        offered, 0,
        "nothing held should need offering after a restart"
    );
    assert!(
        shared.lock().await.mirror_log().is_empty(),
        "no mirror attempt may be logged"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "and no request may reach the node"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Republishing identical bytes must not add them again: the store treats them as one item, so a
/// mirror that added them once should not be asked a second time. This is what keeps restarts and
/// repeated publishes from rewriting the library.
#[tokio::test]
async fn republishing_identical_audio_is_not_added_twice() {
    let outcome = tokio::time::timeout(BOUND, duplicate_add()).await;
    outcome.expect("within the bound")
}

async fn duplicate_add() {
    let (addr, seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 47\r\n\r\n{\"Hash\":\"bafydup\",\"Name\":\"a\"}\r\n"
            .into(),
        1,
    )
    .await;
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(addr)));

    let tone = wav::generate(61, wav::Sound::Tone);
    let first = accept(&store, tone.clone(), None).await;
    let second = accept(&store, tone.clone(), None).await;
    assert_eq!(first, second, "identical bytes have one content hash");
    assert_eq!(store.lock().await.len(), 1);
    // The decisive evidence: the mirror log records one attempt, not two. A redundant add may
    // fail to connect -- but it would still be logged, so this cannot pass vacuously.
    assert_eq!(
        store.lock().await.mirror_log().len(),
        1,
        "a duplicate publish must not attempt a second add"
    );
    // Exactly one add reached the node: the first publish.
    let requests = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(
        requests
            .windows(b"POST ".len())
            .filter(|w| *w == b"POST ")
            .count(),
        1,
        "the same bytes must be offered exactly once"
    );
}

/// A CID for audio we already hold must be served from here, so a Kubo node that answers adds but
/// not cats cannot take held audio with it.
#[tokio::test]
async fn a_held_cid_is_served_without_asking_the_node() {
    let outcome = tokio::time::timeout(BOUND, held_cid_offline()).await;
    outcome.expect("the held-CID exchange must complete within the bound")
}

async fn held_cid_offline() {
    // Add works; cat answers 404. If the daemon asked the node, this would fail.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let api_port = listener.local_addr().expect("stub addr").port();
    tokio::spawn(async move {
        let Ok(Ok((mut stream, _))) =
            tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        else {
            return;
        };
        let mut buf = vec![0u8; 64 * 1024];
        let mut got = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => {
                    got.extend_from_slice(&buf[..n]);
                    if got.ends_with(b"--\r\n") {
                        break;
                    }
                }
                _ => break,
            }
        }
        let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 39\r\n\r\n{\"Hash\":\"bafyheld\",\"Name\":\"a\"}\r\n";
        let _ = stream.write_all(reply).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    });

    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store
        .lock()
        .await
        .set_ipfs(Some(IpfsPublisher::new(format!("127.0.0.1:{api_port}"))));
    let (native, native_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind native");
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let native_shutdown = native.shutdown_handle();
    let shutdown = carrier.shutdown_handle();
    let native_run = tokio::spawn(async move { native.run().await });
    let run = tokio::spawn(async move { carrier.run().await });

    // Publish through the native path so the item is mirrored and the CID index is filled.
    let tone = wav::generate(51, wav::Sound::Tone);
    let hash = match exchange(
        native_addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert_eq!(hash.len(), 64);

    let response = http_get(addr, "/ipfs/bafyheld").await;
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "a held CID must serve without the node's cat: {text}"
    );
    assert!(
        text.to_ascii_lowercase()
            .contains("content-type: audio/wav"),
        "{text}"
    );

    native_shutdown.shutdown();
    shutdown.shutdown();
    for run in [native_run, run] {
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("shutdown bound")
            .expect("join")
            .expect("run");
    }
}

/// Whatever IPFS holds at a CID, only audio may come back out -- and a refusal never echoes it.
#[tokio::test]
async fn a_non_audio_object_is_refused_without_being_echoed() {
    let outcome = tokio::time::timeout(BOUND, refuse_non_audio()).await;
    outcome.expect("the refusal exchange must complete within the bound")
}

async fn refuse_non_audio() {
    let html = b"<html><body>not audio</body></html>".to_vec();
    let api = cat_stub("HTTP/1.1 200 OK", html.clone()).await;

    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store.lock().await.set_ipfs(Some(IpfsPublisher::new(api)));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let response = http_get(addr, "/ipfs/bafynotaudio").await;
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 400"), "{text}");
    assert!(
        !text.contains("<html>"),
        "a refusal must not echo what came back: {text}"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Without a mirror the route does not exist; with an unreachable node it says so plainly.
#[tokio::test]
async fn by_cid_needs_a_reachable_mirror() {
    let outcome = tokio::time::timeout(BOUND, by_cid_errors()).await;
    outcome.expect("the error exchange must complete within the bound")
}

async fn by_cid_errors() {
    // No mirror configured at all.
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });
    let response = http_get(addr, "/ipfs/bafyabc").await;
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 404"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");

    // A mirror that cannot be reached.
    let store = Arc::new(TokioMutex::new(AudioStore::default()));
    store
        .lock()
        .await
        .set_ipfs(Some(IpfsPublisher::new("127.0.0.1:1")));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });
    let response = http_get(addr, "/ipfs/bafyabc").await;
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 502"), "{text}");
    assert!(
        text.to_ascii_lowercase().contains("cannot reach"),
        "the failure should say what happened: {text}"
    );

    // An implausible identifier is refused before it can reach a request line.
    let response = http_get(addr, "/ipfs/..%2f..%2fetc").await;
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 502"),
        "{}",
        String::from_utf8_lossy(&response)
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// The mirror log is a memory bound: it keeps the most recent outcomes and drops the oldest, so a
/// long run cannot grow it without limit. The cap itself (64) is a private constant, so the test
/// pins the behaviour -- capped, newest last -- rather than the number.
#[test]
fn the_mirror_log_is_capped_and_keeps_the_newest() {
    let mut store = AudioStore::default();
    for i in 0..70u64 {
        store.record_mirror_outcome(MirrorOutcome::Failed {
            content_hash: format!("{i:064x}"),
            reason: "unreachable".into(),
        });
    }
    let log = store.mirror_log();
    assert_eq!(log.len(), 64, "the log must be capped");
    assert_eq!(
        store.mirror_summary(),
        (0, 64),
        "the summary counts the retained log"
    );
    match log.front() {
        Some(MirrorOutcome::Failed { content_hash, .. }) => {
            assert_eq!(
                content_hash,
                &format!("{:064x}", 70 - 64),
                "the oldest is dropped"
            )
        }
        other => panic!("expected a failure, got {other:?}"),
    }
    match log.back() {
        Some(MirrorOutcome::Failed { content_hash, .. }) => {
            assert_eq!(content_hash, &format!("{:064x}", 69), "newest last")
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}
