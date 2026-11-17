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
        seen.lock().unwrap().len() > 0,
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

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
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
