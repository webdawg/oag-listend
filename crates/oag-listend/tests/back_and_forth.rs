//! The acceptance test: turn the daemon on, push random audio in, pull audio back, and prove the
//! bytes are the bytes. Everything else a client sends is refused.
//!
//! Bounded end to end: the whole exchange runs under one wall-clock timeout, the listener is on
//! an ephemeral port, and shutdown is requested and awaited rather than assumed.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioFormat, AudioItem, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

async fn send(stream: &mut TcpStream, request: &Request) {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    stream.write_all(&frame).await.expect("write request");
    stream.flush().await.expect("flush request");
}

async fn recv(stream: &mut TcpStream) -> Response {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("read prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    assert!(
        len <= protocol::MAX_FRAME_BYTES,
        "frame {len} exceeds the cap"
    );
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("read body");
    protocol::decode_response(&body).expect("decode response")
}

/// The whole back-and-forth, in one bounded test.
#[tokio::test]
async fn the_daemon_serves_audio_and_refuses_everything_else() {
    let outcome = tokio::time::timeout(BOUND, back_and_forth()).await;
    outcome.expect("the daemon exchange must complete within the bound")
}

async fn back_and_forth() {
    let (server, addr) = ListendServer::bind("127.0.0.1:0", AudioStore::default())
        .await
        .expect("bind an ephemeral port");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to the daemon");

    // 1. Hello.
    send(&mut stream, &Request::Hello).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Hello {
            name: oag_listend::protocol::DAEMON_NAME.to_string(),
            protocol_version: oag_listend::protocol::PROTOCOL_VERSION,
            item_count: 0
        }
    );

    // 2. Push a generated tone. This is "random generated data ... sounds".
    let tone = wav::generate(2026, wav::Sound::Tone);
    assert_eq!(AudioFormat::sniff(&tone), Some(AudioFormat::Wav));
    send(
        &mut stream,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await;
    let hash = match recv(&mut stream).await {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(
        AudioItem::verify(&hash, &tone),
        "the published hash must name the pushed bytes"
    );

    // 3. Pull it back and verify on the receiving side.
    send(
        &mut stream,
        &Request::Fetch {
            content_hash: hash.clone(),
        },
    )
    .await;
    match recv(&mut stream).await {
        Response::Audio {
            content_hash,
            bytes,
        } => {
            assert_eq!(content_hash, hash);
            assert_eq!(bytes, tone, "audio out must be the audio that went in");
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    // 4. The listing carries what was published.
    send(
        &mut stream,
        &Request::List {
            offset: 0,
            limit: 100,
        },
    )
    .await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Listing {
            content_hashes: vec![hash.clone()],
            total: 1,
            offset: 0
        }
    );

    // 5. Non-audio is refused, however it is labelled, and nothing is stored.
    for (bytes, label) in [
        (b"<html><body>hello</body></html>".to_vec(), "text/html"),
        (b"{\"json\":true}".to_vec(), "application/json"),
        (b"\x89PNG\r\n\x1a\n".to_vec(), "image/png"),
        (Vec::new(), "audio/wav"),
        (b"plain text".to_vec(), "audio/mpeg"),
    ] {
        send(
            &mut stream,
            &Request::Publish {
                declared_media_type: Some(label.into()),
                bytes,
            },
        )
        .await;
        match recv(&mut stream).await {
            Response::Rejected { reason } => {
                assert!(!reason.is_empty());
                assert!(
                    !reason.contains("html"),
                    "a rejection must not echo the refused bytes"
                );
            }
            other => panic!("{label} must be rejected, got {other:?}"),
        }
    }
    send(&mut stream, &Request::Hello).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Hello {
            name: oag_listend::protocol::DAEMON_NAME.to_string(),
            protocol_version: oag_listend::protocol::PROTOCOL_VERSION,
            item_count: 1
        }
    );

    // 6. An unknown hash is a typed not-found, not a hang.
    send(
        &mut stream,
        &Request::Fetch {
            content_hash: "ab".repeat(32),
        },
    )
    .await;
    assert_eq!(
        recv(&mut stream).await,
        Response::NotFound {
            content_hash: "ab".repeat(32)
        }
    );

    // 7. Shut down and confirm the accept loop actually stops.
    drop(stream);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("the accept loop must stop within the bound")
        .expect("join")
        .expect("run");
}

/// A second connection must be able to read what the first published: the store is the daemon's,
/// not one session's.
#[tokio::test]
async fn audio_published_on_one_connection_is_served_on_another() {
    let outcome = tokio::time::timeout(BOUND, two_connections()).await;
    outcome.expect("the two-connection exchange must complete within the bound")
}

async fn two_connections() {
    let (server, addr) = ListendServer::bind("127.0.0.1:0", AudioStore::default())
        .await
        .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut first = TcpStream::connect(addr).await.expect("first connection");
    let noise = wav::generate(77, wav::Sound::Noise);
    send(
        &mut first,
        &Request::Publish {
            declared_media_type: None,
            bytes: noise.clone(),
        },
    )
    .await;
    let hash = match recv(&mut first).await {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };

    let mut second = TcpStream::connect(addr).await.expect("second connection");
    send(&mut second, &Request::Fetch { content_hash: hash }).await;
    match recv(&mut second).await {
        Response::Audio { bytes, .. } => assert_eq!(bytes, noise),
        other => panic!("expected Audio, got {other:?}"),
    }

    drop(first);
    drop(second);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Operator directive, `USER_INPUT_RECORD.md` Entry 3: "even pings are music". A native ping must
/// answer with the canonical audio item and its hash -- never an empty control pong.
#[tokio::test]
async fn a_native_ping_is_music() {
    let outcome = tokio::time::timeout(crate::BOUND, ping_native()).await;
    outcome.expect("the ping exchange must complete within the bound")
}

async fn ping_native() {
    use oag_listend::AudioFormat;

    let (server, addr) =
        ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(AudioStore::default())))
            .await
            .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send(&mut stream, &Request::Ping).await;
    match recv(&mut stream).await {
        Response::Audio {
            content_hash,
            bytes,
        } => {
            assert!(!bytes.is_empty(), "a ping must never be an empty pong");
            assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::Wav));
            assert!(
                AudioItem::verify(&content_hash, &bytes),
                "the ping must be hash-verifiable"
            );
        }
        other => panic!("a ping must answer with audio, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A single frame is capped. A client that declares more than `MAX_FRAME_BYTES` must be refused
/// before any body is read or allocated -- otherwise a four-byte lie would make the daemon wait
/// for a body that never comes. The refusal ends the session; the daemon itself keeps serving.
#[tokio::test]
async fn a_frame_over_the_cap_is_refused_without_reading_a_body() {
    let outcome = tokio::time::timeout(BOUND, oversized_frame()).await;
    outcome.expect("the oversized-frame exchange must complete within the bound")
}

async fn oversized_frame() {
    let (server, addr) =
        ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(AudioStore::default())))
            .await
            .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    // Only the prefix: no body follows, because the server should not ask for one.
    let mut hostile = TcpStream::connect(addr).await.expect("connect");
    let declared = (protocol::MAX_FRAME_BYTES + 1) as u32;
    hostile
        .write_all(&declared.to_be_bytes())
        .await
        .expect("write the lying prefix");
    hostile.flush().await.expect("flush");

    let mut buf = [0u8; 1];
    let eof = tokio::time::timeout(Duration::from_secs(5), hostile.read(&mut buf))
        .await
        .expect("the session must close promptly, not wait for a body")
        .expect("read");
    assert_eq!(eof, 0, "the oversized frame must end the session");
    drop(hostile);

    // The daemon refused one frame, not the service: a fresh connection still answers.
    let mut fresh = TcpStream::connect(addr).await.expect("reconnect");
    send(&mut fresh, &Request::Hello).await;
    assert!(matches!(recv(&mut fresh).await, Response::Hello { .. }));
    drop(fresh);

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// `list known hashes` is an operation with an offset and a limit, so a client can walk the store
/// without holding it all. Each page must report the store's real size and where it starts, and a
/// page past the end must be empty rather than wrap or repeat.
#[tokio::test]
async fn the_native_listing_pages() {
    let outcome = tokio::time::timeout(BOUND, listing_pages()).await;
    outcome.expect("the listing exchange must complete within the bound")
}

async fn listing_pages() {
    let (server, addr) =
        ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(AudioStore::default())))
            .await
            .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut hashes = Vec::new();
    for seed in [4u64, 9, 15] {
        send(
            &mut stream,
            &Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: wav::generate(seed, wav::Sound::Noise),
            },
        )
        .await;
        match recv(&mut stream).await {
            Response::Published { content_hash } => hashes.push(content_hash),
            other => panic!("expected Published, got {other:?}"),
        }
    }
    // The listing is sorted by hash, so sort the published hashes the same way before paging.
    hashes.sort();

    let page = |offset, limit| Request::List { offset, limit };
    send(&mut stream, &page(0, 100)).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Listing {
            content_hashes: hashes.clone(),
            total: 3,
            offset: 0
        }
    );
    send(&mut stream, &page(1, 1)).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Listing {
            content_hashes: hashes[1..2].to_vec(),
            total: 3,
            offset: 1
        }
    );
    // A page that runs past the end stops there.
    send(&mut stream, &page(2, 100)).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Listing {
            content_hashes: hashes[2..3].to_vec(),
            total: 3,
            offset: 2
        }
    );
    // An offset beyond the store is clamped to the end and answers empty.
    send(&mut stream, &page(99, 5)).await;
    assert_eq!(
        recv(&mut stream).await,
        Response::Listing {
            content_hashes: Vec::new(),
            total: 3,
            offset: 3
        }
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}
