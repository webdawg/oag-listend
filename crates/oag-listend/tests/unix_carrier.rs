//! The Unix domain socket carrier over a real socket: a local process reaches the same audio
//! through the same frame protocol and the same inbound rule.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioItem, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

fn socket_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "oag-listend-unix-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

async fn round_trip(stream: &mut UnixStream, request: &Request) -> Response {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    stream.write_all(&frame).await.expect("write");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, stream.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("body");
    protocol::decode_response(&body).expect("decode")
}

#[tokio::test]
async fn a_unix_socket_serves_the_same_audio() {
    let path = socket_path("roundtrip");
    let outcome = tokio::time::timeout(BOUND, round_trip_test(&path)).await;
    let _ = std::fs::remove_file(&path);
    outcome.expect("the unix exchange must complete within the bound")
}

async fn round_trip_test(path: &std::path::Path) {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, bound) = oag_listend::unix::UnixCarrier::bind_shared(path, Arc::clone(&store))
        .await
        .expect("bind unix");
    assert!(bound.exists(), "the socket file exists while listening");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let mut stream = UnixStream::connect(path).await.expect("connect unix");
    assert_eq!(
        round_trip(&mut stream, &Request::Hello).await,
        Response::Hello {
            name: oag_listend::protocol::DAEMON_NAME.to_string(),
            protocol_version: oag_listend::protocol::PROTOCOL_VERSION,
            item_count: 0
        }
    );

    let tone = wav::generate(41, wav::Sound::Tone);
    let hash = match round_trip(
        &mut stream,
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
    assert!(AudioItem::verify(&hash, &tone));

    match round_trip(&mut stream, &Request::Fetch { content_hash: hash }).await {
        Response::Audio { bytes, .. } => assert_eq!(bytes, tone),
        other => panic!("expected Audio, got {other:?}"),
    }

    // The musical ping is exposed on the Unix carrier too.
    match round_trip(&mut stream, &Request::Ping).await {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert!(!bytes.is_empty());
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("a ping must answer with audio, got {other:?}"),
    }

    match round_trip(
        &mut stream,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: b"<html>".to_vec(),
        },
    )
    .await
    {
        Response::Rejected { .. } => {}
        other => panic!("expected a rejection, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
    assert!(!bound.exists(), "shutdown removes the socket file");
}

/// The Unix socket and TCP serve one store: publish on the socket, fetch on TCP.
#[tokio::test]
async fn the_unix_and_tcp_carriers_serve_one_store() {
    let path = socket_path("shared");
    let outcome = tokio::time::timeout(BOUND, shared(&path)).await;
    let _ = std::fs::remove_file(&path);
    outcome.expect("shared exchange within the bound")
}

async fn shared(path: &std::path::Path) {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (unix, _) = oag_listend::unix::UnixCarrier::bind_shared(path, Arc::clone(&store))
        .await
        .expect("bind unix");
    let (server, tcp_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind tcp");
    let unix_shutdown = unix.shutdown_handle();
    let tcp_shutdown = server.shutdown_handle();
    let unix_run = tokio::spawn(async move { unix.run().await });
    let tcp_run = tokio::spawn(async move { server.run().await });

    let tone = wav::generate(42, wav::Sound::Noise);
    let mut stream = UnixStream::connect(path).await.expect("connect unix");
    let hash = match round_trip(
        &mut stream,
        &Request::Publish {
            declared_media_type: None,
            bytes: tone.clone(),
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };

    let mut tcp = tokio::net::TcpStream::connect(tcp_addr)
        .await
        .expect("connect tcp");
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
        content_hash: hash,
    }));
    tcp.write_all(&frame).await.expect("write tcp");
    tcp.flush().await.expect("flush tcp");
    let mut prefix = [0u8; 4];
    tokio::time::timeout(BOUND, tcp.read_exact(&mut prefix))
        .await
        .expect("bound")
        .expect("read");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, tcp.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("read");
    match protocol::decode_response(&body).expect("decode") {
        Response::Audio { bytes, .. } => assert_eq!(bytes, tone),
        other => panic!("expected Audio, got {other:?}"),
    }

    unix_shutdown.shutdown();
    tcp_shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), unix_run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
    tokio::time::timeout(Duration::from_secs(5), tcp_run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
}

/// A second daemon must not steal a socket that is already being served; two daemons on one path
/// would silently split clients between them.
#[tokio::test]
async fn a_second_daemon_refuses_to_steal_a_live_socket() {
    let path = socket_path("steal");
    let outcome = tokio::time::timeout(BOUND, steal(&path)).await;
    let _ = std::fs::remove_file(&path);
    outcome.expect("the steal check must complete within the bound")
}

async fn steal(path: &std::path::Path) {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, _) = oag_listend::unix::UnixCarrier::bind_shared(path, Arc::clone(&store))
        .await
        .expect("first bind");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let second = oag_listend::unix::UnixCarrier::bind_shared(
        path,
        Arc::new(Mutex::new(AudioStore::default())),
    )
    .await;
    assert!(second.is_err(), "a live socket must not be stolen");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A path that exists but is not a socket must not be deleted: removing it would destroy something
/// the operator put there.
#[tokio::test]
async fn a_non_socket_path_is_refused_not_deleted() {
    let path = socket_path("notasocket");
    std::fs::write(&path, b"precious").unwrap();
    let outcome = tokio::time::timeout(BOUND, refuse(&path)).await;
    outcome.expect("the check must complete within the bound")
}

async fn refuse(path: &std::path::Path) {
    let error = match oag_listend::unix::UnixCarrier::bind_shared(
        path,
        Arc::new(Mutex::new(AudioStore::default())),
    )
    .await
    {
        Ok(_) => panic!("a regular file must not be replaced by a socket"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        std::fs::read(path).unwrap(),
        b"precious",
        "the file must be left alone"
    );
    let _ = std::fs::remove_file(path);
}

/// SPEC: "A stale socket file from a killed daemon is removed on bind." The live-socket refusal and
/// the non-socket refusal are pinned elsewhere; this pins the other half -- a socket whose listener
/// is gone is the exact thing a killed daemon leaves behind, and it must be replaced rather than
/// leaving the path unusable.
#[tokio::test]
async fn a_stale_socket_file_is_replaced_not_refused() {
    let path = socket_path("stalesocket");
    let stale = std::os::unix::net::UnixListener::bind(&path).expect("bind the stale socket");
    drop(stale);
    assert!(path.exists());
    assert!(
        UnixStream::connect(&path).await.is_err(),
        "nothing is listening on it"
    );

    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, bound) = oag_listend::unix::UnixCarrier::bind_shared(&path, Arc::clone(&store))
        .await
        .expect("a stale socket must be replaced");
    assert_eq!(bound, path);
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    // And it serves once replaced -- on this carrier, not a second one bound to the same path.
    let mut stream = UnixStream::connect(&path).await.expect("connect unix");
    let tone = wav::generate(42, wav::Sound::Tone);
    let hash = match round_trip(
        &mut stream,
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
    assert!(AudioItem::verify(&hash, &tone));
    match round_trip(&mut stream, &Request::Fetch { content_hash: hash }).await {
        Response::Audio { bytes, .. } => assert_eq!(bytes, tone),
        other => panic!("expected Audio, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
    let _ = std::fs::remove_file(&path);
}
