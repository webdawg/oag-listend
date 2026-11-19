//! Panic-safety under garbage. A daemon that panics on hostile input is an availability bug, so
//! random and adversarial bytes go to the codec directly and to every stream carrier, and the daemon
//! must still be serving normally afterwards.
//!
//! Everything here is deterministic -- a fixed-seed LCG, never a random generator -- so a failure
//! reproduces exactly.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::http::HttpCarrier;
use oag_listend::protocol::{self, Request, Response, WireError};
use oag_listend::udp::UdpCarrier;
use oag_listend::unix::UnixCarrier;
use oag_listend::{wav, AudioFormat, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(30);

/// A deterministic 64-bit LCG, so a failure reproduces from the seed alone.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| (self.next() >> 33) as u8).collect()
    }
}

#[test]
fn random_bodies_never_panic_the_codec() {
    let mut lcg = Lcg(0x5EED_1234_ABCD_0001);
    for _ in 0..5_000 {
        // Raw bytes, framed bytes, and lengths drawn from the same stream.
        let n = (lcg.next() as usize) % 512;
        let raw = lcg.bytes(n);
        let _ = protocol::decode_frame(&raw);
        let _ = protocol::decode_request(&raw);
        let _ = protocol::decode_response(&raw);
        let _ = AudioFormat::sniff(&raw);

        let len = (lcg.next() as usize) % 400;
        let mut framed = ((len as u32).min(protocol::MAX_FRAME_BYTES as u32))
            .to_be_bytes()
            .to_vec();
        framed.extend_from_slice(&lcg.bytes(len.min(400)));
        let _ = protocol::decode_frame(&framed);

        // Lengths that would declare more than actually arrived, including the cap boundary.
        for declared in [0u32, 1, 4, protocol::MAX_FRAME_BYTES as u32, u32::MAX] {
            let mut buf = declared.to_be_bytes().to_vec();
            buf.extend_from_slice(&lcg.bytes(8));
            match protocol::decode_frame(&buf) {
                Ok(_) | Err(WireError::Truncated) | Err(WireError::TooLong(_)) => {}
                Err(e) => panic!("an unexpected error for a hostile length: {e}"),
            }
        }
    }
}

#[tokio::test]
async fn garbage_on_the_wire_leaves_the_daemon_serving() {
    let outcome = tokio::time::timeout(BOUND, garbage()).await;
    outcome.expect("the garbage exchange must complete within the bound")
}

async fn garbage() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (native, native_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind native");
    let (http, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let (udp, udp_addr) = UdpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind udp");
    let sock_dir = std::env::temp_dir().join(format!(
        "oag-listend-robustness-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&sock_dir).unwrap();
    let sock_path = sock_dir.join("audio.sock");
    let (unix, _) = UnixCarrier::bind_shared(&sock_path, Arc::clone(&store))
        .await
        .expect("bind unix");
    let native_shutdown = native.shutdown_handle();
    let http_shutdown = http.shutdown_handle();
    let udp_shutdown = udp.shutdown_handle();
    let unix_shutdown = unix.shutdown_handle();
    let native_run = tokio::spawn(async move { native.run().await });
    let http_run = tokio::spawn(async move { http.run().await });
    let udp_run = tokio::spawn(async move { udp.run().await });
    let unix_run = tokio::spawn(async move { unix.run().await });

    let mut lcg = Lcg(0x6A09_E667_F3BC_D001);
    // Valid framing around random bodies: the daemon reads exactly this much, so nothing blocks.
    for _ in 0..100 {
        let n = (lcg.next() as usize) % 2_000;
        let body = lcg.bytes(n);
        let mut stream = match TcpStream::connect(native_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let frame = protocol::encode_frame(&body);
        let _ = stream.write_all(&frame).await;
        let _ = stream.flush().await;
        let mut sink = vec![0u8; 4096];
        let _ = tokio::time::timeout(Duration::from_millis(250), stream.read(&mut sink)).await;
    }

    // Random request heads, each terminated so the reader is never left waiting.
    for _ in 0..100 {
        let n = (lcg.next() as usize) % 512;
        let mut head = lcg.bytes(n);
        head.extend_from_slice(b"\r\n\r\n");
        let mut stream = match TcpStream::connect(http_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let _ = stream.write_all(&head).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
        let mut sink = vec![0u8; 4096];
        let _ = tokio::time::timeout(Duration::from_millis(250), stream.read(&mut sink)).await;
    }

    // Datagrams: random framed bodies to the UDP carrier, whose receive path is its own.
    let udp_client = UdpSocket::bind("127.0.0.1:0").await.expect("bind client");
    for _ in 0..100 {
        let n = (lcg.next() as usize) % 2_000;
        let frame = protocol::encode_frame(&lcg.bytes(n));
        if udp_client.send_to(&frame, udp_addr).await.is_err() {
            break;
        }
    }

    // And the Unix socket, which shares the session loop with TCP but has its own accept path.
    for _ in 0..100 {
        let n = (lcg.next() as usize) % 2_000;
        let frame = protocol::encode_frame(&lcg.bytes(n));
        let mut stream = match tokio::net::UnixStream::connect(&sock_path).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let _ = stream.write_all(&frame).await;
        let _ = stream.flush().await;
        let mut sink = vec![0u8; 4096];
        let _ = tokio::time::timeout(Duration::from_millis(250), stream.read(&mut sink)).await;
    }

    // A length the client declares but never fulfils, then half-closes: the reader must stop at the
    // close rather than wait for bytes that will never arrive. This is the "never an unbounded read"
    // case with a lying peer.
    for declared in [1u32, 100, 100_000, protocol::MAX_FRAME_BYTES as u32] {
        let mut stream = match TcpStream::connect(native_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let mut buf = declared.to_be_bytes().to_vec();
        buf.extend_from_slice(&[0u8; 16]); // far short of the declaration
        let _ = stream.write_all(&buf).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
        // The daemon must notice the close and end the session, not wait for the missing bytes.
        let mut sink = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut sink)).await {
            Ok(Ok(0)) => {}
            other => panic!(
                "a declared length the client does not fulfil must end the session at the close, \
                 got {other:?}"
            ),
        }
    }

    // The same shape over HTTP: a Content-Length the body does not satisfy.
    for content_length in [1usize, 1_000] {
        let mut stream = match TcpStream::connect(http_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let head = format!(
            "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Length: {content_length}\r\n\
             Connection: close\r\n\r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.write_all(b"RIFF").await; // shorter than declared
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
        // Either it answers (the partial body is not audio) or it closes; it must not hang waiting
        // for bytes the client has already said are not coming.
        let mut sink = vec![0u8; 4096];
        if tokio::time::timeout(Duration::from_secs(2), stream.read(&mut sink))
            .await
            .is_err()
        {
            panic!("an unsatisfied Content-Length must not leave the HTTP request open");
        }
    }

    // However hostile the input was, the daemon must still serve normally.
    let mut stream = TcpStream::connect(native_addr).await.expect("reconnect");
    let hello = protocol::encode_frame(&protocol::encode_request(&Request::Hello));
    stream.write_all(&hello).await.expect("write hello");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    assert!(matches!(
        protocol::decode_response(&body).expect("decode hello"),
        Response::Hello { .. }
    ));

    let tone = wav::generate(77, wav::Sound::Tone);
    let publish = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: tone.clone(),
    }));
    stream.write_all(&publish).await.expect("write publish");
    stream.flush().await.expect("flush");
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    let hash = match protocol::decode_response(&body).expect("decode published") {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published after garbage, got {other:?}"),
    };
    assert!(oag_listend::AudioItem::verify(&hash, &tone));

    native_shutdown.shutdown();
    http_shutdown.shutdown();
    udp_shutdown.shutdown();
    unix_shutdown.shutdown();
    for run in [native_run, http_run, udp_run, unix_run] {
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("shutdown bound")
            .expect("join")
            .expect("run");
    }
    let _ = std::fs::remove_dir_all(&sock_dir);
}

/// A client that hangs up while the daemon is answering is ordinary, not hostile: a browser tab
/// closes, a player seeks elsewhere, a script dies. The write must fail quietly and leave the
/// daemon serving. This pins that on both a stream carrier and a request/response carrier.
#[tokio::test]
async fn clients_hanging_up_mid_reply_leave_the_daemon_serving() {
    let outcome = tokio::time::timeout(BOUND, hangups()).await;
    outcome.expect("the hang-up exchange must complete within the bound")
}

async fn hangups() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (native, native_addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind native");
    let (http, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let native_shutdown = native.shutdown_handle();
    let http_shutdown = http.shutdown_handle();
    let native_run = tokio::spawn(async move { native.run().await });
    let http_run = tokio::spawn(async move { http.run().await });

    // The ping is the largest reply the daemon sends unprompted, so it is the most likely to be
    // mid-write when the peer disappears.
    let ping = protocol::encode_frame(&protocol::encode_request(&Request::Ping));
    for _ in 0..200 {
        let mut stream = match TcpStream::connect(native_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let _ = stream.write_all(&ping).await;
        let _ = stream.flush().await;
        // Dropped without reading, so the daemon's write lands on a closed socket.
    }
    for _ in 0..200 {
        let mut stream = match TcpStream::connect(http_addr).await {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let _ = stream
            .write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await;
        let _ = stream.flush().await;
    }

    // The native carrier must still answer a normal exchange.
    let mut stream = TcpStream::connect(native_addr)
        .await
        .expect("reconnect to the native carrier");
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Hello));
    stream.write_all(&frame).await.expect("write hello");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    tokio::time::timeout(BOUND, stream.read_exact(&mut prefix))
        .await
        .expect("bound")
        .expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, stream.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("body");
    assert_eq!(
        protocol::decode_response(&body).expect("decode"),
        Response::Hello {
            name: protocol::DAEMON_NAME.to_string(),
            protocol_version: protocol::PROTOCOL_VERSION,
            item_count: 0,
        }
    );

    // And the HTTP carrier.
    let mut stream = TcpStream::connect(http_addr)
        .await
        .expect("reconnect to the http carrier");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    stream.flush().await.expect("flush");
    let mut sink = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut sink))
        .await
        .expect("bound")
        .expect("read");
    assert!(
        String::from_utf8_lossy(&sink).starts_with("HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&sink)
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
