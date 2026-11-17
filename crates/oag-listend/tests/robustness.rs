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
