//! The HTTP CONNECT tunnel: a client reaches the daemon by asking a proxy to open a tunnel, then
//! speaks the native frame protocol through it. The whole exchange is local -- the proxy and the
//! daemon are both in this process, so the property is pinned without external infrastructure.
//!
//! A new carrier is a new way to reach the same surface; here the surface is unchanged and only the
//! path differs.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioItem, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(30);

/// A minimal CONNECT proxy: it answers `CONNECT`, connects to the target, and pipes bytes both
/// ways for the life of the tunnel.
async fn serve_proxy(listener: TcpListener, target: std::net::SocketAddr) {
    loop {
        let Ok((mut client, _)) = listener.accept().await else {
            return;
        };
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let Ok(n) = client.read(&mut buf).await else {
                return;
            };
            if n == 0 {
                return;
            }
            head.extend_from_slice(&buf[..n]);
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let Ok(mut upstream) = TcpStream::connect(target).await else {
            return;
        };
        let _ = client
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await;
        let _ = client.flush().await;
        let (mut cr, mut cw) = client.split();
        let (mut ur, mut uw) = upstream.split();
        let _ = tokio::join!(
            tokio::io::copy(&mut cr, &mut uw),
            tokio::io::copy(&mut ur, &mut cw),
        );
    }
}

/// One frame over a stream.
async fn exchange(stream: &mut TcpStream, request: &Request) -> std::io::Result<Response> {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    stream.write_all(&frame).await?;
    stream.flush().await?;
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    protocol::decode_response(&body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[tokio::test]
async fn the_daemon_is_reachable_through_a_connect_tunnel() {
    let outcome = tokio::time::timeout(BOUND, tunnel()).await;
    outcome.expect("the tunnelled exchange must complete within the bound")
}

async fn tunnel() {
    let (server, daemon_addr) =
        ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(AudioStore::default())))
            .await
            .expect("bind native");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let proxy = tokio::spawn(serve_proxy(proxy_listener, daemon_addr));

    // Ask the proxy to open a tunnel, then confirm we really are talking to the daemon.
    let mut stream = TcpStream::connect(proxy_addr)
        .await
        .expect("connect to proxy");
    let connect = format!("CONNECT {daemon_addr} HTTP/1.1\r\nHost: {daemon_addr}\r\n\r\n");
    stream
        .write_all(connect.as_bytes())
        .await
        .expect("write CONNECT");
    stream.flush().await.expect("flush CONNECT");

    let mut reply = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await.expect("read proxy reply");
        if n == 0 {
            panic!("the proxy closed before establishing the tunnel");
        }
        reply.extend_from_slice(&buf[..n]);
        if reply.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&reply).starts_with("HTTP/1.1 200"),
        "the proxy must establish the tunnel: {}",
        String::from_utf8_lossy(&reply)
    );

    // The same surface through the tunnel: hello, publish, fetch, verify, refuse.
    assert!(matches!(
        exchange(&mut stream, &Request::Hello).await.expect("hello"),
        Response::Hello { .. }
    ));

    let tone = wav::generate(77, wav::Sound::Tone);
    let hash = match exchange(
        &mut stream,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await
    .expect("publish")
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    match exchange(
        &mut stream,
        &Request::Fetch {
            content_hash: hash.clone(),
        },
    )
    .await
    .expect("fetch")
    {
        Response::Audio { bytes, .. } => {
            assert_eq!(bytes, tone);
            assert!(AudioItem::verify(&hash, &tone));
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    match exchange(
        &mut stream,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: b"<html>".to_vec(),
        },
    )
    .await
    .expect("refusal")
    {
        Response::Rejected { .. } => {}
        other => panic!("expected Rejected, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
    proxy.abort();
}
