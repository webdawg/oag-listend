//! The HTTP carrier, end to end: a browser-shaped request gets audio back, and a non-audio body
//! is refused. The same store serves both this and the native protocol.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::http::HttpCarrier;
use oag_listend::{AudioStore, AudioItem, wav};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

/// A raw HTTP response. Audio bytes are not UTF-8, so nothing here may go through a lossy string
/// conversion on the way to a byte comparison.
async fn http(addr: std::net::SocketAddr, request: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(request.as_bytes()).await.expect("write");
    stream.flush().await.expect("flush");
    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out)).await.expect("read bound").expect("read");
    out
}

async fn http_post(addr: std::net::SocketAddr, body: &[u8], content_type: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.write_all(body).await.expect("write body");
    stream.flush().await.expect("flush");
    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out)).await.expect("read bound").expect("read");
    out
}

fn status_of(response: &[u8]) -> u16 {
    let text = String::from_utf8_lossy(response);
    text.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("an HTTP status line")
}

/// The body is everything after the first blank line; the head is the headers.
fn body_of(response: &[u8]) -> &[u8] {
    let pos = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a header terminator");
    &response[pos + 4..]
}

fn head_contains(response: &[u8], needle: &str) -> bool {
    let pos = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(response.len());
    String::from_utf8_lossy(&response[..pos]).to_ascii_lowercase().contains(needle)
}

#[tokio::test]
async fn http_serves_audio_and_refuses_the_rest() {
    tokio::time::timeout(BOUND, exchange())
        .await
        .expect("the http exchange must complete within the bound")
}

async fn exchange() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    // GET / -- what this is.
    let root = http(addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
    assert_eq!(status_of(&root), 200, "{}", String::from_utf8_lossy(&root));
    assert!(String::from_utf8_lossy(&root).contains("oag-listend"));

    // POST /audio -- a generated tone, labelled as WAV.
    let tone = wav::generate(5, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    assert_eq!(status_of(&posted), 201, "{}", String::from_utf8_lossy(&posted));
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();
    assert_eq!(hash.len(), 64, "the body is the content hash: {}", String::from_utf8_lossy(&posted));
    assert!(AudioItem::verify(&hash, &tone));

    // GET /audio/{hash} -- the same bytes, with an audio content type.
    let fetched = http(
        addr,
        &format!("GET /audio/{hash} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(status_of(&fetched), 200, "{}", String::from_utf8_lossy(&fetched));
    assert!(head_contains(&fetched, "content-type: audio/wav"), "{}", String::from_utf8_lossy(&fetched));
    assert_eq!(body_of(&fetched), tone.as_slice(), "the bytes over http must be the bytes that were posted");

    // POST /audio -- HTML labelled audio/wav is refused.
    let html = b"<html><body>hello</body></html>";
    let refused = http_post(addr, html, "audio/wav").await;
    assert_eq!(status_of(&refused), 400, "{}", String::from_utf8_lossy(&refused));
    let refusal = String::from_utf8_lossy(&refused).to_ascii_lowercase();
    assert!(refusal.contains("not audio"), "{}", refusal);
    assert!(!refusal.contains("<html>"), "a refusal must not echo the refused bytes");

    // GET an unknown hash -- 404, not a hang.
    let missing = http(
        addr,
        &format!("GET /audio/{} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", "ab".repeat(32)),
    )
    .await;
    assert_eq!(status_of(&missing), 404, "{}", String::from_utf8_lossy(&missing));

    // An unknown route is 404.
    let route = http(addr, "GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
    assert_eq!(status_of(&route), 404, "{}", String::from_utf8_lossy(&route));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// The HTTP and native carriers share one store: a publish on one is fetchable on the other.
#[tokio::test]
async fn the_two_carriers_serve_the_same_audio() {
    tokio::time::timeout(BOUND, shared_store()).await.expect("shared-store exchange within bound")
}

async fn shared_store() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let (server, native_addr) = oag_listend::ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind native");
    let http_shutdown = carrier.shutdown_handle();
    let native_shutdown = server.shutdown_handle();
    let http_run = tokio::spawn(async move { carrier.run().await });
    let native_run = tokio::spawn(async move { server.run().await });

    // Publish over HTTP.
    let tone = wav::generate(11, wav::Sound::Tone);
    let posted = http_post(http_addr, &tone, "audio/wav").await;
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();

    // Fetch over the native protocol.
    let mut native = TcpStream::connect(native_addr).await.expect("connect native");
    let frame = oag_listend::protocol::encode_frame(&oag_listend::protocol::encode_request(
        &oag_listend::protocol::Request::Fetch { content_hash: hash },
    ));
    native.write_all(&frame).await.expect("write native");
    native.flush().await.expect("flush native");
    let mut prefix = [0u8; 4];
    tokio::time::timeout(BOUND, native.read_exact(&mut prefix)).await.expect("bound").expect("read");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, native.read_exact(&mut body)).await.expect("bound").expect("read");
    match oag_listend::protocol::decode_response(&body).expect("decode") {
        oag_listend::protocol::Response::Audio { bytes, .. } => assert_eq!(bytes, tone),
        other => panic!("expected Audio, got {other:?}"),
    }

    http_shutdown.shutdown();
    native_shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), http_run).await.expect("bound").expect("join").expect("run");
    tokio::time::timeout(Duration::from_secs(5), native_run).await.expect("bound").expect("join").expect("run");
}

/// A player or a browser uses ranges to seek and to probe; the carrier must answer with a partial
/// body, a correct Content-Range, and 416 when the range is unsatisfiable. HEAD must answer with
/// headers and no body.
async fn get_range(addr: std::net::SocketAddr, hash: &str, range: Option<&str>) -> Vec<u8> {
    let mut request = format!("GET /audio/{hash} HTTP/1.1\r\nHost: x\r\n");
    if let Some(range) = range {
        request.push_str(&format!("Range: {range}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    http(addr, &request).await
}

#[tokio::test]
async fn http_range_requests_and_head() {
    tokio::time::timeout(BOUND, ranges()).await.expect("range exchange within the bound")
}

async fn ranges() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let tone = wav::generate(6, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    assert_eq!(status_of(&posted), 201);
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();
    let len = tone.len();

    // No range: the whole body, advertising that ranges are accepted.
    let whole = get_range(addr, &hash, None).await;
    assert_eq!(status_of(&whole), 200);
    assert!(head_contains(&whole, "accept-ranges: bytes"));
    assert_eq!(body_of(&whole), tone.as_slice());

    // A first-byte range.
    let first = get_range(addr, &hash, Some("bytes=0-9")).await;
    assert_eq!(status_of(&first), 206, "{}", String::from_utf8_lossy(&first));
    assert!(head_contains(&first, &format!("content-range: bytes 0-9/{len}")));
    assert_eq!(body_of(&first), &tone[0..10]);

    // A suffix range.
    let last = get_range(addr, &hash, Some("bytes=-10")).await;
    assert_eq!(status_of(&last), 206);
    assert!(head_contains(&last, &format!("content-range: bytes {}-{}/{len}", len - 10, len - 1)));
    assert_eq!(body_of(&last), &tone[len - 10..]);

    // An open-ended range.
    let tail = get_range(addr, &hash, Some("bytes=100-")).await;
    assert_eq!(status_of(&tail), 206);
    assert_eq!(body_of(&tail), &tone[100..]);

    // An unsatisfiable range.
    let bad = get_range(addr, &hash, Some(&format!("bytes={len}-"))).await;
    assert_eq!(status_of(&bad), 416, "{}", String::from_utf8_lossy(&bad));
    assert!(head_contains(&bad, &format!("content-range: bytes */{len}")));

    // Multiple ranges are ignored, not answered with multipart.
    let multi = get_range(addr, &hash, Some("bytes=0-9,20-29")).await;
    assert_eq!(status_of(&multi), 200);
    assert_eq!(body_of(&multi), tone.as_slice());

    // HEAD: headers, no body.
    let head = http(
        addr,
        &format!("HEAD /audio/{hash} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(status_of(&head), 200, "{}", String::from_utf8_lossy(&head));
    assert!(head_contains(&head, &format!("content-length: {len}")));
    assert!(body_of(&head).is_empty(), "HEAD must not carry a body");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// The content type on the way out comes from the bytes, not from what the publisher claimed. A
/// WAV posted as `application/x-www-form-urlencoded` must still be served as audio, or a browser
/// will not play it.
#[tokio::test]
async fn the_served_content_type_comes_from_the_bytes() {
    tokio::time::timeout(BOUND, content_type()).await.expect("content-type exchange within the bound")
}

async fn content_type() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let tone = wav::generate(8, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "application/x-www-form-urlencoded").await;
    assert_eq!(status_of(&posted), 201);
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();

    let fetched = http(
        addr,
        &format!("GET /audio/{hash} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(status_of(&fetched), 200);
    assert!(
        head_contains(&fetched, "content-type: audio/wav"),
        "the bytes are a WAV, so the response must say audio/wav: {}",
        String::from_utf8_lossy(&fetched)
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}
