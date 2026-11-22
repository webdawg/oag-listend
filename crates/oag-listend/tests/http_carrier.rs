//! The HTTP carrier, end to end: a browser-shaped request gets audio back, and a non-audio body
//! is refused. The same store serves both this and the native protocol.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::http::HttpCarrier;
use oag_listend::{wav, AudioItem, AudioStore};
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
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
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
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
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
    let pos = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(response.len());
    String::from_utf8_lossy(&response[..pos])
        .to_ascii_lowercase()
        .contains(needle)
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
    let root = http(
        addr,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status_of(&root), 200, "{}", String::from_utf8_lossy(&root));
    let root_text = String::from_utf8_lossy(&root);
    assert!(root_text.contains("oag-listend"), "{root_text}");
    // The index is the operator's "what is this": it must say which protocol it speaks, how much
    // audio it holds against its bound, and that only audio is accepted.
    assert!(root_text.contains("protocol 1"), "{root_text}");
    assert!(root_text.contains("items: 0"), "{root_text}");
    assert!(root_text.contains("bytes: 0 / "), "{root_text}");
    assert!(root_text.contains("only audio is accepted"), "{root_text}");

    // POST /audio -- a generated tone, labelled as WAV.
    let tone = wav::generate(5, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    assert_eq!(
        status_of(&posted),
        201,
        "{}",
        String::from_utf8_lossy(&posted)
    );
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();
    assert_eq!(
        hash.len(),
        64,
        "the body is the content hash: {}",
        String::from_utf8_lossy(&posted)
    );
    assert!(AudioItem::verify(&hash, &tone));

    // GET /audio/{hash} -- the same bytes, with an audio content type.
    let fetched = http(
        addr,
        &format!("GET /audio/{hash} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(
        status_of(&fetched),
        200,
        "{}",
        String::from_utf8_lossy(&fetched)
    );
    assert!(
        head_contains(&fetched, "content-type: audio/wav"),
        "{}",
        String::from_utf8_lossy(&fetched)
    );
    assert_eq!(
        body_of(&fetched),
        tone.as_slice(),
        "the bytes over http must be the bytes that were posted"
    );

    // POST /audio -- HTML labelled audio/wav is refused.
    let html = b"<html><body>hello</body></html>";
    let refused = http_post(addr, html, "audio/wav").await;
    assert_eq!(
        status_of(&refused),
        400,
        "{}",
        String::from_utf8_lossy(&refused)
    );
    let refusal = String::from_utf8_lossy(&refused).to_ascii_lowercase();
    assert!(refusal.contains("not audio"), "{}", refusal);
    assert!(
        !refusal.contains("<html>"),
        "a refusal must not echo the refused bytes"
    );

    // GET an unknown hash -- 404, not a hang.
    let missing = http(
        addr,
        &format!(
            "GET /audio/{} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
            "ab".repeat(32)
        ),
    )
    .await;
    assert_eq!(
        status_of(&missing),
        404,
        "{}",
        String::from_utf8_lossy(&missing)
    );

    // An unknown route is 404.
    let route = http(
        addr,
        "GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&route),
        404,
        "{}",
        String::from_utf8_lossy(&route)
    );

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
    tokio::time::timeout(BOUND, shared_store())
        .await
        .expect("shared-store exchange within bound")
}

async fn shared_store() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, http_addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let (server, native_addr) =
        oag_listend::ListendServer::bind_shared("127.0.0.1:0", Arc::clone(&store))
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
    let mut native = TcpStream::connect(native_addr)
        .await
        .expect("connect native");
    let frame = oag_listend::protocol::encode_frame(&oag_listend::protocol::encode_request(
        &oag_listend::protocol::Request::Fetch { content_hash: hash },
    ));
    native.write_all(&frame).await.expect("write native");
    native.flush().await.expect("flush native");
    let mut prefix = [0u8; 4];
    tokio::time::timeout(BOUND, native.read_exact(&mut prefix))
        .await
        .expect("bound")
        .expect("read");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, native.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("read");
    match oag_listend::protocol::decode_response(&body).expect("decode") {
        oag_listend::protocol::Response::Audio { bytes, .. } => assert_eq!(bytes, tone),
        other => panic!("expected Audio, got {other:?}"),
    }

    http_shutdown.shutdown();
    native_shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), http_run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
    tokio::time::timeout(Duration::from_secs(5), native_run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
}

/// A player or a browser uses ranges to seek and to probe; the carrier must answer with a partial
/// body, a correct Content-Range, and 416 when the range is unsatisfiable. HEAD must answer with
/// headers and no body.
async fn get_with(addr: std::net::SocketAddr, url: &str, extra_headers: &str) -> Vec<u8> {
    http(
        addr,
        &format!("GET {url} HTTP/1.1\r\nHost: x\r\n{extra_headers}Connection: close\r\n\r\n"),
    )
    .await
}

async fn get_query(addr: std::net::SocketAddr, query: &str) -> Vec<u8> {
    http(
        addr,
        &format!("GET /audio?{query} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await
}

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
    tokio::time::timeout(BOUND, ranges())
        .await
        .expect("range exchange within the bound")
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
    assert_eq!(
        status_of(&first),
        206,
        "{}",
        String::from_utf8_lossy(&first)
    );
    assert!(head_contains(
        &first,
        &format!("content-range: bytes 0-9/{len}")
    ));
    assert_eq!(body_of(&first), &tone[0..10]);

    // A suffix range.
    let last = get_range(addr, &hash, Some("bytes=-10")).await;
    assert_eq!(status_of(&last), 206);
    assert!(head_contains(
        &last,
        &format!("content-range: bytes {}-{}/{len}", len - 10, len - 1)
    ));
    assert_eq!(body_of(&last), &tone[len - 10..]);

    // An open-ended range.
    let tail = get_range(addr, &hash, Some("bytes=100-")).await;
    assert_eq!(status_of(&tail), 206);
    assert_eq!(body_of(&tail), &tone[100..]);

    // An unsatisfiable range.
    let bad = get_range(addr, &hash, Some(&format!("bytes={len}-"))).await;
    assert_eq!(status_of(&bad), 416, "{}", String::from_utf8_lossy(&bad));
    assert!(head_contains(
        &bad,
        &format!("content-range: bytes */{len}")
    ));

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
    tokio::time::timeout(BOUND, content_type())
        .await
        .expect("content-type exchange within the bound")
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

/// A client that only fetches must be able to discover what exists without already knowing a hash,
/// and a client publishing a large body must get a `100 Continue` before it commits to sending it.
#[tokio::test]
async fn http_lists_the_audio_and_honours_expect_continue() {
    tokio::time::timeout(BOUND, discover())
        .await
        .expect("discovery exchange within the bound")
}

async fn discover() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let tone = wav::generate(12, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();

    // GET /audio is the list operation over HTTP: one hash per line.
    let listing = http(
        addr,
        "GET /audio HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&listing),
        200,
        "{}",
        String::from_utf8_lossy(&listing)
    );
    assert_eq!(body_of(&listing), format!("{hash}\n").as_bytes());

    // GET / points at it.
    let root = http(
        addr,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(String::from_utf8_lossy(&root).contains("GET /audio"));

    // A POST that says Expect: 100-continue is answered before the body is read.
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        tone.len()
    );
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.flush().await.expect("flush head");
    let mut interim = [0u8; 25];
    let n = tokio::time::timeout(BOUND, stream.read_exact(&mut interim))
        .await
        .expect("100 within the bound")
        .expect("read interim");
    assert_eq!(&interim[..n], b"HTTP/1.1 100 Continue\r\n\r\n");
    stream.write_all(&tone).await.expect("write body");
    stream.flush().await.expect("flush body");
    let mut raw = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut raw))
        .await
        .expect("bound")
        .expect("read");
    assert_eq!(status_of(&raw), 201, "{}", String::from_utf8_lossy(&raw));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A page on another origin must be able to fetch the audio. That needs CORS headers on the
/// response and an answer to the OPTIONS preflight, or the browser blocks the request.
#[tokio::test]
async fn http_allows_a_cross_origin_browser_client() {
    tokio::time::timeout(BOUND, cors())
        .await
        .expect("cors exchange within the bound")
}

async fn cors() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let preflight = http(
        addr,
        "OPTIONS /audio HTTP/1.1\r\nHost: x\r\nOrigin: http://example.com\r\n\
         Access-Control-Request-Method: POST\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&preflight),
        204,
        "{}",
        String::from_utf8_lossy(&preflight)
    );
    assert!(head_contains(&preflight, "access-control-allow-origin: *"));
    assert!(head_contains(
        &preflight,
        "access-control-allow-methods: get, head, post, options"
    ));
    assert!(head_contains(
        &preflight,
        "access-control-allow-headers: content-type, range"
    ));
    assert!(body_of(&preflight).is_empty());

    let tone = wav::generate(13, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();

    for path in ["/", "/audio", &format!("/audio/{hash}")] {
        let response = http(
            addr,
            &format!("GET {path} HTTP/1.1\r\nHost: x\r\nOrigin: http://example.com\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert_eq!(status_of(&response), 200, "{path}");
        assert!(
            head_contains(&response, "access-control-allow-origin: *"),
            "{path} must be readable from another origin: {}",
            String::from_utf8_lossy(&response)
        );
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A query string is not part of the path, and the HTTP listing is paginated like the native one:
/// `?offset=&limit=` with `X-Total-Count`/`X-Offset` so a client can walk the store.
#[tokio::test]
async fn http_query_strings_and_paginated_listing() {
    tokio::time::timeout(BOUND, query())
        .await
        .expect("query exchange within the bound")
}

async fn query() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let mut hashes = Vec::new();
    for seed in 0..3u64 {
        let wav = wav::generate(seed, wav::Sound::Tone);
        let posted = http_post(addr, &wav, "audio/wav").await;
        hashes.push(String::from_utf8_lossy(body_of(&posted)).trim().to_string());
    }
    hashes.sort();

    let first = get_query(addr, "offset=0&limit=2").await;
    assert_eq!(status_of(&first), 200);
    assert!(
        head_contains(&first, "x-total-count: 3"),
        "{}",
        String::from_utf8_lossy(&first)
    );
    assert!(head_contains(&first, "x-offset: 0"));
    assert_eq!(
        body_of(&first),
        format!("{}\n{}\n", hashes[0], hashes[1]).as_bytes()
    );

    let second = get_query(addr, "offset=2&limit=2").await;
    assert!(head_contains(&second, "x-offset: 2"));
    assert_eq!(body_of(&second), format!("{}\n", hashes[2]).as_bytes());

    // An offset past the end is clamped, not wrapped.
    let past = get_query(addr, "offset=99&limit=2").await;
    assert!(
        head_contains(&past, "x-offset: 3"),
        "{}",
        String::from_utf8_lossy(&past)
    );
    assert!(body_of(&past).is_empty());

    // An unparsable limit falls back rather than failing.
    let bad = get_query(addr, "limit=abc").await;
    assert_eq!(String::from_utf8_lossy(body_of(&bad)).lines().count(), 3);

    // A query string on another route is ignored, not a 404.
    let fetched = http(
        addr,
        &format!(
            "GET /audio/{}?x=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
            hashes[0]
        ),
    )
    .await;
    assert_eq!(status_of(&fetched), 200);
    let missing = http(
        addr,
        "GET /nope?x=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status_of(&missing), 404);

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Audio is content-addressed, so it is immutable: a client may cache it for a year and revalidate
/// with the hash (If-None-Match) instead of re-downloading it.
#[tokio::test]
async fn http_caches_immutable_audio_with_an_etag() {
    tokio::time::timeout(BOUND, caching())
        .await
        .expect("caching exchange within the bound")
}

async fn caching() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let tone = wav::generate(21, wav::Sound::Tone);
    let posted = http_post(addr, &tone, "audio/wav").await;
    let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();
    let etag = format!("\"{hash}\"");
    let url = format!("/audio/{hash}");

    let full = get_with(addr, &url, "").await;
    assert_eq!(status_of(&full), 200);
    assert!(
        head_contains(&full, &format!("etag: {etag}")),
        "{}",
        String::from_utf8_lossy(&full)
    );
    assert!(head_contains(
        &full,
        "cache-control: public, max-age=31536000, immutable"
    ));
    assert!(
        head_contains(
            &full,
            &format!("content-disposition: inline; filename=\"{hash}.wav\"")
        ),
        "a saved file should have a real name: {}",
        String::from_utf8_lossy(&full)
    );

    let revalidated = get_with(addr, &url, &format!("If-None-Match: {etag}\r\n")).await;
    assert_eq!(
        status_of(&revalidated),
        304,
        "{}",
        String::from_utf8_lossy(&revalidated)
    );
    assert!(head_contains(&revalidated, &format!("etag: {etag}")));
    assert!(body_of(&revalidated).is_empty(), "a 304 has no body");

    let stale = get_with(addr, &url, "If-None-Match: \"stale\"\r\n").await;
    assert_eq!(
        status_of(&stale),
        200,
        "a non-matching validator must be served"
    );

    let star = get_with(addr, &url, "If-None-Match: *\r\n").await;
    assert_eq!(status_of(&star), 304, "`*` matches an existing resource");

    // A validator list is the normal form a shared cache sends.
    let list = get_with(addr, &url, &format!("If-None-Match: \"stale\", {etag}\r\n")).await;
    assert_eq!(
        status_of(&list),
        304,
        "a list containing the validator must match"
    );

    let head = http(
        addr,
        &format!("HEAD {url} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(status_of(&head), 200);
    assert!(head_contains(&head, &format!("etag: {etag}")));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// `GET /` reports the store's byte usage against its bound, so an operator can see the bound
/// working rather than discovering it when audio stops being accepted.
#[tokio::test]
async fn http_root_reports_store_usage() {
    tokio::time::timeout(BOUND, usage())
        .await
        .expect("usage exchange within the bound")
}

async fn usage() {
    let store = Arc::new(Mutex::new(AudioStore::with_capacity(10_000)));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let root = http(
        addr,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    let text = String::from_utf8_lossy(&root).into_owned();
    assert!(text.contains("items: 0"), "{text}");
    assert!(
        text.contains("bytes: 0 / 10000 (audio + declared types)"),
        "{text}"
    );

    let wav = wav::generate(31, wav::Sound::Tone);
    let len = wav.len();
    let posted = http_post(addr, &wav, "audio/wav").await;
    assert_eq!(status_of(&posted), 201);

    let root = http(
        addr,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    let text = String::from_utf8_lossy(&root).into_owned();
    assert!(text.contains("items: 1"), "{text}");
    // The usage counts the whole footprint: the audio plus the declared type persisted beside it.
    let held = len + "audio/wav".len();
    assert!(
        text.contains(&format!("bytes: {held} / 10000 (audio + declared types)")),
        "{text}"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// The HTTP liveness response is the same canonical music: `GET /ping` serves audio, not a control
/// string, with an ETag that names the bytes.
#[tokio::test]
async fn http_ping_serves_the_canonical_music() {
    tokio::time::timeout(BOUND, ping_http())
        .await
        .expect("the http ping must complete within the bound")
}

async fn ping_http() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    // The startup set publishes the canonical ping, so it is a real item.
    store
        .lock()
        .await
        .accept(oag_listend::wav::twinkle(), None)
        .await
        .unwrap();
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let response = http(
        addr,
        "GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&response),
        200,
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(head_contains(&response, "content-type: audio/wav"));
    let body = body_of(&response);
    assert!(!body.is_empty(), "a ping must never be an empty pong");
    assert_eq!(
        oag_listend::AudioFormat::sniff(body),
        Some(oag_listend::AudioFormat::Wav)
    );
    let hash = oag_listend::AudioItem::new(body.to_vec(), None).content_hash;
    assert!(head_contains(&response, &format!("etag: \"{hash}\"")));

    // An uppercased hash resolves too -- hex digests are case-insensitive.
    let upper = format!("/audio/{}", hash.to_ascii_uppercase());
    let upper_response = http(
        addr,
        &format!("GET {upper} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert_eq!(
        status_of(&upper_response),
        200,
        "{}",
        String::from_utf8_lossy(&upper_response)
    );
    assert_eq!(body_of(&upper_response), body);

    // The liveness sound ranges like any other served audio.
    let ranged = http(
        addr,
        "GET /ping HTTP/1.1\r\nHost: x\r\nRange: bytes=0-9\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&ranged),
        206,
        "{}",
        String::from_utf8_lossy(&ranged)
    );
    assert!(head_contains(
        &ranged,
        &format!("content-range: bytes 0-9/{}", body.len())
    ));
    assert_eq!(body_of(&ranged), &body[0..10]);

    // The same bytes are a real item: the startup set publishes the canonical ping, so it is
    // fetchable by hash too.
    let listed = http(
        addr,
        "GET /audio HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(String::from_utf8_lossy(body_of(&listed)).contains(&hash));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// An oversized body must be refused before the `100 Continue`, or the client commits an upload the
/// server is about to reject.
#[tokio::test]
async fn an_oversized_expect_continue_gets_no_interim() {
    tokio::time::timeout(BOUND, oversized_expect())
        .await
        .expect("within the bound")
}

async fn oversized_expect() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        oag_listend::http::MAX_BODY_BYTES + 1
    );
    stream.write_all(head.as_bytes()).await.expect("write head");
    // A body follows the headers; the server refuses on the declared length and must still deliver
    // the response rather than resetting it away with the unread body.
    stream.write_all(b"x").await.expect("write body");
    stream.flush().await.expect("flush");
    let mut first = [0u8; 32];
    let n = tokio::time::timeout(BOUND, stream.read_exact(&mut first))
        .await
        .expect("read bound")
        .expect("read");
    let status = String::from_utf8_lossy(&first[..n]);
    assert!(
        status.starts_with("HTTP/1.1 413"),
        "an oversized body is too large, not malformed: {status:?}"
    );
    assert!(!status.starts_with("HTTP/1.1 100"), "got the interim");
    assert!(
        !status.starts_with("HTTP/1.1 400"),
        "it is not a bad request either"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A chunked upload is not decoded; the refusal says so, rather than reading zero bytes and
/// reporting valid audio as invalid.
#[tokio::test]
async fn a_chunked_upload_is_refused_with_a_clear_message() {
    tokio::time::timeout(BOUND, chunked())
        .await
        .expect("within the bound")
}

async fn chunked() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let wav = wav::generate(5, wav::Sound::Tone);
    let chunk = format!("{:x}\r\n", wav.len());
    let body = format!("{chunk}{}", String::from_utf8_lossy(&wav));
    let request = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{body}\r\n0\r\n\r\n"
    );
    let response = http(addr, &request).await;
    assert_eq!(
        status_of(&response),
        400,
        "{}",
        String::from_utf8_lossy(&response)
    );
    let text = String::from_utf8_lossy(&response).to_ascii_lowercase();
    assert!(
        text.contains("chunked") && text.contains("content-length"),
        "the refusal must say what to do: {}",
        text
    );
    assert!(store.lock().await.is_empty(), "nothing is stored");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Audio that is larger than the store's own bound is refused as too large, with the same status an
/// oversized request body gets -- a client can react to one code for both -- and nothing is stored.
#[tokio::test]
async fn audio_larger_than_the_store_bound_is_too_large() {
    tokio::time::timeout(BOUND, store_bound())
        .await
        .expect("within the bound")
}

async fn store_bound() {
    let store = Arc::new(Mutex::new(AudioStore::with_capacity(256)));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let tone = wav::generate(11, wav::Sound::Tone);
    assert!(tone.len() > 256, "the fixture must exceed the tiny store");

    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        tone.len()
    );
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.write_all(&tone).await.expect("write body");
    stream.flush().await.expect("flush");

    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
    let text = String::from_utf8_lossy(&out);
    assert!(
        text.starts_with("HTTP/1.1 413"),
        "a too-large item is 413, got {text}"
    );
    assert!(
        text.contains("larger than") || text.to_lowercase().contains("too large"),
        "the refusal states why: {text}"
    );
    assert_eq!(
        store.lock().await.len(),
        0,
        "a refused item must not be stored"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A WAV of exactly `total` bytes. `wav::generate_n` caps at 40,000 samples, so a fixture at the
/// frame bound has to be built directly. The container sizes are filled in honestly, as a real file
/// would have them; the payload need not be samples, because the sniff reads the header.
fn wav_of_exact_size(total: usize) -> Vec<u8> {
    let data_len = total - 44;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&((36 + data_len) as u32).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM
    out.extend_from_slice(&1u16.to_le_bytes()); // mono
    out.extend_from_slice(&8000u32.to_le_bytes());
    out.extend_from_slice(&16000u32.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&(data_len as u32).to_le_bytes());
    out.resize(total, 0);
    out
}

/// A 413 must name the bound that actually refused the item. An item over the frame cap but well
/// under the store bound used to be reported against `--max-bytes`, which is not what refused it
/// and leaves a client unable to tell a frame-cap refusal from a store-bound one.
#[tokio::test]
async fn a_frame_cap_refusal_names_the_frame_cap() {
    tokio::time::timeout(BOUND, frame_cap_refusal())
        .await
        .expect("within the bound")
}

async fn frame_cap_refusal() {
    // The default store bound is 1 GiB, so only the frame cap can refuse this item.
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    // A WAV exactly at the servable bound, plus one byte: over the cap, under the store bound.
    let mut wav = wav_of_exact_size(oag_listend::protocol::MAX_SERVABLE_BYTES);
    assert_eq!(wav.len(), oag_listend::protocol::MAX_SERVABLE_BYTES);
    wav.push(0);

    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        wav.len()
    );
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.write_all(&wav).await.expect("write body");
    stream.flush().await.expect("flush");

    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
    let text = String::from_utf8_lossy(&out);
    assert!(text.starts_with("HTTP/1.1 413"), "got {text}");
    let cap = oag_listend::protocol::MAX_SERVABLE_BYTES;
    assert!(
        text.contains(&cap.to_string()),
        "the refusal names the bound that refused it ({cap}): {text}"
    );
    assert!(
        !text.contains(&oag_listend::DEFAULT_MAX_BYTES.to_string()),
        "it must not be reported against the store bound: {text}"
    );
    assert_eq!(store.lock().await.len(), 0, "a refused item is not stored");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Every format the daemon recognises must be served under its own audio content type, whatever the
/// publisher claimed. A browser decides whether it can play from this header, so a FLAC or an MP3
/// mislabelled as WAV would silently fail to play.
#[tokio::test]
async fn every_recognised_format_is_served_with_its_own_content_type() {
    tokio::time::timeout(BOUND, all_content_types())
        .await
        .expect("content-type exchange within the bound")
}

async fn all_content_types() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let ogg_bos = || {
        let mut out = Vec::new();
        out.extend_from_slice(b"OggS");
        out.push(0);
        out.push(0x02); // beginning of stream
        out.extend_from_slice(&[0u8; 8]); // granule position
        out.extend_from_slice(&[0u8; 4]); // serial number
        out.extend_from_slice(&[0u8; 4]); // page sequence
        out.extend_from_slice(&[0u8; 4]); // checksum
        out.push(1); // one segment
        out.push(8); // segment length: the eight-byte codec header
        out.extend_from_slice(b"OpusHead");
        out
    };
    let m4a = || {
        let mut out = vec![0u8, 0, 0, 0];
        out.extend_from_slice(b"ftypisom");
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(b"M4A ");
        out
    };

    // Each fixture is posted labelled as a web form, so only the bytes can decide the answer's type.
    let fixtures: [(&str, Vec<u8>, &str); 6] = [
        ("wav", b"RIFF\x24\x00\x00\x00WAVE".to_vec(), "audio/wav"),
        ("aiff", b"FORM\x00\x00\x00\x00AIFF".to_vec(), "audio/aiff"),
        ("flac", b"fLaC\x00\x00\x00\x22".to_vec(), "audio/flac"),
        ("ogg", ogg_bos(), "audio/ogg"),
        (
            "mp3",
            b"ID3\x04\x00\x00\x00\x00\x00\x00".to_vec(),
            "audio/mpeg",
        ),
        ("m4a", m4a(), "audio/mp4"),
    ];

    for (label, bytes, expected) in fixtures {
        let posted = http_post(addr, &bytes, "application/x-www-form-urlencoded").await;
        assert_eq!(status_of(&posted), 201, "{label} was not accepted");
        let hash = String::from_utf8_lossy(body_of(&posted)).trim().to_string();

        let fetched = http(
            addr,
            &format!("GET /audio/{hash} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert_eq!(status_of(&fetched), 200, "{label}");
        assert!(
            head_contains(&fetched, &format!("content-type: {expected}")),
            "{label} must be served as {expected}: {}",
            String::from_utf8_lossy(&fetched)
        );
        assert_eq!(
            body_of(&fetched),
            &bytes[..],
            "{label} came back as different bytes"
        );
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A header flood is refused at the cap rather than read into memory. A client that sends endless
/// headers with no terminator must get a bad request, not hold the connection until a read timeout.
#[tokio::test]
async fn an_oversized_header_block_is_refused() {
    tokio::time::timeout(BOUND, oversized_head())
        .await
        .expect("the header-flood exchange must complete within the bound")
}

async fn oversized_head() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    // A valid request line followed by padding headers past the cap, with no blank line ending the
    // head: without a cap this reader would wait forever.
    let mut request = String::from("GET / HTTP/1.1\r\nHost: x\r\n");
    while request.len() <= oag_listend::http::MAX_HEAD_BYTES + 1024 {
        request.push_str("X-Pad: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n");
    }

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the flooded head");
    stream.flush().await.expect("flush");
    // Half-close so the daemon's post-refusal drain sees an end instead of waiting out its timeout.
    stream.shutdown().await.expect("half-close");

    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
    let text = String::from_utf8_lossy(&out);
    assert!(
        text.starts_with("HTTP/1.1 400"),
        "a flooded header block is a bad request: {text}"
    );
    assert_eq!(
        store.lock().await.len(),
        0,
        "a refused request must not store anything"
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// An oversized body without `Expect: 100-continue` must be refused on the declared length too: the
/// client is never told to commit an upload this carrier will reject, and nothing is stored.
#[tokio::test]
async fn an_oversized_body_without_expect_is_refused() {
    tokio::time::timeout(BOUND, oversized_body())
        .await
        .expect("within the bound")
}

async fn oversized_body() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let head = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        oag_listend::http::MAX_BODY_BYTES + 1
    );
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(head.as_bytes()).await.expect("write head");
    // Send a few bytes so the reader can see data, but far short of the declaration.
    stream.write_all(b"RIFF").await.expect("write body");
    stream.flush().await.expect("flush");
    // Half-close so the post-refusal drain ends instead of waiting out its timeout.
    stream.shutdown().await.expect("half-close");

    let mut out = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read");
    let text = String::from_utf8_lossy(&out);
    assert!(
        text.starts_with("HTTP/1.1 413"),
        "too large is 413, got {text}"
    );
    assert_eq!(store.lock().await.len(), 0, "nothing is stored");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Two `Content-Length` headers make the body's extent ambiguous, and a reverse proxy in front may
/// choose a different one than the daemon -- the request-smuggling setup. RFC 7230 3.3.3 requires
/// rejecting it, so the ambiguity never reaches the body reader.
#[tokio::test]
async fn two_content_length_headers_are_refused() {
    tokio::time::timeout(BOUND, two_content_lengths())
        .await
        .expect("within the bound")
}

async fn two_content_lengths() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let wav = wav::generate(6, wav::Sound::Tone);
    let request = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\n\
         Content-Length: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        wav.len(),
        wav.len() + 1
    );
    let response = http(addr, &request).await;
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 400"), "got {text}");
    assert!(
        text.to_ascii_lowercase().contains("content-length"),
        "the refusal must say which rule broke: {text}"
    );
    assert!(store.lock().await.is_empty(), "nothing is stored");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// Only `chunked` used to be refused, so `Transfer-Encoding: gzip` fell through and its body was
/// read as if a `Content-Length` framed it. Any transfer encoding is refused now.
#[tokio::test]
async fn a_non_chunked_transfer_encoding_is_refused() {
    tokio::time::timeout(BOUND, non_chunked_transfer_encoding())
        .await
        .expect("within the bound")
}

async fn non_chunked_transfer_encoding() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let wav = wav::generate(7, wav::Sound::Tone);
    let request = format!(
        "POST /audio HTTP/1.1\r\nHost: x\r\nContent-Type: audio/wav\r\n\
         Content-Length: {}\r\nTransfer-Encoding: gzip\r\nConnection: close\r\n\r\n{}",
        wav.len(),
        String::from_utf8_lossy(&wav)
    );
    let response = http(addr, &request).await;
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 400"), "got {text}");
    assert!(
        text.to_ascii_lowercase().contains("transfer encoding"),
        "the refusal must say what is unsupported: {text}"
    );
    assert!(store.lock().await.is_empty(), "nothing is stored");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// A `GET /` request whose head -- request line, headers and the blank line -- is exactly `total`
/// bytes.
fn padded_head(total: usize) -> String {
    let fixed = "GET / HTTP/1.1\r\nHost: x\r\nX-Pad: ".len() + 4; // the trailing CRLF CRLF
    assert!(
        total >= fixed,
        "the bound {total} is smaller than the fixed part of the request"
    );
    format!(
        "GET / HTTP/1.1\r\nHost: x\r\nX-Pad: {}\r\n\r\n",
        "p".repeat(total - fixed)
    )
}

/// The head bound is exact. The growth check runs once per read, so without a check on the head
/// itself a head of `MAX_HEAD_BYTES + 1` was accepted under a `MAX_HEAD_BYTES` constant.
#[tokio::test]
async fn the_request_head_bound_is_exact() {
    tokio::time::timeout(BOUND, head_bound())
        .await
        .expect("within the bound")
}

async fn head_bound() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = HttpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind http");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let response = http(addr, &padded_head(oag_listend::http::MAX_HEAD_BYTES)).await;
    assert_eq!(
        status_of(&response),
        200,
        "a head at the bound is served: {}",
        String::from_utf8_lossy(&response)
    );

    let response = http(addr, &padded_head(oag_listend::http::MAX_HEAD_BYTES + 1)).await;
    assert_eq!(
        status_of(&response),
        400,
        "one byte over the bound is refused: {}",
        String::from_utf8_lossy(&response)
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}
