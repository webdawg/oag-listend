//! The HTTP carrier: the same audio surface over a protocol a browser already speaks.
//!
//! This is deliberately a small, hand-rolled HTTP/1.1 server. It parses one request, answers it,
//! and closes the connection. There is no keep-alive state, no chunked encoding, no TLS, and no
//! framework: the daemon is a service that serves audio, and a carrier that grew into a web server
//! would be a different program. Every rule is still `AudioStore`'s -- `POST /audio` goes through
//! `AudioStore::accept`, and `GET /audio/{hash}` returns the bytes with the item's media type.
//!
//! Routes:
//! - `GET  /`             -- what this is, and how much audio it holds
//! - `GET  /ping`         -- the canonical musical liveness item, ranged like any other audio
//! - `GET  /audio`        -- one page of content hashes, `?offset=&limit=`
//! - `GET  /audio/{hash}` -- the audio bytes (404 if unknown)
//! - `GET  /ipfs/{cid}`   -- audio fetched from IPFS by content identifier, sniffed like any other
//!                           inbound bytes; 404 if no mirror is configured, 502 if it is unreachable,
//!                           400 if what came back is not audio
//! - `POST /audio`        -- publish a body (413 if too large, 400 if not audio)
//! - `OPTIONS`            -- preflight answer, so another origin may `fetch()`
//! - anything else        -- 404

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::server::{write_all_bounded, ShutdownHandle};
use crate::{AudioFormat, AudioStore, ListendError};

/// Bounds for one HTTP request. The head cap keeps a header flood from allocating; the body cap
/// matches the native protocol's frame cap so the two carriers accept the same audio.
pub const MAX_HEAD_BYTES: usize = 16 * 1024;
pub const MAX_BODY_BYTES: usize = super::protocol::MAX_FRAME_BYTES;
pub const IO_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_CONNECTIONS: usize = 64;
/// A total bound on one HTTP connection -- reading the request, routing it, and writing the
/// response -- so a client that dribbles bytes in either direction cannot hold one of the session
/// slots open indefinitely. Each individual read and write also has a timeout. Generous, because a
/// legitimate client may be transferring the largest item the store accepts over a slow link;
/// still finite.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(1800);

pub struct HttpCarrier {
    store: Arc<Mutex<AudioStore>>,
    listener: TcpListener,
    local_addr: std::net::SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl HttpCarrier {
    pub async fn bind_shared(
        addr: &str,
        store: Arc<Mutex<AudioStore>>,
    ) -> std::io::Result<(Self, std::net::SocketAddr)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Ok((
            Self {
                store,
                listener,
                local_addr,
                shutdown_tx,
            },
            local_addr,
        ))
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle::new(self.shutdown_tx.clone())
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                accepted = self.listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    };
                    let Ok(permit) = semaphore.clone().acquire_owned().await else { break };
                    let store = Arc::clone(&self.store);
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(stream, store).await;
                    });
                }
            }
        }
        Ok(())
    }
}

async fn serve_connection(stream: TcpStream, store: Arc<Mutex<AudioStore>>) -> std::io::Result<()> {
    // The total covers the whole connection -- read, route, and response -- so a client that reads
    // the response slowly cannot hold one of the session slots indefinitely either.
    tokio::time::timeout(REQUEST_TIMEOUT, serve_connection_inner(stream, store))
        .await
        .map_err(|_| timeout_error())?
}

async fn serve_connection_inner(
    mut stream: TcpStream,
    store: Arc<Mutex<AudioStore>>,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await? {
        Some(request) => request,
        None => return Ok(()),
    };
    // A browser page on another origin sends OPTIONS first. Answer it, or the fetch is blocked
    // before it starts. The service has no credentials, so `*` is the honest allowance.
    if request.method == "OPTIONS" {
        let preflight = "HTTP/1.1 204 No Content\r\n\
                         Access-Control-Allow-Origin: *\r\n\
                         Access-Control-Allow-Methods: GET, HEAD, POST, OPTIONS\r\n\
                         Access-Control-Allow-Headers: content-type, range\r\n\
                         Access-Control-Max-Age: 3600\r\n\
                         Content-Length: 0\r\n\
                         Connection: close\r\n\r\n";
        write_all_bounded(&mut stream, preflight.as_bytes(), IO_TIMEOUT).await?;
        return Ok(());
    }
    let unread_body = request.unread_body;
    let response = route(request, &store).await;
    write_response(&mut stream, &response).await?;
    if unread_body {
        // Half-close first: the client sees the response and EOF, and the unread body is drained
        // rather than turning the close into a reset that discards the response.
        let _ = stream.shutdown().await;
        drain(&mut stream, MAX_BODY_BYTES).await?;
    }
    Ok(())
}

/// Discard up to `max` bytes of a body the reader did not consume, so closing the socket does not
/// turn into a TCP reset that discards the response.
async fn drain(stream: &mut TcpStream, max: usize) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    let mut left = max;
    while left > 0 {
        let want = buf.len().min(left);
        let n = tokio::time::timeout(IO_TIMEOUT, stream.read(&mut buf[..want]))
            .await
            .map_err(|_| timeout_error())??;
        if n == 0 {
            break;
        }
        left -= n;
    }
    Ok(())
}

struct HttpRequest {
    method: String,
    path: String,
    content_type: Option<String>,
    range: Option<String>,
    if_none_match: Option<String>,
    transfer_encoding: Option<String>,
    /// The declared body is larger than this carrier will read. It is answered `413 Payload Too
    /// Large` rather than `400 Bad Request`: the request is well formed, only too big.
    too_large: bool,
    /// The request carried a body this reader did not consume. Closing with it unread makes TCP
    /// send a reset, which can discard the response we just wrote -- so it is drained first.
    unread_body: bool,
    body: Vec<u8>,
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<HttpRequest>> {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        if head.len() > MAX_HEAD_BYTES {
            return Ok(Some(HttpRequest {
                method: "BAD".into(),
                path: String::new(),
                content_type: None,
                range: None,
                if_none_match: None,
                transfer_encoding: None,
                too_large: false,
                unread_body: true,
                body: Vec::new(),
            }));
        }
        let n = tokio::time::timeout(IO_TIMEOUT, stream.read(&mut buf))
            .await
            .map_err(|_| timeout_error())??;
        if n == 0 {
            return Ok(None);
        }
        head.extend_from_slice(&buf[..n]);
        if let Some(pos) = find_headers_end(&head) {
            let head_str = String::from_utf8_lossy(&head[..pos]).into_owned();
            let mut lines = head_str.split("\r\n");
            let request_line = lines.next().unwrap_or("");
            let mut parts = request_line.split(' ');
            let method = parts.next().unwrap_or("").to_string();
            let path = parts.next().unwrap_or("").to_string();
            if method.is_empty() || path.is_empty() {
                return Ok(Some(HttpRequest {
                    method: "BAD".into(),
                    path: String::new(),
                    content_type: None,
                    range: None,
                    if_none_match: None,
                    transfer_encoding: None,
                    too_large: false,
                    unread_body: true,
                    body: Vec::new(),
                }));
            }
            let mut content_length = 0usize;
            let mut content_type = None;
            let mut range = None;
            let mut if_none_match = None;
            let mut transfer_encoding = None;
            let mut expect_continue = false;
            for line in lines {
                if let Some((name, value)) = line.split_once(':') {
                    let value = value.trim();
                    if name.trim().eq_ignore_ascii_case("content-length") {
                        content_length = value.parse().unwrap_or(0);
                    } else if name.trim().eq_ignore_ascii_case("content-type") {
                        content_type = Some(value.to_string());
                    } else if name.trim().eq_ignore_ascii_case("range") {
                        range = Some(value.to_string());
                    } else if name.trim().eq_ignore_ascii_case("if-none-match") {
                        if_none_match = Some(value.to_string());
                    } else if name.trim().eq_ignore_ascii_case("transfer-encoding") {
                        transfer_encoding = Some(value.to_string());
                    } else if name.trim().eq_ignore_ascii_case("expect")
                        && value.eq_ignore_ascii_case("100-continue")
                    {
                        expect_continue = true;
                    }
                }
            }
            let body = head[pos + 4..].to_vec();
            let mut body = body;
            // Refuse an oversized body before saying `100 Continue`: telling a client to commit an
            // upload we are about to reject wastes its bandwidth and ours.
            if content_length > MAX_BODY_BYTES {
                return Ok(Some(HttpRequest {
                    method: "BAD".into(),
                    path: String::new(),
                    content_type: None,
                    range: None,
                    if_none_match: None,
                    transfer_encoding: None,
                    too_large: true,
                    // The body was not read, so it must be drained or the refusal is reset away.
                    unread_body: true,
                    body: Vec::new(),
                }));
            }
            if expect_continue {
                // A client that is about to send a body it may not want to send needs an answer
                // before it commits. Without this, curl waits a second on every large publish.
                let interim = b"HTTP/1.1 100 Continue\r\n\r\n";
                write_all_bounded(stream, interim, IO_TIMEOUT).await?;
                tokio::time::timeout(IO_TIMEOUT, stream.flush())
                    .await
                    .map_err(|_| timeout_error())??;
            }
            while body.len() < content_length {
                let n = tokio::time::timeout(IO_TIMEOUT, stream.read(&mut buf))
                    .await
                    .map_err(|_| timeout_error())??;
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&buf[..n]);
            }
            body.truncate(content_length);
            let chunked = transfer_encoding
                .as_deref()
                .map(|value| value.to_ascii_lowercase().contains("chunked"))
                .unwrap_or(false);
            return Ok(Some(HttpRequest {
                method,
                path,
                content_type,
                range,
                if_none_match,
                transfer_encoding,
                too_large: false,
                unread_body: chunked || content_length > body.len(),
                body,
            }));
        }
    }
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

struct HttpResponse {
    status: u16,
    reason: &'static str,
    content_type: String,
    /// `Content-Range` for a partial response, `Accept-Ranges` otherwise.
    content_range: Option<String>,
    accept_ranges: bool,
    /// HEAD sends the entity's length with no body, so the length is carried explicitly rather
    /// than derived from an emptied body.
    content_length: Option<usize>,
    /// Listing pagination, for a client that is walking the store.
    total_count: Option<usize>,
    offset: Option<usize>,
    /// Additional response headers, for caching and future metadata.
    extra_headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn route(request: HttpRequest, store: &Arc<Mutex<AudioStore>>) -> HttpResponse {
    if request.method == "BAD" {
        return if request.too_large {
            HttpResponse::text(
                413,
                "Payload Too Large",
                b"request body exceeds the maximum frame size",
            )
        } else {
            HttpResponse::text(400, "Bad Request", b"bad request")
        };
    }

    // A HEAD is a GET with no body; a player and a browser both probe with one before streaming.
    let head_only = request.method == "HEAD";
    let method = if head_only {
        "GET"
    } else {
        request.method.as_str()
    };

    // A query string is not part of the path. Without this, `GET /audio?offset=0` is a 404.
    let (path, query) = match request.path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (request.path.as_str(), None),
    };

    match (method, path) {
        ("GET", "/") => {
            let store = store.lock().await;
            let count = store.len();
            let (held, max) = (store.bytes_held(), store.max_bytes());
            let mut response = HttpResponse::text(
                200,
                "OK",
                format!(
                    "oag-listend\nprotocol {}\nitems: {count}\nbytes: {held} / {max}\n\
                     GET /audio for the content hashes\nGET /ping for the musical liveness sound\n\
                     only audio is accepted\n",
                    crate::protocol::PROTOCOL_VERSION
                )
                .as_bytes(),
            );
            if head_only {
                response.content_length = Some(response.body.len());
                response.body.clear();
            }
            response
        }
        ("GET", "/ping") => {
            // The musical liveness response: the canonical ping item, served as audio.
            let bytes = crate::wav::twinkle();
            let hash = crate::AudioItem::new(bytes.clone(), None).content_hash;
            let extra_headers = vec![
                ("ETag".to_string(), format!("\"{hash}\"")),
                (
                    "Cache-Control".to_string(),
                    "public, max-age=31536000, immutable".to_string(),
                ),
                (
                    "Content-Disposition".to_string(),
                    format!("inline; filename=\"{hash}.wav\""),
                ),
            ];
            match parse_range(request.range.as_deref(), bytes.len()) {
                None => {
                    let mut response = HttpResponse {
                        status: 200,
                        reason: "OK",
                        content_type: "audio/wav".into(),
                        content_range: None,
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers,
                        body: bytes,
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
                Some(Err(())) => {
                    let mut response = HttpResponse {
                        status: 416,
                        reason: "Range Not Satisfiable",
                        content_type: "text/plain".into(),
                        content_range: Some(format!("bytes */{}", bytes.len())),
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers,
                        body: b"range not satisfiable".to_vec(),
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
                Some(Ok((start, end))) => {
                    let slice = bytes[start..=end].to_vec();
                    let mut response = HttpResponse {
                        status: 206,
                        reason: "Partial Content",
                        content_type: "audio/wav".into(),
                        content_range: Some(format!("bytes {start}-{end}/{}", bytes.len())),
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers,
                        body: slice,
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
            }
        }
        ("GET", "/audio") => {
            // The list operation, on HTTP, paginated like the native one: a client that only
            // fetches can discover what exists without already knowing a hash, and the response
            // reports where the page starts and how much remains. The page is copied under the
            // lock; the body is built after it is released.
            let (offset, limit) = parse_query(query);
            let (page, total, offset) = {
                let store = store.lock().await;
                let hashes = store.hashes();
                let total = hashes.len();
                let limit =
                    crate::protocol::normalize_limit(limit, crate::protocol::MAX_LIST_LIMIT);
                let offset = offset.min(total);
                let end = (offset + limit).min(total);
                (hashes[offset..end].to_vec(), total, offset)
            };
            let mut body = String::new();
            for hash in &page {
                body.push_str(hash);
                body.push('\n');
            }
            let mut response = HttpResponse::text(200, "OK", body.as_bytes());
            response.total_count = Some(total);
            response.offset = Some(offset);
            if head_only {
                response.content_length = Some(response.body.len());
                response.body.clear();
            }
            response
        }
        ("GET", path) if path.starts_with("/audio/") => {
            // Hex digests are case-insensitive; normalise so an uppercased hash still resolves.
            let hash = path["/audio/".len()..].to_ascii_lowercase();
            let hash = hash.as_str();
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return not_found();
            }
            // Look up the item and, separately, where else it lives if this daemon mirrors to
            // IPFS. Both are read under one short lock; the byte copies happen after it is dropped.
            let (item, cid) = {
                let store = store.lock().await;
                match store.serve(hash) {
                    Ok(item) => {
                        let cid = store.content_id(hash).map(str::to_string);
                        (item, cid)
                    }
                    Err(_) => return not_found(),
                }
            };
            // The content type comes from the bytes, not from what the publisher claimed: a WAV
            // posted as application/x-www-form-urlencoded must still be served as audio.
            let content_type = AudioFormat::sniff(&item.bytes)
                .map(|format| format.media_type().to_string())
                .or_else(|| item.media_type.clone())
                .unwrap_or_else(|| "application/octet-stream".into());
            let etag = format!("\"{hash}\"");
            let not_modified = etag_matches(request.if_none_match.as_deref(), &etag);

            let mut response = match parse_range(request.range.as_deref(), item.bytes.len()) {
                None => {
                    let mut response = HttpResponse {
                        status: 200,
                        reason: "OK",
                        content_type,
                        content_range: None,
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers: Vec::new(),
                        body: item.bytes.clone(),
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
                Some(Err(())) => {
                    let mut response = HttpResponse {
                        status: 416,
                        reason: "Range Not Satisfiable",
                        content_type: "text/plain".into(),
                        content_range: Some(format!("bytes */{}", item.bytes.len())),
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers: Vec::new(),
                        body: b"range not satisfiable".to_vec(),
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
                Some(Ok((start, end))) => {
                    let slice = item.bytes[start..=end].to_vec();
                    let mut response = HttpResponse {
                        status: 206,
                        reason: "Partial Content",
                        content_type,
                        content_range: Some(format!("bytes {start}-{end}/{}", item.bytes.len())),
                        accept_ranges: true,
                        content_length: None,
                        total_count: None,
                        offset: None,
                        extra_headers: Vec::new(),
                        body: slice,
                    };
                    if head_only {
                        response.content_length = Some(response.body.len());
                        response.body.clear();
                    }
                    response
                }
            };

            // Where else these bytes can be fetched, for a client that would rather ask IPFS.
            if let Some(cid) = cid.as_deref() {
                response
                    .extra_headers
                    .push(("X-IPFS-CID".to_string(), cid.to_string()));
            }
            // A real filename, so a browser or player that saves the audio gets `<hash>.wav`
            // rather than a bare hash.
            if let Some(format) = AudioFormat::sniff(&item.bytes) {
                response.extra_headers.push((
                    "Content-Disposition".to_string(),
                    format!("inline; filename=\"{hash}.{}\"", format.file_extension()),
                ));
            }
            // The item is content-addressed, so it is immutable: a client may cache it for a year
            // and revalidate with the hash instead of re-downloading it.
            response.extra_headers.push(("ETag".to_string(), etag));
            response.extra_headers.push((
                "Cache-Control".to_string(),
                "public, max-age=31536000, immutable".to_string(),
            ));
            if not_modified {
                response.status = 304;
                response.reason = "Not Modified";
                response.content_range = None;
                response.accept_ranges = false;
                // A 304 has no body, but a HEAD still reports the entity it describes.
                response.content_length = if head_only {
                    Some(item.bytes.len())
                } else {
                    None
                };
                response.body.clear();
            }
            response
        }
        ("GET", path) if path.starts_with("/ipfs/") => {
            // The read half of the mirror: fetch by CID from IPFS and apply exactly the audio rule
            // every other inbound path applies. Nothing is stored -- this is a way to reach audio,
            // not a new inbound channel.
            let cid = &path["/ipfs/".len()..];
            let publisher = store.lock().await.ipfs();
            let Some(publisher) = publisher else {
                return not_found();
            };
            let bytes = match publisher.cat(cid).await {
                Ok(bytes) => bytes,
                Err(e) => return HttpResponse::text(502, "Bad Gateway", e.to_string().as_bytes()),
            };
            if AudioFormat::sniff(&bytes).is_none() {
                // Never echo what came back, whatever it was.
                return HttpResponse::text(
                    400,
                    "Bad Request",
                    b"the object at that content identifier is not audio",
                );
            }
            let content_type = AudioFormat::sniff(&bytes)
                .map(|format| format.media_type().to_string())
                .unwrap_or_else(|| "application/octet-stream".into());
            // Content at a CID cannot change, so a client may cache it for good; and the BLAKE3 of
            // what we actually send lets the receiver prove the bytes are the bytes -- the same
            // guarantee every other route gives.
            let content_hash = crate::AudioItem::new(bytes.clone(), None).content_hash;
            let mut extra_headers = vec![
                ("ETag".to_string(), format!("\"{cid}\"")),
                (
                    "Cache-Control".to_string(),
                    "public, max-age=31536000, immutable".to_string(),
                ),
                ("X-Content-Hash".to_string(), content_hash),
            ];
            if let Some(format) = AudioFormat::sniff(&bytes) {
                extra_headers.push((
                    "Content-Disposition".to_string(),
                    format!("inline; filename=\"{cid}.{}\"", format.file_extension()),
                ));
            }
            let mut response = HttpResponse {
                status: 200,
                reason: "OK",
                content_type,
                content_range: None,
                accept_ranges: false,
                content_length: None,
                total_count: None,
                offset: None,
                extra_headers,
                body: bytes,
            };
            if head_only {
                response.content_length = Some(response.body.len());
                response.body.clear();
            }
            response
        }
        ("POST", "/audio") => {
            // Chunked bodies are not decoded. Say so plainly rather than reading zero bytes and
            // reporting the audio as invalid -- the client can then send a Content-Length.
            if request.unread_body
                && request
                    .transfer_encoding
                    .as_deref()
                    .map(|value| value.to_ascii_lowercase().contains("chunked"))
                    .unwrap_or(false)
            {
                return HttpResponse::text(
                    400,
                    "Bad Request",
                    b"chunked transfer encoding is not supported; send Content-Length",
                );
            }
            let body_len = request.body.len();
            // Same entry point as every other carrier: accept under the lock, mirror after it.
            match crate::accept_and_mirror(store, request.body, request.content_type).await {
                Ok(hash) => HttpResponse::text(201, "Created", hash.as_bytes()),
                Err(ListendError::NotAudio) => HttpResponse::text(
                    400,
                    "Bad Request",
                    ListendError::NotAudio.to_string().as_bytes(),
                ),
                // Audio, but bigger than the store can hold: the same "too large" answer as an
                // oversized request body, so a client can react to one status for both.
                Err(ListendError::TooLarge { .. }) => HttpResponse::text(
                    413,
                    "Payload Too Large",
                    ListendError::TooLarge {
                        len: body_len,
                        max: store.lock().await.max_bytes(),
                    }
                    .to_string()
                    .as_bytes(),
                ),
                Err(e) => HttpResponse::text(400, "Bad Request", e.to_string().as_bytes()),
            }
        }
        _ => not_found(),
    }
}

/// Whether an `If-None-Match` header matches. The header may carry a comma-separated list of
/// validators, and `*` matches any existing resource.
fn etag_matches(header: Option<&str>, etag: &str) -> bool {
    let Some(header) = header else { return false };
    header
        .split(',')
        .map(str::trim)
        .any(|validator| validator == "*" || validator == etag)
}

/// `offset` and `limit` from a query string. An absent or unparsable value falls back to the
/// default; a `limit` above the carrier's cap is clamped by the caller.
fn parse_query(query: Option<&str>) -> (usize, usize) {
    let mut offset = 0usize;
    // Zero means "the default", which the caller resolves; starting at the maximum would make an
    // absent limit return ten thousand entries rather than the default page.
    let mut limit = 0usize;
    let Some(query) = query else {
        return (offset, limit);
    };
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == "offset" {
                offset = value.parse().unwrap_or(offset);
            } else if key == "limit" {
                limit = value.parse().unwrap_or(limit);
            }
        }
    }
    (offset, limit)
}

fn not_found() -> HttpResponse {
    HttpResponse::text(404, "Not Found", b"not found")
}

/// A single byte range from a `Range: bytes=...` header.
///
/// `None` means no usable range header (serve the whole body). `Some(Err(()))` means the header was
/// present but unsatisfiable. Multiple ranges are ignored rather than answered with a multipart
/// body: this carrier serves one contiguous stream, not an archive.
fn parse_range(header: Option<&str>, len: usize) -> Option<Result<(usize, usize), ()>> {
    let header = header?;
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') || spec.trim().is_empty() {
        return None;
    }
    let (start_s, end_s) = spec.split_once('-')?;
    if len == 0 {
        return Some(Err(()));
    }
    if start_s.is_empty() {
        // A suffix range: the last N bytes.
        let suffix: usize = end_s.parse().ok()?;
        if suffix == 0 {
            return Some(Err(()));
        }
        let start = len.saturating_sub(suffix);
        return Some(Ok((start, len - 1)));
    }
    let start: usize = start_s.parse().ok()?;
    if start >= len {
        return Some(Err(()));
    }
    let end = if end_s.is_empty() {
        len - 1
    } else {
        end_s.parse::<usize>().ok()?.min(len - 1)
    };
    if end < start {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}

impl HttpResponse {
    fn text(status: u16, reason: &'static str, body: &[u8]) -> Self {
        Self {
            status,
            reason,
            content_type: "text/plain".into(),
            content_range: None,
            accept_ranges: false,
            content_length: None,
            total_count: None,
            offset: None,
            extra_headers: Vec::new(),
            body: body.to_vec(),
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: &HttpResponse) -> std::io::Result<()> {
    let mut out = Vec::new();
    out.extend_from_slice(
        format!("HTTP/1.1 {} {}\r\n", response.status, response.reason).as_bytes(),
    );
    out.extend_from_slice(format!("Content-Type: {}\r\n", response.content_type).as_bytes());
    let content_length = response.content_length.unwrap_or(response.body.len());
    out.extend_from_slice(format!("Content-Length: {content_length}\r\n").as_bytes());
    if let Some(range) = &response.content_range {
        out.extend_from_slice(format!("Content-Range: {range}\r\n").as_bytes());
    } else if response.accept_ranges {
        out.extend_from_slice(b"Accept-Ranges: bytes\r\n");
    }
    if let Some(total) = response.total_count {
        out.extend_from_slice(format!("X-Total-Count: {total}\r\n").as_bytes());
    }
    if let Some(offset) = response.offset {
        out.extend_from_slice(format!("X-Offset: {offset}\r\n").as_bytes());
    }
    for (name, value) in &response.extra_headers {
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    // The service holds no credentials and serves public audio, so any origin may read it.
    out.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out.extend_from_slice(&response.body);
    write_all_bounded(stream, &out, IO_TIMEOUT).await?;
    tokio::time::timeout(IO_TIMEOUT, stream.flush())
        .await
        .map_err(|_| timeout_error())??;
    Ok(())
}

fn timeout_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "http io timed out")
}
