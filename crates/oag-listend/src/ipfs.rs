//! The IPFS mirror: audio the daemon accepts is also offered to IPFS, so it can reach a
//! network whose clients never have to connect to this daemon.
//!
//! This is deliberately a *mirror*, not a carrier and not a new operation. The frame protocol
//! permits only publish, fetch-by-hash, list, hello, and ping, so IPFS cannot add a way to ask for
//! something; it can only carry a copy of what was already accepted. Every inbound byte still goes
//! through [`crate::AudioStore::accept`], and the BLAKE3 content hash remains the identity --
//! the CID is an additional address for the same bytes, never a replacement.
//!
//! A mirror must never lose audio. `add` is called after the durable write has succeeded, and its
//! result is recorded rather than propagated: if IPFS is down, slow, or speaking something this
//! client does not understand, the audio is still stored and served locally.
//!
//! Honest limitation: this client speaks just enough HTTP/1.1 to post one multipart form and read
//! one small JSON reply. It has been tested against a stub that mimics Kubo's `/api/v0/add`; it has
//! **not** been tested against a real Kubo node, and it does not decode chunked transfer encoding.

#![forbid(unsafe_code)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A mirror attempt is bounded like every other network call the daemon makes.
pub const ADD_TIMEOUT: Duration = Duration::from_secs(10);
/// The reply to an add is a few hundred bytes. A larger reply is not a Kubo add response, and
/// reading it without limit would let a hostile endpoint allocate for us.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;
/// The largest object `/api/v0/cat` will return. Matches the largest item the store can serve, so
/// fetching by CID cannot pull in more than the daemon would otherwise hold.
pub const MAX_CAT_BYTES: usize = crate::protocol::MAX_SERVABLE_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum IpfsError {
    #[error("cannot reach the IPFS API at {api}: {reason}")]
    Connect { api: String, reason: String },
    #[error("the IPFS API said {status}")]
    Status { status: String },
    #[error("the IPFS API reply was not an add response")]
    NotAdd,
    #[error("the IPFS API reply exceeded its bound")]
    TooLarge,
    #[error("{0:?} is not a plausible content identifier")]
    BadCid(String),
    #[error("the IPFS API sent a chunked reply, which this client does not decode")]
    Chunked,
}

/// Whether the reply head asks for chunked transfer encoding. Kubo does this for larger cat
/// responses; treating the chunk framing as audio would silently corrupt it, so it is refused.
fn is_chunked(head: &str) -> bool {
    head.lines().any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    })
}

/// Posts bytes to a Kubo node's `/api/v0/add` and returns the resulting CID.
#[derive(Debug, Clone)]
pub struct IpfsPublisher {
    api: String,
}

impl IpfsPublisher {
    pub fn new(api: impl Into<String>) -> Self {
        Self { api: api.into() }
    }

    /// The address this publisher posts to, for logs.
    pub fn api(&self) -> &str {
        &self.api
    }

    /// Add `bytes` and return the CID Kubo assigned.
    ///
    /// Any failure is the caller's to record; it never fails the accept that triggered it.
    pub async fn add(&self, bytes: &[u8]) -> Result<String, IpfsError> {
        let (host, path) = split_api(&self.api)?;
        let boundary = "oag-listend-mirror";
        let mut body = Vec::with_capacity(bytes.len() + 256);
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"audio\"\r\n\
              Content-Type: application/octet-stream\r\n\r\n",
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let head = format!(
            "POST {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: multipart/form-data; boundary={boundary}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );

        let mut stream = tokio::time::timeout(ADD_TIMEOUT, TcpStream::connect(&host))
            .await
            .map_err(|_| connect_error(&self.api, "timed out"))?
            .map_err(|e| connect_error(&self.api, &e.to_string()))?;

        write_all_bounded(&mut stream, head.as_bytes(), &self.api).await?;
        write_all_bounded(&mut stream, &body, &self.api).await?;
        stream.flush().await.map_err(|e| IpfsError::Connect {
            api: self.api.clone(),
            reason: e.to_string(),
        })?;

        // The reply is small and we asked the server to close, so read to end under a bound.
        let mut reply = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            if reply.len() > MAX_RESPONSE_BYTES {
                return Err(IpfsError::TooLarge);
            }
            let n = tokio::time::timeout(ADD_TIMEOUT, stream.read(&mut chunk))
                .await
                .map_err(|_| IpfsError::Connect {
                    api: self.api.clone(),
                    reason: "reply timed out".into(),
                })?
                .map_err(|e| IpfsError::Connect {
                    api: self.api.clone(),
                    reason: e.to_string(),
                })?;
            if n == 0 {
                break;
            }
            reply.extend_from_slice(&chunk[..n]);
        }
        parse_add_response(&reply)
    }

    /// Fetch the bytes stored at `cid`.
    ///
    /// This is the read half of the mirror. The bytes are whatever IPFS holds at that identifier; the
    /// caller must still apply the audio rule, exactly as it would to bytes arriving on any other
    /// carrier. Nothing here verifies that the bytes hash to the CID -- that needs an IPFS multihash
    /// implementation this crate deliberately does not carry -- so the caller identifies them by CID
    /// and says so.
    pub async fn cat(&self, cid: &str) -> Result<Vec<u8>, IpfsError> {
        validate_cid(cid)?;
        let (host, _) = split_api(&self.api)?;
        let mut stream = tokio::time::timeout(ADD_TIMEOUT, TcpStream::connect(&host))
            .await
            .map_err(|_| connect_error(&self.api, "timed out"))?
            .map_err(|e| connect_error(&self.api, &e.to_string()))?;

        let head = format!(
            "POST /api/v0/cat?arg={cid} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
        );
        write_all_bounded(&mut stream, head.as_bytes(), &self.api).await?;

        let mut reply = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            if reply.len() > MAX_CAT_BYTES + MAX_RESPONSE_BYTES {
                return Err(IpfsError::TooLarge);
            }
            let n = tokio::time::timeout(ADD_TIMEOUT, stream.read(&mut chunk))
                .await
                .map_err(|_| connect_error(&self.api, "read timed out"))?
                .map_err(|e| connect_error(&self.api, &e.to_string()))?;
            if n == 0 {
                break;
            }
            reply.extend_from_slice(&chunk[..n]);
        }

        let head_end = find_subslice(&reply, b"\r\n\r\n").ok_or(IpfsError::NotAdd)?;
        let head = String::from_utf8_lossy(&reply[..head_end]);
        if is_chunked(&head) {
            return Err(IpfsError::Chunked);
        }
        let status = head.lines().next().unwrap_or("");
        let ok = status
            .split_whitespace()
            .nth(1)
            .map(|code| code.starts_with('2'))
            .unwrap_or(false);
        if !ok {
            return Err(IpfsError::Status {
                status: status.to_string(),
            });
        }
        let body = reply[head_end + 4..].to_vec();
        if body.len() > MAX_CAT_BYTES {
            return Err(IpfsError::TooLarge);
        }
        Ok(body)
    }
}

/// A CID is base32 or base58 text. Anything else is refused before it can reach a request line.
pub(crate) fn validate_cid(cid: &str) -> Result<(), IpfsError> {
    // Multibase encodings use alphanumerics plus `-` and `_` (base64url); anything else is
    // refused before it can reach a request line or a filesystem path.
    let ok = !cid.is_empty()
        && cid.len() <= 128
        && cid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(IpfsError::BadCid(cid.to_string()))
    }
}

fn connect_error(api: &str, reason: &str) -> IpfsError {
    IpfsError::Connect {
        api: api.to_string(),
        reason: reason.to_string(),
    }
}

/// Split `http://host:port/` or `host:port` into the connect address and the request path.
///
/// TLS is deliberately refused rather than half-supported: stripping `https://` and speaking plain
/// HTTP to that port would look like a working configuration and silently send audio in clear.
fn split_api(api: &str) -> Result<(String, String), IpfsError> {
    let trimmed = api.trim();
    if trimmed.starts_with("https://") {
        return Err(IpfsError::Connect {
            api: api.to_string(),
            reason: "https is not supported; give an http:// Kubo address".into(),
        });
    }
    let rest = trimmed.strip_prefix("http://").unwrap_or(trimmed);
    let (host, path) = match rest.find('/') {
        Some(0) => {
            return Err(IpfsError::Connect {
                api: api.to_string(),
                reason: "no host".into(),
            })
        }
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/api/v0/add"),
    };
    if host.is_empty() {
        return Err(IpfsError::Connect {
            api: api.to_string(),
            reason: "no host".into(),
        });
    }
    // The path is fixed: this client only ever adds. Anything past the host is ignored.
    let _ = path;
    Ok((host.to_string(), "/api/v0/add".to_string()))
}

/// Pull `"Hash"` out of a Kubo add reply without a JSON dependency.
fn parse_add_response(reply: &[u8]) -> Result<String, IpfsError> {
    let head_end = find_subslice(reply, b"\r\n\r\n").ok_or(IpfsError::NotAdd)?;
    let head = String::from_utf8_lossy(&reply[..head_end]);
    if is_chunked(&head) {
        return Err(IpfsError::Chunked);
    }
    let mut lines = head.lines();
    let status = lines.next().ok_or(IpfsError::NotAdd)?;
    let ok = status
        .split_whitespace()
        .nth(1)
        .map(|code| code.starts_with('2'))
        .unwrap_or(false);
    if !ok {
        return Err(IpfsError::Status {
            status: status.to_string(),
        });
    }
    let body = &reply[head_end + 4..];
    let text = String::from_utf8_lossy(body);
    let key = text.find("\"Hash\"").ok_or(IpfsError::NotAdd)? + "\"Hash\"".len();
    let rest = text[key..].trim_start();
    let rest = rest
        .strip_prefix(':')
        .ok_or(IpfsError::NotAdd)?
        .trim_start();
    let rest = rest.strip_prefix('"').ok_or(IpfsError::NotAdd)?;
    let end = rest.find('"').ok_or(IpfsError::NotAdd)?;
    let cid = &rest[..end];
    if cid.is_empty() {
        return Err(IpfsError::NotAdd);
    }
    Ok(cid.to_string())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn write_all_bounded(
    stream: &mut TcpStream,
    mut buf: &[u8],
    api: &str,
) -> Result<(), IpfsError> {
    while !buf.is_empty() {
        let n = tokio::time::timeout(ADD_TIMEOUT, stream.write(buf))
            .await
            .map_err(|_| IpfsError::Connect {
                api: api.to_string(),
                reason: "write timed out".into(),
            })?
            .map_err(|e| IpfsError::Connect {
                api: api.to_string(),
                reason: e.to_string(),
            })?;
        if n == 0 {
            return Err(IpfsError::Connect {
                api: api.to_string(),
                reason: "the connection accepted no more bytes".into(),
            });
        }
        buf = &buf[n..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A stub that answers one POST like Kubo's `/api/v0/add` does.
    async fn stub(response: String) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 64 * 1024];
            let mut got = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.expect("read");
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
                // The multipart body ends with the closing boundary.
                if got.windows(2).any(|w| w == b"--") && got.ends_with(b"--\r\n") {
                    break;
                }
            }
            stream.write_all(response.as_bytes()).await.expect("write");
            stream.flush().await.expect("flush");
            let _ = stream.shutdown().await;
            got
        });
        (format!("127.0.0.1:{port}", port = addr.port()), handle)
    }

    #[tokio::test]
    async fn an_add_returns_the_cid_kubo_assigned() {
        let (addr, server) =
            stub("HTTP/1.1 200 OK\r\nContent-Length: 58\r\n\r\n{\"Hash\":\"bafytestcid\",\"Name\":\"audio\",\"Size\":\"8\"}\r\n".into())
                .await;
        let cid = IpfsPublisher::new(addr)
            .add(b"RIFF\x00\x00\x00\x00WAVE")
            .await
            .expect("add");
        assert_eq!(cid, "bafytestcid");
        let sent = server.await.expect("join");
        let text = String::from_utf8_lossy(&sent);
        assert!(text.starts_with("POST /api/v0/add HTTP/1.1"), "{text}");
        assert!(text.contains("multipart/form-data; boundary=oag-listend-mirror"));
        assert!(text.contains("name=\"file\""));
    }

    #[tokio::test]
    async fn a_non_2xx_reply_is_an_error_that_names_the_status() {
        let (addr, server) =
            stub("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".into()).await;
        match IpfsPublisher::new(addr).add(b"RIFF....WAVE").await {
            Err(IpfsError::Status { status }) => assert!(status.contains("500"), "{status}"),
            other => expected_err(other),
        }
        server.await.expect("join");
    }

    #[tokio::test]
    async fn a_reply_without_a_hash_is_not_an_add_response() {
        let (addr, server) =
            stub("HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n{\"Size\":\"8\"}\r\n".into()).await;
        assert!(matches!(
            IpfsPublisher::new(addr).add(b"RIFF....WAVE").await,
            Err(IpfsError::NotAdd)
        ));
        server.await.expect("join");
    }

    /// A chunked reply is refused rather than decoded as bytes, which would corrupt the audio.
    #[tokio::test]
    async fn a_chunked_reply_is_refused_not_misread() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
            else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
                )
                .await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        });
        match IpfsPublisher::new(format!("127.0.0.1:{addr}"))
            .cat("bafyabc")
            .await
        {
            Err(IpfsError::Chunked) => {}
            other => panic!("expected a chunked refusal, got {other:?}"),
        }
    }

    /// An implausible identifier is refused before it can reach a request line, so a crafted path
    /// cannot turn into query injection.
    #[tokio::test]
    async fn an_implausible_cid_is_refused_without_a_network_call() {
        // Multibase characters are accepted; separators, whitespace and over-length are not.
        for cid in [
            "bafyabc-_",
            "Qm----__",
            "",
            "..%2f..",
            "has space",
            &"a".repeat(200),
        ] {
            if cid.contains('-') || cid.contains('_') {
                assert!(
                    crate::ipfs::validate_cid(cid).is_ok(),
                    "{cid:?} is a plausible multibase identifier"
                );
                continue;
            }
            match IpfsPublisher::new("127.0.0.1:1").cat(cid).await {
                Err(IpfsError::BadCid(_)) => {}
                other => panic!("{cid:?} must be refused as a CID, got {other:?}"),
            }
        }
    }

    /// A TLS endpoint must be refused plainly, not downgraded to a cleartext request that appears
    /// to work.
    #[test]
    fn a_tls_endpoint_is_refused_rather_than_sent_in_clear() {
        match split_api("https://127.0.0.1:5001") {
            Err(e) => assert!(e.to_string().contains("https is not supported"), "{e}"),
            Ok(_) => panic!("a TLS endpoint must not be accepted"),
        }
        // Plain http and a bare host:port both still resolve.
        assert!(split_api("http://127.0.0.1:5001").is_ok());
        assert!(split_api("127.0.0.1:5001").is_ok());
    }

    #[tokio::test]
    async fn an_unreachable_api_is_a_connect_error() {
        // Port 1 on loopback is not listening.
        match IpfsPublisher::new("127.0.0.1:1").add(b"RIFF....WAVE").await {
            Err(IpfsError::Connect { .. }) => {}
            other => expected_err(other),
        }
    }

    fn expected_err<T: std::fmt::Debug>(other: T) {
        panic!("expected an error, got {other:?}");
    }
}
