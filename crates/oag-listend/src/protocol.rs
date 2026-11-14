//! The wire protocol: length-prefixed frames carrying one operation and bytes.
//!
//! This is the whole of what a carrier may say to the daemon. There is no command channel, no
//! JSON-RPC, no path or method string a client could use to make the daemon do something other
//! than audio. The operations are exactly: identify, publish audio, fetch audio, list audio.
//!
//! Every frame is `u32` big-endian length followed by that many body bytes, and no body may
//! exceed [`MAX_FRAME_BYTES`]. The length cap is enforced before the body is allocated, so a
//! client cannot make the daemon allocate on its word.

#![forbid(unsafe_code)]

/// The largest frame a client may send. 16 MiB is far larger than any test fixture and far
/// smaller than a disk.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

const OP_HELLO: u8 = 0x01;
const OP_PUBLISH: u8 = 0x02;
const OP_FETCH: u8 = 0x03;
const OP_LIST: u8 = 0x04;

const OP_RESP_HELLO: u8 = 0x10;
const OP_RESP_AUDIO: u8 = 0x11;
const OP_RESP_REJECTED: u8 = 0x12;
const OP_RESP_NOT_FOUND: u8 = 0x13;
const OP_RESP_LISTING: u8 = 0x14;
const OP_RESP_PUBLISHED: u8 = 0x15;

const HASH_LEN: usize = 64;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("the frame is truncated")]
    Truncated,
    #[error("the frame length {0} exceeds the {MAX_FRAME_BYTES} byte cap")]
    TooLong(usize),
    #[error("unknown operation {0:#04x}")]
    UnknownOp(u8),
    #[error("a length-prefixed string is malformed")]
    BadString,
    #[error("a content hash must be {HASH_LEN} hex characters")]
    BadHash,
}

/// A client request. Constructed by a carrier, never by the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// What are you, and how much audio do you hold.
    Hello,
    /// Here are audio bytes. `declared_media_type` is metadata the client claims; the daemon
    /// decides on the bytes.
    Publish { declared_media_type: Option<String>, bytes: Vec<u8> },
    /// Give me the audio with this content hash.
    Fetch { content_hash: String },
    /// What content hashes do you hold.
    List,
}

/// A daemon response. Every request gets exactly one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Hello { item_count: usize },
    /// Audio bytes, with the hash they must verify against.
    Audio { content_hash: String, bytes: Vec<u8> },
    /// The inbound rule refused these bytes. `reason` describes the refusal, never the bytes.
    Rejected { reason: String },
    NotFound { content_hash: String },
    Listing { content_hashes: Vec<String> },
    Published { content_hash: String },
}

// ---------------------------------------------------------------- encoding

/// Wrap a body in the length prefix. The caller supplies the body; the prefix is the only framing.
pub fn encode_frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Validate a complete buffer and return its body, enforcing the length cap before trusting the
/// length. `TooLong` is returned even when the buffer is otherwise incomplete, because the cap is
/// the property that matters.
pub fn decode_frame(buf: &[u8]) -> Result<&[u8], WireError> {
    if buf.len() < 4 {
        return Err(WireError::Truncated);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(WireError::TooLong(len));
    }
    if buf.len() < 4 + len {
        return Err(WireError::Truncated);
    }
    Ok(&buf[4..4 + len])
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u16(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
}

fn put_opt_str(out: &mut Vec<u8>, s: &Option<String>) {
    match s {
        Some(s) => put_str(out, s),
        None => put_u16(out, 0),
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self.pos.checked_add(n).ok_or(WireError::Truncated)?;
        if end > self.buf.len() {
            return Err(WireError::Truncated);
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, WireError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn string(&mut self) -> Result<String, WireError> {
        let len = self.u16()? as usize;
        if len > self.buf.len() {
            return Err(WireError::BadString);
        }
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| WireError::BadString)
    }

    fn opt_string(&mut self) -> Result<Option<String>, WireError> {
        let len = self.u16()? as usize;
        if len == 0 {
            return Ok(None);
        }
        if len > self.buf.len() {
            return Err(WireError::BadString);
        }
        let bytes = self.take(len)?;
        Ok(Some(String::from_utf8(bytes.to_vec()).map_err(|_| WireError::BadString)?))
    }

    fn hash(&mut self) -> Result<String, WireError> {
        let s = self.string()?;
        if s.len() != HASH_LEN || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(WireError::BadHash);
        }
        Ok(s)
    }

    fn rest(&mut self) -> &'a [u8] {
        let out = &self.buf[self.pos..];
        self.pos = self.buf.len();
        out
    }
}

pub fn encode_request(req: &Request) -> Vec<u8> {
    let mut out = Vec::new();
    match req {
        Request::Hello => out.push(OP_HELLO),
        Request::List => out.push(OP_LIST),
        Request::Publish { declared_media_type, bytes } => {
            out.push(OP_PUBLISH);
            put_opt_str(&mut out, declared_media_type);
            out.extend_from_slice(bytes);
        }
        Request::Fetch { content_hash } => {
            out.push(OP_FETCH);
            put_str(&mut out, content_hash);
        }
    }
    out
}

pub fn decode_request(body: &[u8]) -> Result<Request, WireError> {
    let mut r = Reader::new(body);
    let op = r.u8()?;
    match op {
        OP_HELLO => Ok(Request::Hello),
        OP_LIST => Ok(Request::List),
        OP_PUBLISH => {
            let declared_media_type = r.opt_string()?;
            Ok(Request::Publish { declared_media_type, bytes: r.rest().to_vec() })
        }
        OP_FETCH => Ok(Request::Fetch { content_hash: r.hash()? }),
        other => Err(WireError::UnknownOp(other)),
    }
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
    let mut out = Vec::new();
    match resp {
        Response::Hello { item_count } => {
            out.push(OP_RESP_HELLO);
            put_u32(&mut out, *item_count as u32);
        }
        Response::Audio { content_hash, bytes } => {
            out.push(OP_RESP_AUDIO);
            put_str(&mut out, content_hash);
            put_u32(&mut out, bytes.len() as u32);
            out.extend_from_slice(bytes);
        }
        Response::Rejected { reason } => {
            out.push(OP_RESP_REJECTED);
            put_str(&mut out, reason);
        }
        Response::NotFound { content_hash } => {
            out.push(OP_RESP_NOT_FOUND);
            put_str(&mut out, content_hash);
        }
        Response::Listing { content_hashes } => {
            out.push(OP_RESP_LISTING);
            put_u32(&mut out, content_hashes.len() as u32);
            for h in content_hashes {
                put_str(&mut out, h);
            }
        }
        Response::Published { content_hash } => {
            out.push(OP_RESP_PUBLISHED);
            put_str(&mut out, content_hash);
        }
    }
    out
}

pub fn decode_response(body: &[u8]) -> Result<Response, WireError> {
    let mut r = Reader::new(body);
    let op = r.u8()?;
    match op {
        OP_RESP_HELLO => Ok(Response::Hello { item_count: r.u32()? as usize }),
        OP_RESP_AUDIO => {
            let content_hash = r.hash()?;
            let len = r.u32()? as usize;
            if len > r.buf.len() {
                return Err(WireError::Truncated);
            }
            Ok(Response::Audio { content_hash, bytes: r.take(len)?.to_vec() })
        }
        OP_RESP_REJECTED => Ok(Response::Rejected { reason: r.string()? }),
        OP_RESP_NOT_FOUND => Ok(Response::NotFound { content_hash: r.hash()? }),
        OP_RESP_LISTING => {
            let count = r.u32()? as usize;
            if count > r.buf.len() {
                return Err(WireError::Truncated);
            }
            let mut content_hashes = Vec::with_capacity(count);
            for _ in 0..count {
                content_hashes.push(r.hash()?);
            }
            Ok(Response::Listing { content_hashes })
        }
        OP_RESP_PUBLISHED => Ok(Response::Published { content_hash: r.hash()? }),
        other => Err(WireError::UnknownOp(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(req: &Request) {
        let body = encode_request(req);
        assert_eq!(&decode_request(&body).unwrap(), req);
    }

    fn roundtrip_response(resp: &Response) {
        let body = encode_response(resp);
        assert_eq!(&decode_response(&body).unwrap(), resp);
    }

    #[test]
    fn requests_round_trip() {
        roundtrip_request(&Request::Hello);
        roundtrip_request(&Request::List);
        roundtrip_request(&Request::Publish { declared_media_type: None, bytes: b"RIFF....WAVE".to_vec() });
        roundtrip_request(&Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: vec![1, 2, 3],
        });
        roundtrip_request(&Request::Fetch {
            content_hash: "ab".repeat(32),
        });
    }

    #[test]
    fn responses_round_trip() {
        roundtrip_response(&Response::Hello { item_count: 3 });
        roundtrip_response(&Response::Audio {
            content_hash: "cd".repeat(32),
            bytes: vec![9, 8, 7],
        });
        roundtrip_response(&Response::Rejected { reason: "input is not audio".into() });
        roundtrip_response(&Response::NotFound { content_hash: "00".repeat(32) });
        roundtrip_response(&Response::Listing { content_hashes: vec!["11".repeat(32), "22".repeat(32)] });
        roundtrip_response(&Response::Published { content_hash: "33".repeat(32) });
    }

    #[test]
    fn a_frame_longer_than_the_cap_is_refused_before_it_is_trusted() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes());
        assert_eq!(decode_frame(&buf), Err(WireError::TooLong(MAX_FRAME_BYTES + 1)));
    }

    #[test]
    fn a_short_or_unknown_frame_is_a_typed_error() {
        assert_eq!(decode_frame(&[0, 0]), Err(WireError::Truncated));
        assert_eq!(decode_request(&[0x7f]), Err(WireError::UnknownOp(0x7f)));
        assert_eq!(
            decode_request(&[OP_FETCH, 0, 3, b'a', b'b', b'c']),
            Err(WireError::BadHash)
        );
    }

    #[test]
    fn a_malformed_length_cannot_make_the_reader_run_off_the_end() {
        let mut body = vec![OP_PUBLISH, 0xff, 0xff];
        body.extend_from_slice(b"tiny");
        assert_eq!(decode_request(&body), Err(WireError::BadString));
    }
}
