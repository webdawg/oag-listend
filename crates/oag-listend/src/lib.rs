//! `oag-listend` -- the global daemon that serves audio.
//!
//! The founding statement (`USER_INPUT_RECORD.md`, verbatim): "this is the global daemon that
//! serves all audio information to all networks ... a real audio serving service that serves audio
//! to any present and future technology - any way that anyone connects to this service - it just
//! serves audio."
//!
//! The architecture that statement implies is a single stable audio surface behind pluggable
//! carriers. The audio is the service; a carrier is one way to reach it. See `SPEC.md`.
//!
//! The second founding statement (verbatim): "the service also listens too all incoming but only
//! accepting audio as an input and nothing else." That is the inbound rule, enforced at the audio
//! surface so no carrier can forget it: [`AudioStore::accept`] admits bytes only if they carry a
//! recognised audio signature, checked on the bytes and never on a declared type.
//!
//! **Status: founded.** This crate holds the audio surface -- an item, its content hash, the
//! inbound rule, and a store that serves it -- and no carrier yet. The first carrier is the next
//! commit.

#![forbid(unsafe_code)]

use std::collections::HashMap;

/// One piece of audio the daemon serves: bytes and the BLAKE3 of those bytes.
///
/// The hash is the identity. A receiver on any carrier can verify what it got against what it
/// asked for, which is the one property every present and future carrier needs in common.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioItem {
    pub content_hash: String,
    pub bytes: Vec<u8>,
    /// A MIME type if the publisher stated one. Not negotiated or transcoded here: the daemon
    /// serves the bytes it holds, and format is a carrier concern.
    pub media_type: Option<String>,
}

impl AudioItem {
    pub fn new(bytes: Vec<u8>, media_type: Option<String>) -> Self {
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        Self { content_hash, bytes, media_type }
    }

    /// Whether `bytes` are the bytes this item's hash names -- the receiver-side check.
    pub fn verify(content_hash: &str, bytes: &[u8]) -> bool {
        blake3::hash(bytes).to_hex().to_string() == content_hash
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ListendError {
    #[error("no audio item with content hash {0}")]
    NotFound(String),
    /// The inbound rule: the bytes did not carry a recognised audio signature, so they were not
    /// accepted. Carries nothing about the rejected bytes -- a rejection must not become a channel
    /// for the content it rejected.
    #[error("input is not audio; only audio is accepted")]
    NotAudio,
}

/// A recognised audio container or codec, identified from the bytes themselves.
///
/// This is the whole of what the daemon knows about formats: enough to tell audio from
/// not-audio, and no more. It does not decode, and it is deliberately a closed list that grows
/// only when a real carrier needs a format -- the inbound rule is "audio only", and a sniff that
/// accepts anything plausible is no rule at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioFormat {
    /// RIFF container with a WAVE form type.
    Wav,
    /// FLAC stream marker.
    Flac,
    /// Ogg container (Vorbis, Opus, or FLAC-in-Ogg; the daemon does not distinguish).
    Ogg,
    /// MPEG audio: an ID3v2 tag, or a raw frame sync.
    Mp3,
    /// ISO base media (MP4/M4A) with an audio-bearing brand.
    M4a,
    /// AIFF container.
    Aiff,
}

impl AudioFormat {
    /// The format these bytes carry, from their leading signature, or `None` if they are not a
    /// recognised audio format. Checks magic bytes only -- a full parse is a decoder's job.
    pub fn sniff(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
            return Some(Self::Wav);
        }
        if bytes.len() >= 12 && &bytes[0..4] == b"FORM" && (&bytes[8..12] == b"AIFF" || &bytes[8..12] == b"AIFC") {
            return Some(Self::Aiff);
        }
        if bytes.starts_with(b"fLaC") {
            return Some(Self::Flac);
        }
        if bytes.starts_with(b"OggS") {
            return Some(Self::Ogg);
        }
        if bytes.starts_with(b"ID3") {
            return Some(Self::Mp3);
        }
        // A raw MPEG frame: 11 sync bits set, and a valid (non-reserved) layer/version/bitrate.
        if bytes.len() >= 4 && bytes[0] == 0xFF && (bytes[1] & 0xE0) == 0xE0 {
            let version = (bytes[1] >> 3) & 0x03;
            let layer = (bytes[1] >> 1) & 0x03;
            let bitrate = bytes[2] >> 4;
            if version != 0x01 && layer != 0x00 && bitrate != 0x00 && bitrate != 0x0F {
                return Some(Self::Mp3);
            }
        }
        // ISO BMFF: a size, then "ftyp", then a brand. Only audio-bearing brands are accepted; a
        // plain MP4 video has a different brand and is not audio.
        if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
            let brand = &bytes[8..12];
            if matches!(brand, b"M4A " | b"M4B " | b"M4P " | b"mp42" | b"isom") {
                return Some(Self::M4a);
            }
        }
        None
    }
}

/// The audio surface: everything the daemon can serve, keyed by content hash.
///
/// In-memory and bounded by `max_bytes`, because an audio store is a disk-exhaustion vector if
/// it is not. Eviction is oldest-first, the honest default for a store with no usage statistics.
#[derive(Debug)]
pub struct AudioStore {
    items: HashMap<String, AudioItem>,
    order: std::collections::VecDeque<String>,
    bytes_held: usize,
    max_bytes: usize,
}

/// The default store bound. Generous for a daemon, small enough that a misbehaving publisher
/// cannot fill a disk through it.
pub const DEFAULT_MAX_BYTES: usize = 1024 * 1024 * 1024;

impl Default for AudioStore {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_BYTES)
    }
}

impl AudioStore {
    pub fn with_capacity(max_bytes: usize) -> Self {
        Self { items: HashMap::new(), order: Default::default(), bytes_held: 0, max_bytes }
    }

    /// The inbound rule. Accepts `bytes` only if they carry a recognised audio signature, and
    /// then publishes them; anything else is [`ListendError::NotAudio`] and is not stored.
    ///
    /// This is the one entry point every carrier's inbound path goes through, so the rule holds
    /// for a carrier that does not exist yet. The check is on the bytes, never on
    /// `declared_media_type`: a declared type is a claim and the bytes are the evidence. The
    /// declared type is kept on the item as metadata only if the bytes agree it is audio at all.
    pub fn accept(&mut self, bytes: Vec<u8>, declared_media_type: Option<String>) -> Result<String, ListendError> {
        if AudioFormat::sniff(&bytes).is_none() {
            return Err(ListendError::NotAudio);
        }
        Ok(self.publish(AudioItem::new(bytes, declared_media_type)))
    }

    /// Publishes an item, returning its content hash. Re-publishing identical bytes is a no-op
    /// that returns the same hash: the hash is the identity, so there is nothing to add.
    ///
    /// Does **not** apply the inbound rule: this is the trusted, local path for audio the
    /// operator already holds. Anything that arrived over a carrier goes through
    /// [`Self::accept`].
    pub fn publish(&mut self, item: AudioItem) -> String {
        let hash = item.content_hash.clone();
        if self.items.contains_key(&hash) {
            return hash;
        }
        self.bytes_held += item.bytes.len();
        self.items.insert(hash.clone(), item);
        self.order.push_back(hash.clone());
        while self.bytes_held > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else { break };
            if let Some(evicted) = self.items.remove(&oldest) {
                self.bytes_held -= evicted.bytes.len();
            }
        }
        hash
    }

    /// Serves an item by content hash. This is the one operation every carrier wraps.
    pub fn serve(&self, content_hash: &str) -> Result<&AudioItem, ListendError> {
        self.items.get(content_hash).ok_or_else(|| ListendError::NotFound(content_hash.to_string()))
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn bytes_held(&self) -> usize {
        self.bytes_held
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_item_is_served_by_its_content_hash_and_verifies() {
        let mut store = AudioStore::default();
        let item = AudioItem::new(b"not really audio, but bytes".to_vec(), Some("audio/wav".into()));
        let hash = store.publish(item.clone());
        let served = store.serve(&hash).unwrap();
        assert_eq!(served, &item);
        assert!(AudioItem::verify(&hash, &served.bytes), "the receiver-side check must pass");
        assert!(!AudioItem::verify(&hash, b"tampered"), "and must fail on different bytes");
    }

    /// The inbound rule, as a property: a recognised audio signature is accepted, anything else
    /// is rejected and leaves the store untouched. The rejection must not depend on the declared
    /// type -- a lie in the header does not get bytes in.
    #[test]
    fn only_audio_is_accepted_and_the_declared_type_cannot_override_the_bytes() {
        let mut store = AudioStore::default();

        let mut wav = b"RIFF".to_vec();
        wav.extend_from_slice(&[0, 0, 0, 0]);
        wav.extend_from_slice(b"WAVE");
        wav.extend_from_slice(b"fmt ");
        assert!(store.accept(wav.clone(), None).is_ok(), "a WAV signature is audio");

        assert!(store.accept(b"fLaC\0\0\0\x22".to_vec(), None).is_ok(), "FLAC is audio");
        assert!(store.accept(b"OggS\0\x02".to_vec(), None).is_ok(), "Ogg is audio");
        assert!(store.accept(b"ID3\x04\0\0".to_vec(), None).is_ok(), "an ID3 tag is MPEG audio");
        assert!(store.accept(vec![0xFF, 0xFB, 0x90, 0x00], None).is_ok(), "a raw MPEG frame is audio");
        let mut m4a = vec![0, 0, 0, 0x20];
        m4a.extend_from_slice(b"ftypM4A ");
        assert!(store.accept(m4a, None).is_ok(), "an M4A brand is audio");
        let before = store.len();

        // Not audio, however it is labelled.
        for (bytes, label) in [
            (b"<html><body>hello</body></html>".to_vec(), "text/html"),
            (b"{\"json\": true}".to_vec(), "application/json"),
            (b"\x89PNG\r\n\x1a\n".to_vec(), "image/png"),
            (b"%PDF-1.4".to_vec(), "application/pdf"),
            (vec![], "audio/wav"),
            (b"just some text".to_vec(), "audio/mpeg"),
        ] {
            let result = store.accept(bytes, Some(label.to_string()));
            assert!(
                matches!(result, Err(ListendError::NotAudio)),
                "{label} must be rejected regardless of its declared type, got {result:?}"
            );
        }
        assert_eq!(store.len(), before, "a rejected input must not be stored");

        // A video brand is ISO BMFF but not audio.
        let mut mp4_video = vec![0, 0, 0, 0x20];
        mp4_video.extend_from_slice(b"ftypavc1");
        assert!(matches!(store.accept(mp4_video, None), Err(ListendError::NotAudio)));
    }

    #[test]
    fn an_unknown_hash_is_a_typed_error() {
        let store = AudioStore::default();
        assert!(matches!(store.serve("nope"), Err(ListendError::NotFound(_))));
    }

    #[test]
    fn republishing_identical_bytes_is_a_no_op() {
        let mut store = AudioStore::default();
        let a = store.publish(AudioItem::new(vec![1, 2, 3], None));
        let b = store.publish(AudioItem::new(vec![1, 2, 3], None));
        assert_eq!(a, b);
        assert_eq!(store.len(), 1);
        assert_eq!(store.bytes_held(), 3);
    }

    #[test]
    fn the_store_is_bounded_and_evicts_oldest_first() {
        let mut store = AudioStore::with_capacity(10);
        let first = store.publish(AudioItem::new(vec![0; 6], None));
        let second = store.publish(AudioItem::new(vec![1; 6], None));
        assert!(store.serve(&first).is_err(), "the oldest item must be evicted past the byte cap");
        assert!(store.serve(&second).is_ok());
        assert!(store.bytes_held() <= 10);
    }
}
