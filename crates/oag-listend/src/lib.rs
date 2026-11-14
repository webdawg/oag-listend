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
//! The daemon is live on three carriers: [`server`] (native TCP frames), [`http`], and [`udp`].
//! [`protocol`] is the audio-only frame format they share, and [`wav`] generates the sounds a
//! fresh daemon publishes. The rules did not move -- every inbound path still goes through
//! [`AudioStore::accept`], checked on bytes.
//!
//! The store is content-addressed: an item's name is the BLAKE3 of its bytes, so it can be
//! written to a directory and found again without an index. [`AudioStore::with_directory`] opens
//! such a directory, and every [`AudioStore::accept`] into it persists before it is answerable.
//!
//! **Status: serves, and survives a restart.** See `SPEC.md` for the carriers and the bounds.

#![forbid(unsafe_code)]

pub mod http;
pub mod protocol;
pub mod server;
pub mod udp;
pub mod wav;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use protocol::{Request, Response};
pub use server::ListendServer;

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
    /// The audio was accepted by the inbound rule but could not be persisted. It is refused
    /// rather than answered as though it were stored: a daemon that loses audio silently is
    /// worse than one that says so.
    #[error("could not persist the audio: {0}")]
    Persist(String),
}

/// What a directory load found. `skipped` counts entries that were not usable audio -- a file
/// whose name is not the BLAKE3 of its bytes, or whose bytes are not a recognised audio format.
/// They are reported, never silently dropped or silently trusted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LoadReport {
    pub loaded: usize,
    pub skipped: usize,
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
    /// When `Some`, every accepted item is written here under its content hash, and a restart
    /// loads it back. `None` is the in-memory store the tests and the default use.
    dir: Option<PathBuf>,
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
        Self {
            items: HashMap::new(),
            order: Default::default(),
            bytes_held: 0,
            max_bytes,
            dir: None,
        }
    }

    /// Open a content-addressed directory: `<dir>/<hash>` is the audio, `<dir>/<hash>.json` is an
    /// optional sidecar holding the declared media type. Every entry is re-checked on the way in
    /// -- the name must be the BLAKE3 of the bytes and the bytes must still sniff as audio -- so a
    /// hand-placed file cannot smuggle anything into the store.
    ///
    /// The load is reported, not silent: an entry that fails either check is skipped and counted.
    pub fn with_directory(dir: impl Into<PathBuf>, max_bytes: usize) -> std::io::Result<(Self, LoadReport)> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let mut store = Self {
            items: HashMap::new(),
            order: Default::default(),
            bytes_held: 0,
            max_bytes,
            dir: Some(dir.clone()),
        };
        let mut report = LoadReport::default();

        let entries = std::fs::read_dir(&dir)?;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    report.skipped += 1;
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().is_some() {
                continue; // a sidecar, not audio
            }
            let hash = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) if name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()) => {
                    name.to_string()
                }
                _ => {
                    report.skipped += 1;
                    continue;
                }
            };
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => {
                    report.skipped += 1;
                    continue;
                }
            };
            if !AudioItem::verify(&hash, &bytes) || AudioFormat::sniff(&bytes).is_none() {
                report.skipped += 1;
                continue;
            }
            let media_type = read_sidecar(&dir, &hash);
            store.publish(AudioItem { content_hash: hash, bytes, media_type });
            report.loaded += 1;
        }

        Ok((store, report))
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
        let item = AudioItem::new(bytes, declared_media_type);
        if self.items.contains_key(&item.content_hash) {
            return Ok(item.content_hash);
        }
        if let Some(dir) = &self.dir {
            self.persist(dir, &item)?;
        }
        Ok(self.publish(item))
    }

    /// Write the item under its content hash, plus a sidecar for the declared media type. The
    /// write happens before the item is answerable, so a failure is a refusal.
    fn persist(&self, dir: &Path, item: &AudioItem) -> Result<(), ListendError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| ListendError::Persist(e.to_string()))?;
        std::fs::write(dir.join(&item.content_hash), &item.bytes)
            .map_err(|e| ListendError::Persist(e.to_string()))?;
        if let Some(media_type) = &item.media_type {
            // A sidecar is metadata; a failure to write it is not a failure to store the audio.
            let sidecar = dir.join(format!("{}.json", item.content_hash));
            let _ = std::fs::write(sidecar, media_type.as_bytes());
        }
        Ok(())
    }

    /// Remove an evicted item's file, so the directory and the store agree on what is held.
    fn forget(&self, content_hash: &str) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_file(dir.join(content_hash));
            let _ = std::fs::remove_file(dir.join(format!("{content_hash}.json")));
        }
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
                self.forget(&oldest);
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

    /// Every content hash currently held, in the order the items arrived.
    ///
    /// Order is part of the contract: a client that walks the listing twice must see the same
    /// walk, and a listing whose order changed between requests would be a reproducibility
    /// hazard with no benefit.
    pub fn hashes(&self) -> Vec<String> {
        self.order
            .iter()
            .filter(|hash| self.items.contains_key(*hash))
            .cloned()
            .collect()
    }

    /// The backing directory, if this store persists.
    pub fn directory(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// Skip the load and the `create_dir_all` check, for the one test that needs a store whose
    /// directory is deliberately unusable.
    #[cfg(test)]
    pub(crate) fn with_directory_path_for_test(dir: PathBuf, max_bytes: usize) -> Self {
        Self {
            items: HashMap::new(),
            order: Default::default(),
            bytes_held: 0,
            max_bytes,
            dir: Some(dir),
        }
    }
}

/// The declared media type recorded beside a persisted item, if any.
fn read_sidecar(dir: &Path, hash: &str) -> Option<String> {
    let raw = std::fs::read(dir.join(format!("{hash}.json"))).ok()?;
    let text = String::from_utf8(raw).ok()?;
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
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

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "oag-listend-store-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn accepted_audio_survives_a_reopen() {
        let dir = temp_dir("reopen");
        let wav = wav::generate(1, wav::Sound::Tone);
        {
            let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
            let hash = store.accept(wav.clone(), Some("audio/wav".into())).unwrap();
            assert_eq!(store.serve(&hash).unwrap().bytes, wav);
        }
        let (store, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(store.serve(&store.hashes()[0]).unwrap().bytes, wav);
        assert_eq!(store.serve(&store.hashes()[0]).unwrap().media_type.as_deref(), Some("audio/wav"));
        let _ = store;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_that_is_not_its_hash_is_skipped() {
        let dir = temp_dir("badhash");
        let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        let wav = wav::generate(2, wav::Sound::Tone);
        let hash = store.accept(wav.clone(), None).unwrap();
        // Corrupt the payload: the name no longer names the bytes.
        std::fs::write(dir.join(&hash), b"tampered").unwrap();
        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 0);
        assert_eq!(report.skipped, 1);
        assert!(reopened.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_non_audio_file_in_the_directory_is_skipped() {
        let dir = temp_dir("nonaudio");
        let name = "ab".repeat(32);
        std::fs::write(dir.join(&name), b"<html>").unwrap();
        let (store, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 0);
        assert_eq!(report.skipped, 1);
        assert!(store.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_removes_the_file_so_disk_and_store_agree() {
        let dir = temp_dir("evict");
        // Each tiny WAV is 52 bytes; the bound is 60, so the second accept evicts the first.
        let (mut store, _) = AudioStore::with_directory(&dir, 60).unwrap();
        let first = wav::generate_n(1, wav::Sound::Tone, 4);
        let second = wav::generate_n(2, wav::Sound::Tone, 4);
        let first_hash = store.accept(first, None).unwrap();
        assert!(dir.join(&first_hash).exists());
        let second_hash = store.accept(second, None).unwrap();

        assert!(store.serve(&first_hash).is_err(), "the first item must be evicted");
        assert!(store.serve(&second_hash).is_ok());
        assert!(!dir.join(&first_hash).exists(), "eviction must delete the file, not orphan it");
        assert!(dir.join(&second_hash).exists());
        assert!(store.bytes_held() <= 60);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_persist_failure_refuses_the_inbound() {
        // A path that is a file cannot be a directory, so create_dir_all fails.
        let blocker = temp_dir("blocked");
        let file = blocker.join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let mut store = AudioStore::with_directory_path_for_test(file, 1024);
        let result = store.accept(wav::generate(3, wav::Sound::Tone), None);
        assert!(matches!(result, Err(ListendError::Persist(_))), "got {result:?}");
        let _ = std::fs::remove_dir_all(&blocker);
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
