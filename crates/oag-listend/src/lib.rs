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
//! The daemon is live on four carriers: [`server`] (native TCP frames), [`http`], [`udp`],
//! and [`unix`].
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
pub mod unix;
pub mod wav;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
        Self {
            content_hash,
            bytes,
            media_type,
        }
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
    /// The bytes are audio, but they are larger than the store can hold. Refused rather than
    /// written and immediately evicted: a bound that is not checked on the way in is not a bound.
    #[error("audio is {len} bytes, larger than the {max} byte store bound")]
    TooLarge { len: usize, max: usize },
}

/// What a directory load found. `skipped` counts entries that were not usable audio -- a file
/// whose name is not the BLAKE3 of its bytes, or whose bytes are not a recognised audio format.
/// They are reported, never silently dropped or silently trusted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LoadReport {
    /// Entries now held. Not "entries read": a load can evict older items once the byte bound is
    /// reached, and a report that counts evicted entries as loaded overstates what is served.
    pub loaded: usize,
    /// Entries not read, or read and rejected: a name that is not the BLAKE3 of the bytes, bytes
    /// that do not sniff as audio, or a file larger than the store bound.
    pub skipped: usize,
    /// Entries read and accepted but pushed out by the byte bound while later entries loaded.
    pub evicted: usize,
    /// The directory held more entries than the load will consider. Nothing is deleted or changed
    /// by this; the operator is told instead of the load silently ignoring the rest.
    pub truncated: bool,
}

/// The codec inside an Ogg container, from the first (beginning-of-stream) page.
///
/// Ogg is a container, not a codec: the same `OggS` marker wraps Vorbis, Opus, and FLAC audio, and
/// also Theora video. The BOS page's payload is the codec identification header, so it is the only
/// place that can tell them apart. Only known audio codecs are accepted; anything else -- including
/// a video codec -- is not audio.
fn sniff_ogg_codec(bytes: &[u8]) -> Option<AudioFormat> {
    // header_type 0x02 marks the beginning-of-stream page; page_segments is at 26 and the segment
    // table follows it, with the payload after that.
    if bytes.len() < 28 || bytes[5] != 0x02 {
        return None;
    }
    let segments = bytes[26] as usize;
    let payload = 27 + segments;
    if bytes.len() < payload {
        return None;
    }
    let codec = &bytes[payload..];
    let is_audio = codec.starts_with(b"\x01vorbis")
        || codec.starts_with(b"OpusHead")
        || codec.starts_with(b"\x7fFLAC")
        || codec.starts_with(b"Speex   ")
        || codec.starts_with(b"PCM     ");
    if is_audio {
        Some(AudioFormat::Ogg)
    } else {
        None
    }
}

/// A recognised audio container or codec, identified from the bytes themselves.
///
/// This is the whole of what the daemon knows about formats: enough to tell audio from
/// not-audio, and no more. It does not decode, and it is deliberately a closed list that grows
/// only when a real carrier needs a format -- the inbound rule is "audio only", and a sniff that
/// accepts anything plausible is no rule at all.
///
/// For ISO BMFF (`ftyp`) the whole brand list is read, not just the major brand: `isom` and `mp42`
/// are generic brands that MP4 *video* uses, while ffmpeg-written M4A often has `isom` as the major
/// brand and `M4A ` among the compatible brands. For Ogg, the beginning-of-stream page's codec
/// header is read, because the same `OggS` marker wraps Theora video as well as Vorbis, Opus, and
/// FLAC audio.
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
        if bytes.len() >= 12
            && &bytes[0..4] == b"FORM"
            && (&bytes[8..12] == b"AIFF" || &bytes[8..12] == b"AIFC")
        {
            return Some(Self::Aiff);
        }
        if bytes.starts_with(b"fLaC") {
            return Some(Self::Flac);
        }
        if bytes.starts_with(b"OggS") {
            return sniff_ogg_codec(bytes);
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
        // ISO BMFF: a box size, then "ftyp", then the major brand, a minor version, and zero or
        // more compatible brands. Only an audio-bearing brand is accepted. `isom` and `mp42` are
        // generic brands that MP4 *video* uses, so the major brand alone cannot decide this --
        // ffmpeg-written M4A often has `isom` as the major brand and `M4A ` among the compatible
        // brands. Checking the whole list separates the two.
        if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
            let size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
            // A size of 0 means the box runs to the end of the file; a size of 1 means the real
            // size is a 64-bit field after the minor version, and the compatible brands start
            // after it.
            let (end, first_compatible) = match size {
                0 => (bytes.len(), 16),
                1 if bytes.len() >= 24 => {
                    let large = u64::from_be_bytes([
                        bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                        bytes[22], bytes[23],
                    ]) as usize;
                    (
                        if large == 0 {
                            bytes.len()
                        } else {
                            large.min(bytes.len())
                        },
                        24,
                    )
                }
                _ => (size.min(bytes.len()), 16),
            };
            if matches!(&bytes[8..12], b"M4A " | b"M4B " | b"M4P ") {
                return Some(Self::M4a);
            }
            let mut offset = first_compatible;
            while offset + 4 <= end {
                if matches!(&bytes[offset..offset + 4], b"M4A " | b"M4B " | b"M4P ") {
                    return Some(Self::M4a);
                }
                offset += 4;
            }
        }
        None
    }

    /// The canonical MIME type for this format, derived from the bytes.
    ///
    /// The HTTP carrier serves this rather than a publisher's declared type: a declared type is a
    /// claim, and a browser that is told `application/x-www-form-urlencoded` about a WAV will not
    /// play it. The bytes are the evidence.
    pub fn media_type(&self) -> &'static str {
        match self {
            Self::Wav => "audio/wav",
            Self::Flac => "audio/flac",
            Self::Ogg => "audio/ogg",
            Self::Mp3 => "audio/mpeg",
            Self::M4a => "audio/mp4",
            Self::Aiff => "audio/aiff",
        }
    }

    /// A file extension for the format, for a `Content-Disposition` filename.
    pub fn file_extension(&self) -> &'static str {
        match self {
            Self::Wav => "wav",
            Self::Flac => "flac",
            Self::Ogg => "ogg",
            Self::Mp3 => "mp3",
            Self::M4a => "m4a",
            Self::Aiff => "aiff",
        }
    }
}

/// The audio surface: everything the daemon can serve, keyed by content hash.
///
/// In-memory and bounded by `max_bytes`, because an audio store is a disk-exhaustion vector if
/// it is not. Eviction is oldest-first, the honest default for a store with no usage statistics.
#[derive(Debug)]
pub struct AudioStore {
    items: HashMap<String, Arc<AudioItem>>,
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
    pub fn with_directory(
        dir: impl Into<PathBuf>,
        max_bytes: usize,
    ) -> std::io::Result<(Self, LoadReport)> {
        Self::load_dir(dir, max_bytes, MAX_LOAD_ENTRIES)
    }

    /// See [`Self::with_directory`].
    fn load_dir(
        dir: impl Into<PathBuf>,
        max_bytes: usize,
        max_entries: usize,
    ) -> std::io::Result<(Self, LoadReport)> {
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
        let mut inserted = 0usize;
        let mut considered = 0usize;

        let entries = std::fs::read_dir(&dir)?;
        for entry in entries {
            if considered >= max_entries {
                report.truncated = true;
                break;
            }
            considered += 1;
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
            // Only regular files. A FIFO or device named like a hash would make `std::fs::read`
            // block forever, and a directory would error; both are skipped rather than read.
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => {
                    report.skipped += 1;
                    continue;
                }
            };
            if !metadata.is_file() {
                report.skipped += 1;
                continue;
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
            // A file larger than the store can hold is skipped without being read: an unbounded
            // read on the startup path is the same defect as an unbounded network read.
            let len = metadata.len() as usize;
            if len > servable_bound(store.max_bytes) {
                report.skipped += 1;
                continue;
            }
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
            store.publish(AudioItem {
                content_hash: hash,
                bytes,
                media_type,
            });
            inserted += 1;
        }

        report.evicted = inserted - store.len();
        report.loaded = store.len();
        Ok((store, report))
    }

    /// The inbound rule. Accepts `bytes` only if they carry a recognised audio signature, and
    /// then publishes them; anything else is [`ListendError::NotAudio`] and is not stored.
    ///
    /// This is the one entry point every carrier's inbound path goes through, so the rule holds
    /// for a carrier that does not exist yet. The check is on the bytes, never on
    /// `declared_media_type`: a declared type is a claim and the bytes are the evidence. The
    /// declared type is kept on the item as metadata only if the bytes agree it is audio at all.
    pub async fn accept(
        &mut self,
        bytes: Vec<u8>,
        declared_media_type: Option<String>,
    ) -> Result<String, ListendError> {
        if AudioFormat::sniff(&bytes).is_none() {
            return Err(ListendError::NotAudio);
        }
        let bound = servable_bound(self.max_bytes);
        if bytes.len() > bound {
            return Err(ListendError::TooLarge {
                len: bytes.len(),
                max: bound,
            });
        }
        let item = AudioItem::new(bytes, declared_media_type);
        if let Some(existing) = self.items.get(&item.content_hash).cloned() {
            // The durable copy is the source of truth. If the file went missing out of band (an
            // operator deleted it, or the disk dropped it), a republish restores it, so the item
            // does not silently become one the next start will lose. The existence check costs a
            // syscall only on a duplicate publish, never on the read path.
            if let Some(dir) = &self.dir {
                // `try_exists`, not `Path::exists`: this is the async path, and a blocking stat
                // would stall the executor. An error is treated as absent, so the restore is
                // attempted; `persist` recreates a missing directory and fails loudly if it
                // cannot.
                let present = tokio::fs::try_exists(dir.join(&existing.content_hash))
                    .await
                    .unwrap_or(false);
                if !present {
                    self.persist(dir, &existing).await?;
                }
            }
            return Ok(item.content_hash);
        }
        if let Some(dir) = &self.dir {
            self.persist(dir, &item).await?;
        }
        Ok(self.publish(item))
    }

    /// Write the item under its content hash, plus a sidecar for the declared media type. The
    /// write happens before the item is answerable, so a failure is a refusal.
    /// Write the item under its content hash, plus a sidecar for the declared media type.
    ///
    /// Async, and using `tokio::fs`: the write happens while the store mutex is held, and a blocking
    /// write on a slow disk would stall every carrier's executor thread. The write still completes
    /// before the item is answerable, so a failure is still a refusal.
    async fn persist(&self, dir: &Path, item: &AudioItem) -> Result<(), ListendError> {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| ListendError::Persist(e.to_string()))?;
        tokio::fs::write(dir.join(&item.content_hash), &item.bytes)
            .await
            .map_err(|e| ListendError::Persist(e.to_string()))?;
        if let Some(media_type) = &item.media_type {
            // A sidecar is metadata; a failure to write it is not a failure to store the audio.
            let sidecar = dir.join(format!("{}.json", item.content_hash));
            let _ = tokio::fs::write(sidecar, media_type.as_bytes()).await;
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
    /// [`Self::accept`]. The caller is responsible for [`servable_bound`]; the production paths
    /// (`accept` and the directory load) enforce it, so a direct `publish` of an oversized item
    /// would be stored but not servable.
    pub fn publish(&mut self, item: AudioItem) -> String {
        let hash = item.content_hash.clone();
        if self.items.contains_key(&hash) {
            return hash;
        }
        self.bytes_held += item.bytes.len();
        self.items.insert(hash.clone(), Arc::new(item));
        self.order.push_back(hash.clone());
        while self.bytes_held > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.items.remove(&oldest) {
                self.bytes_held -= evicted.bytes.len();
                self.forget(&oldest);
            }
        }
        hash
    }

    /// Serves an item by content hash. This is the one operation every carrier wraps.
    ///
    /// Returns an `Arc`, not a reference: the caller clones the handle under the lock and copies
    /// the bytes after releasing it. A large fetch therefore does not hold the store mutex during
    /// a multi-megabyte copy, which would stall every other operation.
    pub fn serve(&self, content_hash: &str) -> Result<Arc<AudioItem>, ListendError> {
        self.items
            .get(content_hash)
            .cloned()
            .ok_or_else(|| ListendError::NotFound(content_hash.to_string()))
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

    /// Every content hash currently held, sorted by hash.
    ///
    /// Sorted rather than insertion order because a restart rebuilds the store in
    /// directory-iteration order, which is filesystem-dependent. Insertion order is reproducible
    /// within a run and not across one; a sorted order is reproducible always, and a listing whose
    /// walk changes between a client's requests -- or across a daemon restart -- is a
    /// reproducibility hazard with no benefit.
    pub fn hashes(&self) -> Vec<String> {
        let mut hashes: Vec<String> = self.items.keys().cloned().collect();
        hashes.sort();
        hashes
    }

    /// The byte bound this store enforces.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
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

/// The largest item this store will hold and serve: the store bound, capped by what a single
/// frame can carry. A store bound above the frame cap would otherwise admit items that no carrier
/// can fetch.
pub fn servable_bound(max_bytes: usize) -> usize {
    max_bytes.min(crate::protocol::MAX_SERVABLE_BYTES)
}

/// The maximum number of directory entries one load will consider. A store directory is an
/// operator-controlled path; a mistake (or a symlink to somewhere huge) must not make startup
/// process an unbounded number of entries. The load reports `truncated` rather than ignoring the
/// rest silently.
pub const MAX_LOAD_ENTRIES: usize = 100_000;

/// The largest sidecar that will be read. A media type is a few dozen bytes; anything larger is
/// not a media type and is ignored rather than pulled into memory.
const MAX_SIDECAR_BYTES: usize = 4096;

/// The declared media type recorded beside a persisted item, if any.
fn read_sidecar(dir: &Path, hash: &str) -> Option<String> {
    let path = dir.join(format!("{hash}.json"));
    if std::fs::metadata(&path).ok()?.len() as usize > MAX_SIDECAR_BYTES {
        return None;
    }
    let raw = std::fs::read(path).ok()?;
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
    fn every_recognised_format_maps_to_an_audio_media_type() {
        for (format, expected) in [
            (AudioFormat::Wav, "audio/wav"),
            (AudioFormat::Flac, "audio/flac"),
            (AudioFormat::Ogg, "audio/ogg"),
            (AudioFormat::Mp3, "audio/mpeg"),
            (AudioFormat::M4a, "audio/mp4"),
            (AudioFormat::Aiff, "audio/aiff"),
        ] {
            assert_eq!(format.media_type(), expected);
            assert!(format.media_type().starts_with("audio/"));
            assert!(!format.file_extension().contains('.'));
        }
    }

    #[test]
    fn an_ftyp_box_that_runs_to_the_end_of_the_file_is_still_sniffed() {
        // size 0 means "to the end of the file", which is legal and must not hide the brands.
        let mut bytes = vec![0, 0, 0, 0];
        bytes.extend_from_slice(b"ftypisom");
        bytes.extend_from_slice(&[0, 0, 0, 1]);
        bytes.extend_from_slice(b"M4A ");
        assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::M4a));

        // size 1 means a 64-bit largesize follows the minor version.
        let mut bytes = vec![0, 0, 0, 1];
        bytes.extend_from_slice(b"ftypisom");
        bytes.extend_from_slice(&[0, 0, 0, 1]);
        let total = bytes.len() as u64 + 12; // this 8-byte field plus the brand that follows
        bytes.extend_from_slice(&total.to_be_bytes());
        bytes.extend_from_slice(b"M4A ");
        assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::M4a));
    }

    #[test]
    fn an_item_is_served_by_its_content_hash_and_verifies() {
        let mut store = AudioStore::default();
        let item = AudioItem::new(
            b"not really audio, but bytes".to_vec(),
            Some("audio/wav".into()),
        );
        let hash = store.publish(item.clone());
        let served = store.serve(&hash).unwrap();
        assert_eq!(served.as_ref(), &item);
        assert!(
            AudioItem::verify(&hash, &served.bytes),
            "the receiver-side check must pass"
        );
        assert!(
            !AudioItem::verify(&hash, b"tampered"),
            "and must fail on different bytes"
        );
    }

    /// The inbound rule, as a property: a recognised audio signature is accepted, anything else
    /// is rejected and leaves the store untouched. The rejection must not depend on the declared
    /// type -- a lie in the header does not get bytes in.
    #[tokio::test]
    async fn only_audio_is_accepted_and_the_declared_type_cannot_override_the_bytes() {
        let mut store = AudioStore::default();

        let mut wav = b"RIFF".to_vec();
        wav.extend_from_slice(&[0, 0, 0, 0]);
        wav.extend_from_slice(b"WAVE");
        wav.extend_from_slice(b"fmt ");
        assert!(
            store.accept(wav.clone(), None).await.is_ok(),
            "a WAV signature is audio"
        );

        assert!(
            store.accept(b"fLaC\0\0\0\x22".to_vec(), None).await.is_ok(),
            "FLAC is audio"
        );
        assert!(
            store.accept(ogg_bos(b"\x01vorbis"), None).await.is_ok(),
            "Ogg Vorbis is audio"
        );
        assert!(
            store.accept(ogg_bos(b"OpusHead"), None).await.is_ok(),
            "Ogg Opus is audio"
        );
        assert!(
            matches!(
                store.accept(ogg_bos(b"\x80theora"), None).await,
                Err(ListendError::NotAudio)
            ),
            "Ogg Theora is video, not audio"
        );
        assert!(
            store.accept(b"ID3\x04\0\0".to_vec(), None).await.is_ok(),
            "an ID3 tag is MPEG audio"
        );
        assert!(
            store
                .accept(vec![0xFF, 0xFB, 0x90, 0x00], None)
                .await
                .is_ok(),
            "a raw MPEG frame is audio"
        );
        let mut m4a = vec![0, 0, 0, 0x20];
        m4a.extend_from_slice(b"ftypM4A ");
        assert!(
            store.accept(m4a, None).await.is_ok(),
            "an M4A brand is audio"
        );
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
            let result = store.accept(bytes, Some(label.to_string())).await;
            assert!(
                matches!(result, Err(ListendError::NotAudio)),
                "{label} must be rejected regardless of its declared type, got {result:?}"
            );
        }
        assert_eq!(store.len(), before, "a rejected input must not be stored");

        // A video brand is ISO BMFF but not audio.
        let mut mp4_video = vec![0, 0, 0, 0x20];
        mp4_video.extend_from_slice(b"ftypavc1");
        assert!(matches!(
            store.accept(mp4_video, None).await,
            Err(ListendError::NotAudio)
        ));

        // A generic `isom` major brand with no audio brand anywhere is video, not audio.
        let mut isom_video = vec![0, 0, 0, 0x20];
        isom_video.extend_from_slice(b"ftypisom");
        isom_video.extend_from_slice(&[0, 0, 0, 1]);
        isom_video.extend_from_slice(b"avc1");
        assert!(matches!(
            store.accept(isom_video, None).await,
            Err(ListendError::NotAudio)
        ));

        // The same generic major brand with `M4A ` among the compatible brands is audio.
        let mut isom_m4a = vec![0, 0, 0, 0x24];
        isom_m4a.extend_from_slice(b"ftypisom");
        isom_m4a.extend_from_slice(&[0, 0, 0, 1]);
        isom_m4a.extend_from_slice(b"M4A ");
        assert!(
            store.accept(isom_m4a, None).await.is_ok(),
            "an audio compatible brand must be accepted"
        );
    }

    #[test]
    fn an_unknown_hash_is_a_typed_error() {
        let store = AudioStore::default();
        assert!(matches!(
            store.serve("nope"),
            Err(ListendError::NotFound(_))
        ));
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

    /// A minimal Ogg beginning-of-stream page carrying `codec` as its identification header.
    fn ogg_bos(codec: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"OggS");
        out.push(0); // stream structure version
        out.push(0x02); // beginning of stream
        out.extend_from_slice(&[0u8; 8]); // granule position
        out.extend_from_slice(&[0u8; 4]); // serial number
        out.extend_from_slice(&[0u8; 4]); // page sequence
        out.extend_from_slice(&[0u8; 4]); // checksum
        out.push(1); // one segment
        out.push(codec.len() as u8);
        out.extend_from_slice(codec);
        out
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

    #[tokio::test]
    async fn accepted_audio_survives_a_reopen() {
        let dir = temp_dir("reopen");
        let wav = wav::generate(1, wav::Sound::Tone);
        {
            let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
            let hash = store
                .accept(wav.clone(), Some("audio/wav".into()))
                .await
                .unwrap();
            assert_eq!(store.serve(&hash).unwrap().bytes, wav);
        }
        let (store, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(store.serve(&store.hashes()[0]).unwrap().bytes, wav);
        assert_eq!(
            store
                .serve(&store.hashes()[0])
                .unwrap()
                .media_type
                .as_deref(),
            Some("audio/wav")
        );
        let _ = store;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_file_that_is_not_its_hash_is_skipped() {
        let dir = temp_dir("badhash");
        let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        let wav = wav::generate(2, wav::Sound::Tone);
        let hash = store.accept(wav.clone(), None).await.unwrap();
        // Corrupt the payload: the name no longer names the bytes.
        std::fs::write(dir.join(&hash), b"tampered").unwrap();
        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 0);
        assert_eq!(report.skipped, 1);
        assert!(reopened.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_republish_restores_a_file_deleted_out_of_band() {
        let dir = temp_dir("selfheal");
        let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        let wav = wav::generate(11, wav::Sound::Tone);
        let hash = store.accept(wav.clone(), None).await.unwrap();
        assert!(dir.join(&hash).exists());
        std::fs::remove_file(dir.join(&hash)).unwrap();

        // The same bytes again: the in-memory item is still served, and the durable copy is
        // restored rather than the item becoming one a restart would lose.
        let again = store.accept(wav, None).await.unwrap();
        assert_eq!(again, hash);
        assert!(
            dir.join(&hash).exists(),
            "a republish must restore the file"
        );

        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 1);
        assert!(reopened.serve(&hash).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_load_is_capped_and_reports_the_truncation() {
        let dir = temp_dir("capped");
        let (mut seed, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        for seed_n in 0..3u64 {
            let wav = wav::generate_n(seed_n, wav::Sound::Tone, 4);
            seed.accept(wav, None).await.unwrap();
        }
        // A cap below the number of entries: the load stops and says so.
        let (store, report) = AudioStore::load_dir(&dir, 1024 * 1024, 2).unwrap();
        assert!(report.truncated, "the cap must be reported");
        assert_eq!(store.len(), 2, "only the cap's worth is considered");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_non_regular_file_named_like_a_hash_is_skipped_not_read() {
        let dir = temp_dir("nonregular");
        // A directory (and, in production, a FIFO or device) named like a hash must be skipped
        // without being read: reading a FIFO blocks forever.
        let name = "ab".repeat(32);
        std::fs::create_dir(dir.join(&name)).unwrap();
        let (store, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 0);
        assert_eq!(report.skipped, 1);
        assert!(store.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_oversized_sidecar_is_ignored() {
        let dir = temp_dir("sidecar");
        let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        let wav = wav::generate(9, wav::Sound::Tone);
        let hash = store.accept(wav, Some("audio/wav".into())).await.unwrap();
        // A sidecar far larger than any media type must not be read into memory.
        std::fs::write(dir.join(format!("{hash}.json")), vec![b'x'; 100_000]).unwrap();
        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 1);
        assert_eq!(
            reopened.serve(&hash).unwrap().media_type,
            None,
            "an oversized sidecar is ignored, not trusted"
        );
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

    #[tokio::test]
    async fn eviction_removes_the_file_so_disk_and_store_agree() {
        let dir = temp_dir("evict");
        // Each tiny WAV is 52 bytes; the bound is 60, so the second accept evicts the first.
        let (mut store, _) = AudioStore::with_directory(&dir, 60).unwrap();
        let first = wav::generate_n(1, wav::Sound::Tone, 4);
        let second = wav::generate_n(2, wav::Sound::Tone, 4);
        let first_hash = store.accept(first, None).await.unwrap();
        assert!(dir.join(&first_hash).exists());
        let second_hash = store.accept(second, None).await.unwrap();

        assert!(
            store.serve(&first_hash).is_err(),
            "the first item must be evicted"
        );
        assert!(store.serve(&second_hash).is_ok());
        assert!(
            !dir.join(&first_hash).exists(),
            "eviction must delete the file, not orphan it"
        );
        assert!(dir.join(&second_hash).exists());
        assert!(store.bytes_held() <= 60);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_item_bound_is_the_store_bound_capped_by_a_frame() {
        assert_eq!(servable_bound(10), 10, "a small store bound is unchanged");
        assert_eq!(
            servable_bound(usize::MAX),
            crate::protocol::MAX_SERVABLE_BYTES,
            "a large store bound is capped by what a frame can carry"
        );
        assert!(servable_bound(usize::MAX) < crate::protocol::MAX_FRAME_BYTES);
    }

    #[tokio::test]
    async fn an_item_larger_than_the_store_bound_is_refused() {
        let mut store = AudioStore::with_capacity(10);
        let big = wav::generate_n(1, wav::Sound::Tone, 8); // 44 + 16 = 60 bytes
        assert!(big.len() > 10);
        let result = store.accept(big, None).await;
        assert!(
            matches!(result, Err(ListendError::TooLarge { .. })),
            "an item larger than the bound must be refused, got {result:?}"
        );
        assert!(store.is_empty(), "a refused item must not be stored");
    }

    #[test]
    fn the_load_report_counts_only_what_is_held() {
        let dir = temp_dir("report");
        // Three 52-byte WAVs written straight to the directory, bound 120: all three are read,
        // then the oldest is pushed out. (Writing them through a store would have deleted the
        // evicted file already, which is not the case under test.)
        let mut hashes = Vec::new();
        for seed in [1, 2, 3] {
            let wav = wav::generate_n(seed, wav::Sound::Tone, 4);
            let hash = blake3::hash(&wav).to_hex().to_string();
            std::fs::write(dir.join(&hash), &wav).unwrap();
            hashes.push(hash);
        }
        let (reopened, report) = AudioStore::with_directory(&dir, 120).unwrap();
        assert_eq!(report.loaded, 2, "only what the bound holds is loaded");
        assert_eq!(
            report.evicted, 1,
            "the pushed-out entry is reported, not hidden"
        );
        assert_eq!(reopened.len(), 2);
        assert!(hashes.iter().any(|h| reopened.serve(h).is_err()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_file_larger_than_the_store_bound_is_skipped_without_being_read() {
        let dir = temp_dir("toolarge");
        let (mut store, _) = AudioStore::with_directory(&dir, 64).unwrap();
        // A valid WAV whose bytes exceed the bound, named with its real hash.
        let big = wav::generate_n(2, wav::Sound::Tone, 64);
        let hash = blake3::hash(&big).to_hex().to_string();
        std::fs::write(dir.join(&hash), &big).unwrap();
        assert!(big.len() > 64);

        // A second, small item so the load has something to accept.
        let small = wav::generate_n(3, wav::Sound::Tone, 4);
        let small_hash = store.accept(small, None).await.unwrap();

        let (reopened, report) = AudioStore::with_directory(&dir, 64).unwrap();
        assert_eq!(report.loaded, 1, "only the item that fits is loaded");
        assert_eq!(
            report.skipped, 1,
            "the oversized file is skipped and counted"
        );
        assert!(reopened.serve(&small_hash).is_ok());
        assert!(reopened.serve(&hash).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_persist_failure_refuses_the_inbound() {
        // A path that is a file cannot be a directory, so create_dir_all fails.
        let blocker = temp_dir("blocked");
        let file = blocker.join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let mut store = AudioStore::with_directory_path_for_test(file, 1024 * 1024);
        let result = store.accept(wav::generate(3, wav::Sound::Tone), None).await;
        assert!(
            matches!(result, Err(ListendError::Persist(_))),
            "got {result:?}"
        );
        let _ = std::fs::remove_dir_all(&blocker);
    }

    #[test]
    fn the_store_is_bounded_and_evicts_oldest_first() {
        let mut store = AudioStore::with_capacity(10);
        let first = store.publish(AudioItem::new(vec![0; 6], None));
        let second = store.publish(AudioItem::new(vec![1; 6], None));
        assert!(
            store.serve(&first).is_err(),
            "the oldest item must be evicted past the byte cap"
        );
        assert!(store.serve(&second).is_ok());
        assert!(store.bytes_held() <= 10);
    }
}
