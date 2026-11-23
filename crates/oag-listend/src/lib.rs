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
//! The daemon is live on six ways in: [`server`] (native TCP frames), [`http`], [`udp`],
//! [`unix`], the [`ipfs`] mirror, and -- behind a cargo feature -- `reticulum`. The four socket
//! carriers are the ones that bind an address; IPFS reaches audio this daemon already holds and
//! fetches the rest from a node, and Reticulum serves over its own TCP interface.
//! [`protocol`] is the audio-only frame format the socket carriers share, and [`wav`] generates
//! the sounds a fresh daemon publishes. The rules did not move -- every inbound path still goes through
//! [`AudioStore::accept`], checked on bytes.
//!
//! The store is content-addressed: an item's name is the BLAKE3 of its bytes, so it can be
//! written to a directory and found again without an index. [`AudioStore::with_directory`] opens
//! such a directory, and every [`AudioStore::accept`] into it persists before it is answerable.
//!
//! **Status: serves, and survives a restart.** See `SPEC.md` for the carriers and the bounds.

#![forbid(unsafe_code)]

pub mod http;
pub mod ipfs;
pub mod protocol;
#[cfg(feature = "reticulum")]
pub mod reticulum;
pub mod server;
pub mod udp;
pub mod unix;
pub mod wav;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::ipfs::IpfsPublisher;

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
        // A declared type is metadata, so it carries the same bound the loader applies on the way
        // back in. Without it a publisher could hand the daemon a media type almost as large as a
        // frame -- megabytes -- which would be written to disk and then ignored by the loader,
        // so `--max-bytes` would bound the audio but not the bytes actually stored. Equal bounds
        // on the way out and the way in are what make the store bound a bound on disk.
        let media_type = media_type.and_then(|stated| {
            let stated = stated.trim();
            if stated.is_empty() || stated.len() > MAX_SIDECAR_BYTES {
                None
            } else {
                Some(stated.to_string())
            }
        });
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

    /// The bytes this item occupies in the store: the audio, plus the declared media type that is
    /// persisted beside it.
    ///
    /// The bound is a bound on disk, so the metadata has to count. Without this a publisher could
    /// send a 12-byte WAV carrying a 4 KiB declared type: the audio is small, so it is accepted,
    /// but `--max-bytes` then bounds almost none of what is stored. A store that reports itself
    /// within its bound while the directory grows far past it is not bounded.
    pub fn footprint(&self) -> usize {
        self.bytes.len() + self.media_type.as_ref().map_or(0, String::len)
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
    /// The bytes are audio, but they are larger than a bound the daemon enforces: either the
    /// store's `--max-bytes`, or the point at which a fetch reply would no longer fit a frame.
    /// Refused rather than written and immediately evicted: a bound that is not checked on the
    /// way in is not a bound.
    #[error(
        "item is {len} bytes (audio plus its declared media type), larger than the {max} byte bound"
    )]
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
    /// Entries read and accepted but pushed out by a bound -- the byte bound or the entry cap --
    /// while later entries loaded. A read-only check ([`AudioStore::check_directory`]) reports them
    /// the same way but leaves their files alone.
    pub evicted: usize,
    /// The directory held more entries than the load will consider. Nothing is deleted or changed
    /// by this; the operator is told instead of the load silently ignoring the rest.
    pub truncated: bool,
    /// The load stopped at its wall-clock deadline with entries left unread. Nothing is deleted or
    /// changed by this either; the operator is told rather than left waiting on a directory that is
    /// slow (or whose filesystem has stalled) without knowing why.
    pub timed_out: bool,
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
        // ISO BMFF: a box header, then the major brand, a minor version, and zero or more
        // compatible brands. Only an audio-bearing brand is accepted. `isom` and `mp42` are
        // generic brands that MP4 *video* uses, so the major brand alone cannot decide this --
        // ffmpeg-written M4A often has `isom` as the major brand and `M4A ` among the compatible
        // brands. Checking the whole list separates the two.
        if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
            let size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
            // A size of 0 means the box runs to the end of the file. A size of 1 means a 64-bit
            // largesize immediately after the type field (bytes 8..16, per ISO 14496-12), which
            // pushes the major brand, minor version, and compatible brands eight bytes along.
            let (end, major_at, first_compatible) = match size {
                0 => (bytes.len(), 8usize, 16usize),
                1 if bytes.len() >= 24 => {
                    let large = u64::from_be_bytes([
                        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                        bytes[15],
                    ]) as usize;
                    (
                        if large == 0 {
                            bytes.len()
                        } else {
                            large.min(bytes.len())
                        },
                        16,
                        24,
                    )
                }
                _ => (size.min(bytes.len()), 8, 16),
            };
            if matches!(&bytes[major_at..major_at + 4], b"M4A " | b"M4B " | b"M4P ") {
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
    /// Files eviction removed from the index but has not unlinked yet. Unlinking is a blocking
    /// syscall, so it is taken off the lock and done asynchronously; a load drains it inline.
    evicted: Vec<PathBuf>,
    /// When `Some`, every accepted item is also offered to IPFS. Best-effort: a mirror failure is
    /// recorded, never propagated, because losing a copy must not lose the audio.
    ipfs: Option<crate::ipfs::IpfsPublisher>,
    /// The most recent mirror outcomes, newest last, capped. Reported rather than swallowed.
    mirror_log: VecDeque<MirrorOutcome>,
    /// The CID Kubo assigned to each mirrored item, keyed by content hash, so a client that fetches
    /// audio can be told where else to find it. Dropped when the item is evicted.
    mirror_cids: HashMap<String, String>,
    /// The same CIDs keyed the other way, so a client that arrives with only a CID can be served
    /// from what we already hold instead of depending on the node being up.
    cid_index: HashMap<String, String>,
}

/// Accept bytes through the inbound rule and then offer them to IPFS if a mirror is configured.
///
/// The two halves are deliberately separate. `accept` takes the store lock for the rule, the
/// durable write, and the insert; the network call happens **after** that lock is released. A Kubo
/// node that is down or slow must not stall every other client's publish and fetch behind it.
///
/// This is the function carriers call. `AudioStore::accept` stays the pure inbound rule, so a
/// mirror cannot become part of what "accepted" means.
pub async fn accept_and_mirror(
    store: &Arc<tokio::sync::Mutex<AudioStore>>,
    bytes: Vec<u8>,
    declared_media_type: Option<String>,
) -> Result<String, ListendError> {
    let hash = {
        let mut store = store.lock().await;
        store.accept(bytes, declared_media_type).await?
    };
    let publisher = store.lock().await.ipfs.clone();
    if let Some(publisher) = publisher {
        // These exact bytes are already on IPFS under a CID we recorded, so do not add them again.
        // Without this, every repeated publish and every restart would rewrite the library.
        if store.lock().await.mirror_cids.contains_key(&hash) {
            return Ok(hash);
        }
        // Read the bytes under a short lock, then send them without it.
        let item = match store.lock().await.serve(&hash) {
            Ok(item) => item,
            Err(_) => {
                // The item was evicted between the accept and the mirror (a concurrent publish
                // pushed it out). Record that rather than skipping silently: every mirror outcome
                // is reported, and an operator reading the shutdown report would otherwise have no
                // way to tell this mirror never happened.
                store
                    .lock()
                    .await
                    .record_mirror_outcome(mirror_skipped(&hash));
                return Ok(hash);
            }
        };
        let outcome = match publisher.add(&item.bytes).await {
            Ok(cid) => MirrorOutcome::Mirrored {
                content_hash: hash.clone(),
                cid,
            },
            Err(e) => MirrorOutcome::Failed {
                content_hash: hash.clone(),
                reason: e.to_string(),
            },
        };
        // Off the lock: a slow disk must not hold up another client's request.
        let dir = store.lock().await.dir_path().map(Path::to_path_buf);
        if let MirrorOutcome::Mirrored { content_hash, cid } = &outcome {
            persist_cid(dir.as_deref(), content_hash, cid).await;
        }
        store.lock().await.record_mirror_outcome(outcome);
    }
    Ok(hash)
}

/// The outcome for a mirror that never ran because the item was gone by the time the mirror was
/// ready. Extracted so the policy is pinned by a test even though the interleaving that reaches it
/// (an eviction landing between the accept and the mirror) cannot be constructed deterministically.
fn mirror_skipped(content_hash: &str) -> MirrorOutcome {
    MirrorOutcome::Failed {
        content_hash: content_hash.to_string(),
        reason: "evicted before it could be mirrored".into(),
    }
}

/// What happened when an accepted item was offered to IPFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MirrorOutcome {
    /// Kubo assigned this CID to bytes we already hold.
    Mirrored { content_hash: String, cid: String },
    /// The mirror did not happen. The audio is still stored and served locally.
    Failed {
        content_hash: String,
        reason: String,
    },
}

/// How many mirror outcomes are kept. Enough to read, small enough that a long run cannot grow it.
const MIRROR_LOG_LIMIT: usize = 64;

/// A recorded CID is short text. A longer sidecar is not one and is ignored rather than read.
const MAX_CID_SIDECAR_BYTES: usize = 128;

/// The whole startup mirror pass is bounded by this, so a large store cannot hold the daemon's
/// startup open indefinitely.
pub const STARTUP_MIRROR_BUDGET: Duration = Duration::from_secs(30);

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
            evicted: Vec::new(),
            ipfs: None,
            mirror_log: VecDeque::new(),
            mirror_cids: HashMap::new(),
            cid_index: HashMap::new(),
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
        Self::load_dir_impl(dir, max_bytes, MAX_LOAD_ENTRIES, false, LOAD_DEADLINE)
    }

    /// Open a store directory and report on it **without changing it**. `--check` uses this.
    ///
    /// A verification that unlinks an operator's audio because it happens to exceed
    /// `--max-bytes` is not a verification: `with_directory` prunes on load, which is right for a
    /// daemon starting up and destructive for a command whose whole purpose is to look. In this
    /// mode entries over the bound are reported as `evicted` but their files are left alone, and a
    /// missing directory is an error rather than something to create.
    pub fn check_directory(
        dir: impl Into<PathBuf>,
        max_bytes: usize,
    ) -> std::io::Result<(Self, LoadReport)> {
        Self::load_dir_impl(dir, max_bytes, MAX_LOAD_ENTRIES, true, LOAD_DEADLINE)
    }

    /// See [`Self::with_directory`]. Only the tests use this directly, to vary the entry cap.
    #[cfg(test)]
    fn load_dir(
        dir: impl Into<PathBuf>,
        max_bytes: usize,
        max_entries: usize,
    ) -> std::io::Result<(Self, LoadReport)> {
        Self::load_dir_impl(dir, max_bytes, max_entries, false, LOAD_DEADLINE)
    }

    fn load_dir_impl(
        dir: impl Into<PathBuf>,
        max_bytes: usize,
        max_entries: usize,
        read_only: bool,
        deadline_budget: Duration,
    ) -> std::io::Result<(Self, LoadReport)> {
        let dir = dir.into();
        // A check must not create the directory it is checking: creating it would hide a typo in
        // the operator's `--dir` and leave an empty directory behind as a side effect.
        if !read_only {
            std::fs::create_dir_all(&dir)?;
        }
        let mut store = Self {
            items: HashMap::new(),
            order: Default::default(),
            bytes_held: 0,
            max_bytes,
            dir: Some(dir.clone()),
            evicted: Vec::new(),
            ipfs: None,
            mirror_log: VecDeque::new(),
            mirror_cids: HashMap::new(),
            cid_index: HashMap::new(),
        };
        let mut report = LoadReport::default();
        let mut inserted = 0usize;
        let mut considered = 0usize;
        let deadline = std::time::Instant::now() + deadline_budget;

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
            // Checked before each entry, so a slow directory stops at the deadline rather than
            // after it. A single read already in flight is not interrupted.
            if std::time::Instant::now() >= deadline {
                report.timed_out = true;
                break;
            }
            // The cap counts audio entries, so a directory of sidecars alone cannot
            // truncate the audio load.
            if considered >= max_entries {
                report.truncated = true;
                break;
            }
            considered += 1;
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
            let cid = read_cid_sidecar(&dir, &hash);
            store.publish(AudioItem {
                content_hash: hash.clone(),
                bytes,
                media_type,
            });
            if let Some(cid) = cid {
                store.mirror_cids.insert(hash.clone(), cid.clone());
                store.cid_index.insert(cid, hash);
            }
            inserted += 1;
        }

        // Startup has no lock to protect, so the evicted files are unlinked inline. A read-only
        // check reports them instead: it must leave the operator's directory exactly as it found
        // it.
        if !read_only {
            for path in std::mem::take(&mut store.evicted) {
                let _ = std::fs::remove_file(path);
            }
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
        // The inbound rule, on the bytes and before anything is stored. Unmoved.
        if AudioFormat::sniff(&bytes).is_none() {
            return Err(ListendError::NotAudio);
        }
        let item = AudioItem::new(bytes, declared_media_type);
        // Two bounds, both enforced on the way in, and they are not the same one.
        //
        // The *audio* must fit a fetch reply: the reply also carries the hash, a length prefix
        // and an optional CID, so an item at the frame cap would make a reply larger than the cap.
        // The declared media type is not in the reply, so it must not shrink this bound.
        let servable = servable_bound(self.max_bytes);
        if item.bytes.len() > servable {
            return Err(ListendError::TooLarge {
                len: item.bytes.len(),
                max: servable,
            });
        }
        // The whole *footprint* -- audio plus the media type persisted beside it -- must fit the
        // store, or `--max-bytes` bounds almost none of what is actually written to disk.
        if item.footprint() > self.max_bytes {
            return Err(ListendError::TooLarge {
                len: item.footprint(),
                max: self.max_bytes,
            });
        }
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
                // A stat is a storage operation too. On a timeout the file is treated as absent,
                // which means the bounded write below simply re-persists it.
                let present = tokio::time::timeout(
                    STORAGE_TIMEOUT,
                    tokio::fs::try_exists(dir.join(&existing.content_hash)),
                )
                .await
                .ok()
                .and_then(|answer| answer.ok())
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
        // Offer the same bytes to IPFS, best-effort, once they are durable here. A mirror failure
        // is recorded, never propagated: losing a copy must not lose the audio.
        let hash = self.publish(item);
        // Unlink what eviction pushed out, off the lock. A failed unlink is best-effort, exactly
        // as the synchronous version was; the item is gone from the index either way.
        self.drain_evicted().await;
        Ok(hash)
    }

    async fn drain_evicted(&mut self) {
        if self.evicted.is_empty() {
            return;
        }
        let paths = std::mem::take(&mut self.evicted);
        for path in paths {
            // Bounded, like the write: this runs inside `accept`, so an unbounded unlink would
            // hold the publish open on a filesystem that has stalled.
            let _ = tokio::time::timeout(STORAGE_TIMEOUT, tokio::fs::remove_file(path)).await;
        }
    }

    /// Write the item under its content hash, plus a sidecar for the declared media type. The
    /// write happens before the item is answerable, so a failure is a refusal.
    /// Write the item under its content hash, plus a sidecar for the declared media type.
    ///
    /// Async, and using `tokio::fs`: the write happens while the store mutex is held, and a blocking
    /// write on a slow disk would stall every carrier's executor thread. The write still completes
    /// before the item is answerable, so a failure is still a refusal.
    async fn persist(&self, dir: &Path, item: &AudioItem) -> Result<(), ListendError> {
        persist_bounded(dir, item, STORAGE_TIMEOUT).await
    }

    /// Offer every accepted item to IPFS as well. `None` (the default) mirrors nothing, so the
    /// daemon behaves exactly as before unless `--ipfs-api` is given.
    pub fn set_ipfs(&mut self, publisher: Option<crate::ipfs::IpfsPublisher>) {
        self.ipfs = publisher;
    }

    /// The most recent mirror outcomes, oldest first.
    pub fn mirror_log(&self) -> &VecDeque<MirrorOutcome> {
        &self.mirror_log
    }

    /// `(mirrored, failed)` counts over the retained log.
    pub fn mirror_summary(&self) -> (usize, usize) {
        let mut ok = 0;
        let mut failed = 0;
        for outcome in &self.mirror_log {
            match outcome {
                MirrorOutcome::Mirrored { .. } => ok += 1,
                MirrorOutcome::Failed { .. } => failed += 1,
            }
        }
        (ok, failed)
    }

    /// Offer bytes to IPFS and record what happened. Returns whether an add was actually attempted,
    /// so a caller counting work done does not mistake a skip for an offer.
    async fn offer_to_ipfs(&mut self, content_hash: &str, bytes: &[u8]) -> bool {
        let ipfs = self.ipfs.clone();
        let Some(ipfs) = ipfs else { return false };
        if self.mirror_cids.contains_key(content_hash) {
            return false; // already offered; a restart must not rewrite the library
        }
        let outcome = add_to_ipfs(&ipfs, content_hash, bytes).await;
        if let (Some(dir), MirrorOutcome::Mirrored { content_hash, cid }) =
            (self.dir.as_deref(), &outcome)
        {
            persist_cid(Some(dir), content_hash, cid).await;
        }
        self.record_mirror(outcome);
        true
    }

    /// The configured mirror, if any, so a carrier can also *read* by CID.
    pub fn ipfs(&self) -> Option<crate::ipfs::IpfsPublisher> {
        self.ipfs.clone()
    }

    /// The content hash we hold for a CID, if this daemon has mirrored that item.
    pub fn hash_for_cid(&self, cid: &str) -> Option<&str> {
        self.cid_index.get(cid).map(String::as_str)
    }

    /// Where else this item can be fetched, if the daemon has offered it to IPFS.
    pub fn content_id(&self, content_hash: &str) -> Option<&str> {
        self.mirror_cids.get(content_hash).map(String::as_str)
    }

    /// Offer everything already held to IPFS. A restart with `--ipfs-api` would otherwise mirror
    /// only audio published after it started, and an existing library would never reach IPFS.
    ///
    /// Bounded as a whole, because a large store must not hold startup open indefinitely: each
    /// item still has its own per-call bound, and the overall deadline stops the pass. Items not
    /// reached before the deadline are simply left for their next change.
    pub async fn mirror_existing(&mut self, budget: Duration) -> usize {
        let items: Vec<(String, Arc<AudioItem>)> = self
            .items
            .values()
            .map(|i| (i.content_hash.clone(), Arc::clone(i)))
            .collect();
        let mut offered = 0usize;
        let deadline = std::time::Instant::now() + budget;
        for (content_hash, item) in items {
            let remaining = match deadline.checked_duration_since(std::time::Instant::now()) {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => break,
            };
            // Each item is bounded by what is left, so the whole pass cannot exceed the budget.
            match tokio::time::timeout(remaining, self.offer_to_ipfs(&content_hash, &item.bytes))
                .await
            {
                Ok(true) => offered += 1,
                Ok(false) => {} // already recorded; nothing was offered
                Err(_) => break,
            }
        }
        offered
    }

    /// Record a mirror outcome produced outside [`Self::accept`] -- by
    /// [`accept_and_mirror`], which sends without holding the lock.
    pub fn record_mirror_outcome(&mut self, outcome: MirrorOutcome) {
        self.record_mirror(outcome);
    }

    fn record_mirror(&mut self, outcome: MirrorOutcome) {
        // Remember the CID where it is known, whichever path produced the outcome: a client that
        // fetches audio must be able to learn it, not only the shutdown report.
        if let MirrorOutcome::Mirrored { content_hash, cid } = &outcome {
            self.mirror_cids.insert(content_hash.clone(), cid.clone());
            // And the other way round, so a client arriving with only a CID can be served from what
            // we hold rather than depending on the node.
            self.cid_index.insert(cid.clone(), content_hash.clone());
        }
        if self.mirror_log.len() >= MIRROR_LOG_LIMIT {
            self.mirror_log.pop_front();
        }
        self.mirror_log.push_back(outcome);
    }

    /// Remove an evicted item's file, so the directory and the store agree on what is held.
    fn forget(&mut self, content_hash: &str) {
        if let Some(cid) = self.mirror_cids.remove(content_hash) {
            self.cid_index.remove(&cid);
        }
        if let Some(dir) = &self.dir {
            self.evicted.push(dir.join(content_hash));
            self.evicted.push(dir.join(format!("{content_hash}.json")));
            self.evicted.push(dir.join(format!("{content_hash}.cid")));
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
        self.bytes_held += item.footprint();
        self.items.insert(hash.clone(), Arc::new(item));
        self.order.push_back(hash.clone());
        // Both bounds, and they are independent. The byte bound does not bound the *number* of
        // items: each one costs a map entry, a queue entry and an `Arc` whether the audio is
        // 12 bytes or 16 MiB, so a publisher of tiny sounds could drive memory roughly 200x past
        // `--max-bytes` while the daemon reported itself within it. Measured: 20,000 twelve-byte
        // items cost 4 MB. The entry cap is the same one a directory load honours, so a start and a
        // lifetime of publishes agree.
        while self.bytes_held > self.max_bytes || self.items.len() > MAX_STORE_ENTRIES {
            if !self.evict_oldest() {
                break;
            }
        }
        hash
    }

    /// Drop the oldest item. Both bounds call this, so the accounting lives in one place. Returns
    /// whether there was one, so the eviction loop above cannot spin on an inconsistent store.
    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self.order.pop_front() else {
            return false;
        };
        if let Some(evicted) = self.items.remove(&oldest) {
            self.bytes_held -= evicted.footprint();
            self.forget(&oldest);
        }
        true
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

    /// The persistence directory, if `--dir` was given.
    pub fn dir_path(&self) -> Option<&Path> {
        self.dir.as_deref()
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
            evicted: Vec::new(),
            ipfs: None,
            mirror_log: VecDeque::new(),
            mirror_cids: HashMap::new(),
            cid_index: HashMap::new(),
        }
    }
}

/// Add `bytes` to the node and build the outcome. Shared by the owned-store pass and the
/// serving-path pass so the two cannot drift.
async fn add_to_ipfs(ipfs: &IpfsPublisher, content_hash: &str, bytes: &[u8]) -> MirrorOutcome {
    match ipfs.add(bytes).await {
        Ok(cid) => MirrorOutcome::Mirrored {
            content_hash: content_hash.to_string(),
            cid,
        },
        Err(e) => MirrorOutcome::Failed {
            content_hash: content_hash.to_string(),
            reason: e.to_string(),
        },
    }
}

/// Offer one already-held item to IPFS, taking the store lock only for the check and the recording
/// -- never across the network call. This is what lets the startup pass run beside live traffic.
async fn offer_shared(
    store: &Arc<Mutex<AudioStore>>,
    item: Arc<AudioItem>,
    remaining: Duration,
) -> bool {
    let (ipfs, dir, already) = {
        let store = store.lock().await;
        (
            store.ipfs.clone(),
            store.dir_path().map(Path::to_path_buf),
            store.mirror_cids.contains_key(&item.content_hash),
        )
    };
    if already {
        return false; // already offered; a restart must not rewrite the library
    }
    let Some(ipfs) = ipfs else { return false };
    let outcome = match tokio::time::timeout(
        remaining,
        add_to_ipfs(&ipfs, &item.content_hash, &item.bytes),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => MirrorOutcome::Failed {
            content_hash: item.content_hash.clone(),
            reason: "the node did not answer in time".into(),
        },
    };
    if let (Some(dir), MirrorOutcome::Mirrored { content_hash, cid }) = (dir.as_deref(), &outcome) {
        persist_cid(Some(dir), content_hash, cid).await;
    }
    store.lock().await.record_mirror(outcome);
    true
}

/// The startup mirror pass, against a store the carriers are already serving.
///
/// [`AudioStore::mirror_existing`] needs an owned store, so it can only run before anything binds:
/// measured, a node answering three seconds per add delayed the daemon from serving by 12.3 s, and
/// a stalled node would hold it up for the whole 30 s budget. That contradicts the mirror being
/// best-effort. This version takes the lock only for the snapshot and the recording, so the pass
/// runs beside live traffic and a slow node delays nothing.
pub async fn mirror_existing_shared(store: &Arc<Mutex<AudioStore>>, budget: Duration) -> usize {
    let items: Vec<Arc<AudioItem>> = {
        let store = store.lock().await;
        store
            .hashes()
            .into_iter()
            .filter_map(|hash| store.serve(&hash).ok())
            .collect()
    };
    let mut offered = 0usize;
    let deadline = std::time::Instant::now() + budget;
    for item in items {
        let remaining = match deadline.checked_duration_since(std::time::Instant::now()) {
            Some(remaining) if !remaining.is_zero() => remaining,
            _ => break,
        };
        if offer_shared(store, item, remaining).await {
            offered += 1;
        }
    }
    offered
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

/// The bound on a whole directory load.
///
/// A load is already bounded in work -- `MAX_LOAD_ENTRIES` entries, each at most the item bound --
/// but a filesystem that is slow rather than stalled would still take as long as it takes. The
/// deadline stops the load and reports it, so a big directory on a slow share cannot look like a
/// hang. It cannot interrupt a single syscall that is already blocked in the kernel; bounding that
/// needs asynchronous I/O, which the load does not use, and the limit is recorded as such.
pub const LOAD_DEADLINE: Duration = Duration::from_secs(300);

/// The bound on one filesystem operation in the accept path.
///
/// Every network read and write has a timeout, and so does the disk. `accept` awaits the durable
/// write before the item is answerable, so a filesystem that hangs -- an SMB share whose server has
/// stalled, say -- would otherwise hold a publish open indefinitely. A timeout turns that into the
/// same typed refusal as any other write failure: the audio is not answered as stored. It is far
/// above what the largest item needs (16 MiB at the measured 9.8 MB/s of the operator's share is
/// about two seconds).
pub const STORAGE_TIMEOUT: Duration = Duration::from_secs(60);

/// The most items the store will hold at once, however small they are. The byte bound does not
/// bound this, and per-entry overhead dominates for tiny audio. It is the same cap a directory load
/// honours ([`MAX_LOAD_ENTRIES`]), so a start and a lifetime of publishes agree.
pub const MAX_STORE_ENTRIES: usize = MAX_LOAD_ENTRIES;

/// The largest sidecar that will be read. A media type is a few dozen bytes; anything larger is
/// not a media type and is ignored rather than pulled into memory.
const MAX_SIDECAR_BYTES: usize = 4096;

/// The declared media type recorded beside a persisted item, if any.
/// Run one storage operation under `bound`, turning a filesystem that never answers into the same
/// typed refusal as any other write failure. Every blocking operation in the accept path goes
/// through here.
async fn with_storage_bound<T>(
    bound: Duration,
    op: impl std::future::Future<Output = Result<T, ListendError>>,
) -> Result<T, ListendError> {
    match tokio::time::timeout(bound, op).await {
        Ok(inner) => inner,
        Err(_) => Err(ListendError::Persist(
            "the filesystem did not answer in time".into(),
        )),
    }
}

/// Write an item durably under `bound`. Split from [`AudioStore::persist`] so a test can prove the
/// bound is applied without needing a filesystem that actually hangs.
async fn persist_bounded(
    dir: &Path,
    item: &AudioItem,
    bound: Duration,
) -> Result<(), ListendError> {
    with_storage_bound(bound, async {
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
    })
    .await
}

/// Write the `<hash>.cid` association beside the audio, so a restart skips re-adding it.
///
/// Called with no lock held and asynchronously, exactly like the audio write: a blocking write on the
/// accept path would stall every other client behind one disk operation.
async fn persist_cid(dir: Option<&Path>, content_hash: &str, cid: &str) {
    if let Some(dir) = dir {
        // Best-effort: a lost sidecar costs one redundant add, never audio. Bounded like every
        // other storage operation, so a hung filesystem cannot hold the mirror open.
        let _ = tokio::time::timeout(
            STORAGE_TIMEOUT,
            tokio::fs::write(dir.join(format!("{content_hash}.cid")), cid.as_bytes()),
        )
        .await;
    }
}

/// Read a sidecar only if it is a regular file no larger than `max` bytes.
///
/// Stat before reading, for the same reasons the audio path does: a hand-placed FIFO would make
/// `std::fs::read` block forever and hang startup, and a huge file would be pulled into memory
/// only to be rejected. Neither is a sidecar, so neither is read -- the bound is enforced on the
/// way in, not after the read.
fn read_bounded_regular(path: &Path, max: usize) -> Option<Vec<u8>> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() as usize > max {
        return None;
    }
    std::fs::read(path).ok()
}

/// The CID this store recorded for a content hash, if the sidecar is present and plausible. Written
/// when an item is mirrored, so a restart can skip re-adding what is already on IPFS; the association
/// is trustworthy because the loader verified the file's name is the BLAKE3 of its bytes.
fn read_cid_sidecar(dir: &Path, hash: &str) -> Option<String> {
    let bytes = read_bounded_regular(&dir.join(format!("{hash}.cid")), MAX_CID_SIDECAR_BYTES)?;
    if bytes.is_empty() {
        return None;
    }
    let cid = String::from_utf8(bytes).ok()?.trim().to_string();
    crate::ipfs::validate_cid(&cid).ok()?;
    Some(cid)
}

fn read_sidecar(dir: &Path, hash: &str) -> Option<String> {
    let raw = read_bounded_regular(&dir.join(format!("{hash}.json")), MAX_SIDECAR_BYTES)?;
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
        // The box body is major brand, minor version, then compatible brands.
        const MINOR: [u8; 4] = [0, 0, 0, 1];

        // A normal-sized box: size counts the whole box, here 8 header + 4 + 4 + 4.
        let mut normal = Vec::new();
        normal.extend_from_slice(&28u32.to_be_bytes());
        normal.extend_from_slice(b"ftypisom");
        normal.extend_from_slice(&MINOR);
        normal.extend_from_slice(b"M4A ");
        assert_eq!(AudioFormat::sniff(&normal), Some(AudioFormat::M4a));

        // Size 0 means "the box runs to the end of the file", which must not hide the brands.
        let mut to_end = Vec::new();
        to_end.extend_from_slice(&[0, 0, 0, 0]);
        to_end.extend_from_slice(b"ftypisom");
        to_end.extend_from_slice(&MINOR);
        to_end.extend_from_slice(b"M4A ");
        assert_eq!(AudioFormat::sniff(&to_end), Some(AudioFormat::M4a));

        // Size 1 means a 64-bit largesize **immediately after the type field** (bytes 8..16,
        // ISO 14496-12), so the major brand sits at 16 and the compatible brands at 24.
        let mut large = Vec::new();
        large.extend_from_slice(&[0, 0, 0, 1]);
        large.extend_from_slice(b"ftyp");
        large.extend_from_slice(&28u64.to_be_bytes());
        large.extend_from_slice(b"isom");
        large.extend_from_slice(&MINOR);
        large.extend_from_slice(b"M4A ");
        assert_eq!(
            AudioFormat::sniff(&large),
            Some(AudioFormat::M4a),
            "an audio brand after a 64-bit largesize must still be read"
        );

        // The same shape with the audio brand as the major brand.
        let mut large_major = Vec::new();
        large_major.extend_from_slice(&[0, 0, 0, 1]);
        large_major.extend_from_slice(b"ftyp");
        large_major.extend_from_slice(&24u64.to_be_bytes());
        large_major.extend_from_slice(b"M4A ");
        large_major.extend_from_slice(&MINOR);
        assert_eq!(AudioFormat::sniff(&large_major), Some(AudioFormat::M4a));

        // Video in the same shapes stays refused.
        let mut large_video = Vec::new();
        large_video.extend_from_slice(&[0, 0, 0, 1]);
        large_video.extend_from_slice(b"ftyp");
        large_video.extend_from_slice(&28u64.to_be_bytes());
        large_video.extend_from_slice(b"avc1");
        large_video.extend_from_slice(&MINOR);
        large_video.extend_from_slice(b"mp42");
        assert_eq!(AudioFormat::sniff(&large_video), None);
    }

    /// Each container and codec the inbound rule recognises, asserted from its signature alone.
    /// The directive names WAV, AIFF, FLAC, Ogg, MP3, and M4A. If one of them stops being
    /// recognised, valid audio is refused; the near-miss test below covers the other direction.
    #[test]
    fn every_named_signature_is_recognised_from_its_bytes() {
        // RIFF .... WAVE
        assert_eq!(
            AudioFormat::sniff(b"RIFF\x24\x00\x00\x00WAVE"),
            Some(AudioFormat::Wav)
        );
        // FORM .... AIFF, and the compressed form AIFC
        assert_eq!(
            AudioFormat::sniff(b"FORM\x00\x00\x00\x00AIFF"),
            Some(AudioFormat::Aiff)
        );
        assert_eq!(
            AudioFormat::sniff(b"FORM\x00\x00\x00\x00AIFC"),
            Some(AudioFormat::Aiff)
        );
        // FLAC stream marker
        assert_eq!(
            AudioFormat::sniff(b"fLaC\x00\x00\x00\x22"),
            Some(AudioFormat::Flac)
        );
        // An ID3v2 tag before MPEG audio
        assert_eq!(
            AudioFormat::sniff(b"ID3\x04\x00\x00\x00\x00\x00\x00"),
            Some(AudioFormat::Mp3)
        );
        // A raw MPEG-1 Layer III frame header: eleven sync bits, version 3, layer 1, bitrate 9.
        assert_eq!(
            AudioFormat::sniff(&[0xFF, 0xFB, 0x90, 0x00]),
            Some(AudioFormat::Mp3)
        );
        // Audio codecs inside an Ogg container, read from the beginning-of-stream page.
        assert_eq!(
            AudioFormat::sniff(&ogg_bos(b"\x01vorbis")),
            Some(AudioFormat::Ogg)
        );
        assert_eq!(
            AudioFormat::sniff(&ogg_bos(b"OpusHead")),
            Some(AudioFormat::Ogg)
        );
        assert_eq!(
            AudioFormat::sniff(&ogg_bos(b"\x7fFLAC")),
            Some(AudioFormat::Ogg)
        );
    }

    /// Near-misses must stay rejected. The rule is "audio only", so a plausible-looking but wrong
    /// signature is still refused -- otherwise the sniff would become a suggestion.
    #[test]
    fn near_miss_signatures_are_not_audio() {
        // RIFF carrying a video form type
        assert_eq!(AudioFormat::sniff(b"RIFF\x00\x00\x00\x00AVI "), None);
        // FORM carrying a non-AIFF form type
        assert_eq!(AudioFormat::sniff(b"FORM\x00\x00\x00\x008SVX"), None);
        // An MPEG frame header with the reserved version field set
        assert_eq!(AudioFormat::sniff(&[0xFF, 0xE8, 0x90, 0x00]), None);
        // An MPEG frame header with a zero bitrate field (none defined)
        assert_eq!(AudioFormat::sniff(&[0xFF, 0xFB, 0x00, 0x00]), None);
        // Theora video inside an Ogg container: same marker, not audio
        assert_eq!(AudioFormat::sniff(&ogg_bos(b"\x80theora")), None);
        // Plain text, and nothing at all
        assert_eq!(AudioFormat::sniff(b"plain text, not audio"), None);
        assert_eq!(AudioFormat::sniff(b""), None);
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
    fn a_declared_media_type_is_bounded_and_trimmed_like_the_loader_reads_it() {
        // The write bound is the read bound: MAX_SIDECAR_BYTES. A type the loader would ignore
        // must not be written in the first place, or the disk bound counts audio but not the
        // bytes actually stored.
        let item = AudioItem::new(b"x".to_vec(), Some("  audio/wav  ".into()));
        assert_eq!(item.media_type.as_deref(), Some("audio/wav"));

        let item = AudioItem::new(b"x".to_vec(), Some("   ".into()));
        assert_eq!(item.media_type, None, "a blank type is not a type");

        let oversized = "x".repeat(MAX_SIDECAR_BYTES + 1);
        let item = AudioItem::new(b"x".to_vec(), Some(oversized));
        assert_eq!(item.media_type, None, "an oversized type is not kept");

        assert_eq!(
            AudioItem::new(b"x".to_vec(), None).media_type,
            None,
            "no declared type stays none"
        );
    }

    #[tokio::test]
    async fn a_publish_with_an_absurd_media_type_keeps_the_audio_and_writes_no_sidecar() {
        let dir = temp_dir("media-type-bound");
        let (mut store, _report) = AudioStore::with_directory(&dir, 1024 * 1024).expect("store");
        let tone = crate::wav::generate(7, crate::wav::Sound::Tone);
        // Almost a frame's worth of declared type: it fits the frame, so only the media-type
        // bound can stop it from being written and then ignored on every later load.
        let hash = store
            .accept(tone.clone(), Some("x".repeat(16 * 1024 * 1024)))
            .await
            .expect("the audio is accepted");
        let served = store.serve(&hash).expect("served");
        assert_eq!(
            served.bytes, tone,
            "the audio is never lost to its metadata"
        );
        assert_eq!(served.media_type, None);
        assert!(
            !dir.join(format!("{hash}.json")).exists(),
            "no sidecar the loader would ignore is written"
        );
    }

    #[tokio::test]
    async fn a_normal_declared_media_type_survives_a_reopen() {
        let dir = temp_dir("media-type-round-trip");
        let (mut store, _report) = AudioStore::with_directory(&dir, 1024 * 1024).expect("store");
        let tone = crate::wav::generate(8, crate::wav::Sound::Tone);
        let hash = store
            .accept(tone, Some("audio/wav".into()))
            .await
            .expect("accepted");
        drop(store);

        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).expect("reopened");
        assert_eq!(report.loaded, 1);
        assert_eq!(
            reopened.serve(&hash).expect("served").media_type.as_deref(),
            Some("audio/wav"),
            "the bound must not cost a legitimate media type"
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

    /// The policy for a mirror that never ran because the item was evicted first. The interleaving
    /// that reaches the branch cannot be built deterministically, so the policy itself is pinned.
    #[test]
    fn a_skipped_mirror_is_a_recorded_failure() {
        let outcome = mirror_skipped("ab".repeat(32).as_str());
        assert_eq!(
            outcome,
            MirrorOutcome::Failed {
                content_hash: "ab".repeat(32),
                reason: "evicted before it could be mirrored".into(),
            }
        );
    }

    #[tokio::test]
    async fn sidecars_do_not_count_toward_the_load_cap() {
        let dir = temp_dir("sidecarcap");
        let (mut seed, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        for seed_n in 0..3u64 {
            let wav = wav::generate_n(seed_n, wav::Sound::Tone, 4);
            seed.accept(wav, None).await.unwrap();
        }
        // Three audio entries plus their sidecars, cap three: all three load.
        let (store, report) = AudioStore::load_dir(&dir, 1024 * 1024, 3).unwrap();
        assert_eq!(store.len(), 3, "sidecars must not count toward the cap");
        assert!(!report.truncated);
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

    /// `truncated` must be reported rather than silently ignored when the directory holds more
    /// audio entries than the load considers. The cap counts audio entries, so sidecars don't count.
    #[test]
    fn a_directory_over_the_entry_cap_is_reported_as_truncated() {
        let dir = temp_dir("truncated");
        for i in 0..(MAX_LOAD_ENTRIES + 1) {
            std::fs::write(dir.join(format!("{i:064x}")), b"RIFF0000WAVE").unwrap();
        }
        let (_store, report) =
            AudioStore::load_dir(&dir, 1024 * 1024 * 1024, MAX_LOAD_ENTRIES).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(report.truncated, "the cap must be reported, not ignored");
        assert_eq!(
            report.skipped, MAX_LOAD_ENTRIES,
            "the report counts the entries it considered"
        );
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

    /// A FIFO named like a sidecar must be ignored, not read. Reading one blocks forever, so a
    /// store directory containing one would hang startup instead of refusing it. This is the same
    /// class the audio path already guards against; the sidecar readers did not, and the CID
    /// reader also read before checking its size.
    #[cfg(unix)]
    #[test]
    fn a_fifo_sidecar_does_not_hang_the_load() {
        let dir = temp_dir("fifo-sidecar");
        // A valid audio entry, so the load reaches the sidecar readers.
        let item = AudioItem::new(wav::generate(9, wav::Sound::Tone), None);
        std::fs::write(dir.join(&item.content_hash), &item.bytes).unwrap();
        for suffix in ["cid", "json"] {
            let made = std::process::Command::new("mkfifo")
                .arg(dir.join(format!("{}.{suffix}", item.content_hash)))
                .status()
                .and_then(|status| {
                    status
                        .success()
                        .then_some(())
                        .ok_or(std::io::Error::other("mkfifo failed"))
                })
                .is_ok();
            assert!(made, "mkfifo must be available to exercise this");
        }

        // The load is blocking, so it runs on a thread: a reader that blocks would otherwise hang
        // the test rather than fail it.
        let loaded = dir.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(AudioStore::with_directory(&loaded, 1024 * 1024));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(Ok((store, report))) => {
                assert_eq!(report.loaded, 1, "the audio itself still loads");
                assert_eq!(
                    store.content_id(&item.content_hash),
                    None,
                    "a FIFO is not a CID"
                );
                assert_eq!(
                    store.serve(&item.content_hash).unwrap().media_type,
                    None,
                    "a FIFO is not a media type"
                );
            }
            Ok(Err(e)) => panic!("the load failed: {e}"),
            Err(_) => panic!("a FIFO sidecar hung the load: it was read, not stat'd"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_oversized_cid_sidecar_is_ignored() {
        let dir = temp_dir("cid-sidecar");
        let (mut store, _) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        let wav = wav::generate(9, wav::Sound::Tone);
        let hash = store.accept(wav, None).await.unwrap();
        // Larger than the CID bound, so it must not be read into memory at all.
        std::fs::write(dir.join(format!("{hash}.cid")), vec![b'c'; 4096]).unwrap();
        let (reopened, report) = AudioStore::with_directory(&dir, 1024 * 1024).unwrap();
        assert_eq!(report.loaded, 1);
        assert_eq!(
            reopened.content_id(&hash),
            None,
            "an oversized CID sidecar is ignored"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn write_named(dir: &Path, bytes: &[u8]) -> String {
        let hash = blake3::hash(bytes).to_hex().to_string();
        std::fs::write(dir.join(&hash), bytes).unwrap();
        hash
    }

    /// A check is a look, not a pruning. `with_directory` unlinks what the bound pushes out (right
    /// for a daemon starting up); `--check` must leave the operator's directory exactly as it found
    /// it, and report what it would have pushed out instead.
    #[test]
    fn a_check_does_not_delete_what_the_bound_would_evict() {
        let dir = temp_dir("check-readonly");
        let wavs: Vec<Vec<u8>> = (0..3)
            .map(|seed| wav::generate_n(seed, wav::Sound::Tone, 4))
            .collect();
        let hashes: Vec<String> = wavs.iter().map(|w| write_named(&dir, w)).collect();
        // Three 52-byte items, bound 100: each new one pushes the oldest out.
        let bound = 100;

        let (checked, report) = AudioStore::check_directory(&dir, bound).unwrap();
        assert_eq!(checked.len(), 1);
        assert_eq!(report.loaded, 1);
        assert_eq!(
            report.evicted, 2,
            "what the bound would push out is reported"
        );
        for hash in &hashes {
            assert!(dir.join(hash).exists(), "a check must not unlink {hash}");
        }

        // The same directory opened for a real start does prune, which is the behaviour the
        // check must not have.
        let (opened, _) = AudioStore::with_directory(&dir, bound).unwrap();
        assert_eq!(opened.len(), 1);
        let survivors = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_none())
            .count();
        assert_eq!(survivors, 1, "a start prunes, a check does not");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A check must not create the directory it is checking: creating it would hide a typo in the
    /// operator's `--dir` and leave an empty directory behind as a side effect.
    #[test]
    fn a_check_does_not_create_a_missing_directory() {
        let dir = std::env::temp_dir().join(format!(
            "oag-listend-missing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let result = AudioStore::check_directory(&dir, 1024);
        assert!(
            result.is_err(),
            "checking a directory that does not exist must say so, not report an empty store"
        );
        assert!(!dir.exists(), "a check must not create the directory");
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

    /// The bound is a bound on disk, and the declared media type is persisted beside the audio, so
    /// it has to count. A publisher could otherwise send a 12-byte WAV with a 4 KiB type: the audio
    /// is small, so it is accepted, and the store would hold far more than `--max-bytes` while
    /// reporting itself within it.
    #[tokio::test]
    async fn the_store_bound_counts_the_declared_media_type() {
        let mut store = AudioStore::with_capacity(20);
        // The smallest bytes the inbound rule recognises as WAV.
        let header = b"RIFF\x00\x00\x00\x00WAVE".to_vec();
        assert_eq!(header.len(), 12);
        match store.accept(header.clone(), Some("x".repeat(32))).await {
            Err(ListendError::TooLarge { len, max }) => {
                assert_eq!(len, 12 + 32, "the footprint is what is measured");
                assert_eq!(max, 20);
            }
            other => panic!("the metadata must count toward the bound, got {other:?}"),
        }
        assert!(store.is_empty(), "a refused item must not be stored");
        assert_eq!(store.bytes_held(), 0);

        // The same audio with a type that fits is accepted, and the footprint is what is held.
        let mut store = AudioStore::with_capacity(44);
        let hash = store
            .accept(header, Some("x".repeat(32)))
            .await
            .expect("12 + 32 is exactly the bound");
        assert_eq!(store.bytes_held(), 44);
        assert!(store.serve(&hash).is_ok());
    }

    /// A sidecar already on disk counts too, so loading a store cannot walk the directory past the
    /// bound while `bytes_held` says otherwise.
    #[tokio::test]
    async fn a_loaded_sidecar_counts_toward_the_bound() {
        let dir = temp_dir("sidecar-footprint");
        let wav = wav::generate_n(3, wav::Sound::Tone, 4); // 52 bytes
        let hash = blake3::hash(&wav).to_hex().to_string();
        std::fs::write(dir.join(&hash), &wav).unwrap();
        // A maximal media type: the largest the loader will read.
        std::fs::write(
            dir.join(format!("{hash}.json")),
            "y".repeat(MAX_SIDECAR_BYTES),
        )
        .unwrap();

        let (store, report) = AudioStore::with_directory(&dir, 100).unwrap();
        assert_eq!(report.loaded, 0, "the footprint is over the bound");
        assert_eq!(report.evicted, 1, "and it is reported, not silently held");
        assert_eq!(store.len(), 0);
        assert_eq!(store.bytes_held(), 0);
        assert!(
            !dir.join(&hash).exists(),
            "eviction must remove the file from disk"
        );
        let _ = std::fs::remove_dir_all(&dir);
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

    /// The real durable write, through `persist_bounded`, against a filesystem operation that never
    /// completes. `persist` is what `accept` awaits before the item is answerable, so a bound that
    /// is not applied there would leave a publish hung on a stalled filesystem.
    ///
    /// A FIFO with no reader blocks in `open()` indefinitely -- a deterministic stand-in for the
    /// stalled share, and one that does not depend on timer granularity (a real write finishes in
    /// about a millisecond, which a wall-clock bound cannot reliably lose to).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_durable_write_that_never_completes_is_refused() {
        let dir = temp_dir("hung-disk");
        let item = AudioItem::new(wav::generate(2, wav::Sound::Tone), None);
        let made = std::process::Command::new("mkfifo")
            .arg(dir.join(&item.content_hash))
            .status()
            .and_then(|s| {
                s.success()
                    .then_some(())
                    .ok_or(std::io::Error::other("mkfifo failed"))
            })
            .is_ok();
        assert!(made, "mkfifo must be available to exercise this");

        let result = persist_bounded(&dir, &item, Duration::from_millis(500)).await;
        assert!(
            matches!(result, Err(ListendError::Persist(_))),
            "a write that never completes is a refusal, got {result:?}"
        );

        // Cancelling the timed-out future does not cancel the blocking `open` behind it, so the
        // writer would stay blocked and the runtime would wait for it on the way out. Opening the
        // FIFO read-write does not block and lets that open finish; it is held for a moment so the
        // kernel sees a reader when it evaluates the blocked open.
        let unblocked = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(&item.content_hash));
        std::thread::sleep(Duration::from_millis(200));
        drop(unblocked);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SPEC: "Only regular files are read -- a FIFO or device named like a hash would block
    /// forever". A directory is already covered elsewhere; this pins the case that would actually
    /// hang, on a thread with a bound, so dropping the regularity check fails the test instead of
    /// stalling the suite.
    #[cfg(unix)]
    #[test]
    fn a_fifo_named_like_a_hash_is_skipped_not_read() {
        let dir = temp_dir("fifo-entry");
        let made = std::process::Command::new("mkfifo")
            .arg(dir.join("0".repeat(64)))
            .status()
            .and_then(|s| {
                s.success()
                    .then_some(())
                    .ok_or(std::io::Error::other("mkfifo failed"))
            })
            .is_ok();
        assert!(made, "mkfifo must be available to exercise this");

        let loaded = dir.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(AudioStore::load_dir_impl(
                &loaded,
                1024 * 1024,
                MAX_LOAD_ENTRIES,
                false,
                LOAD_DEADLINE,
            ));
        });
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok((store, report))) => {
                assert_eq!(report.loaded, 0);
                assert_eq!(report.skipped, 1, "the FIFO is skipped, not read");
                assert!(store.is_empty());
            }
            Ok(Err(e)) => panic!("the load failed: {e}"),
            Err(_) => panic!("a FIFO named like a hash hung the load: it was read, not stat'd"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A load that cannot finish inside its deadline stops and says so, rather than looking like a
    /// hang. The deadline is checked between entries; it cannot interrupt a single read that is
    /// already blocked in the kernel, and the docs say so.
    #[test]
    fn a_load_that_cannot_meet_its_deadline_reports_it() {
        let dir = temp_dir("deadline");
        for i in 0..4u64 {
            let wav = wav::generate_n(i, wav::Sound::Tone, 4);
            let hash = blake3::hash(&wav).to_hex().to_string();
            std::fs::write(dir.join(hash), wav).unwrap();
        }
        // A budget that has already elapsed: the first entry stops the load.
        let (_store, report) = AudioStore::load_dir_impl(
            &dir,
            1024 * 1024,
            MAX_LOAD_ENTRIES,
            false,
            Duration::from_nanos(1),
        )
        .unwrap();
        assert!(
            report.timed_out,
            "the deadline must be reported, not swallowed"
        );
        assert_eq!(report.loaded, 0, "nothing was read before the deadline");
        assert_eq!(report.skipped, 0);

        // And a real budget loads the same directory whole.
        let (_store, report) =
            AudioStore::load_dir_impl(&dir, 1024 * 1024, MAX_LOAD_ENTRIES, false, LOAD_DEADLINE)
                .unwrap();
        assert!(
            !report.timed_out,
            "a small directory loads well inside the deadline"
        );
        assert_eq!(report.loaded, 4);
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

    /// The byte bound does not bound the number of items, and per-entry overhead dominates for tiny
    /// audio. Measured against the real binary: 20,000 twelve-byte items cost 4 MB of RSS while
    /// `bytes_held` was 240 KB against a 1 GiB bound. So the store is capped in entries too, by the
    /// same cap a directory load honours.
    #[test]
    fn the_store_is_bounded_in_entries_too() {
        // A byte bound that cannot engage, so only the entry cap can be what stops it.
        let mut store = AudioStore::with_capacity(usize::MAX);
        let mut first = None;
        for i in 0..(MAX_STORE_ENTRIES + 1) {
            // Eight distinct leading bytes: `vec![i as u8; 12]` would repeat every 256 and a
            // republish is a no-op, which is not what this test is about.
            let mut bytes = vec![0u8; 12];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            let hash = store.publish(AudioItem::new(bytes, None));
            if first.is_none() {
                first = Some(hash);
            }
        }
        assert_eq!(
            store.len(),
            MAX_STORE_ENTRIES,
            "the entry cap holds however small the items are"
        );
        assert!(
            store.serve(&first.expect("the first hash")).is_err(),
            "the oldest item is evicted, not kept alongside the cap"
        );
        assert!(
            store.bytes_held() < store.max_bytes(),
            "the byte bound never engaged; the entry cap did"
        );
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
