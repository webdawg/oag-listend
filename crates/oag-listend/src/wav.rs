//! Seeded WAV generation: real, valid audio from a seed, so a test or a fresh daemon never has
//! to ship a fixture and every run is reproducible.
//!
//! Three kinds, all 16-bit PCM mono WAV, all small: silence, a tone, and noise. Silence is the
//! honest zero; a tone is the one thing a listener can check by ear; noise is the stress case.
//! Nothing here decodes or plays -- it only writes bytes the inbound rule recognises as WAV.

#![forbid(unsafe_code)]

/// PCM sample rate. 8 kHz is telephone bandwidth: enough to be recognisably audio, small enough
/// that a generated fixture stays in the kilobytes.
pub const SAMPLE_RATE: u32 = 8000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sound {
    Silence,
    Tone,
    Noise,
}

/// SplitMix64. Deterministic, no dependency, good enough to make a tone and some noise.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A deterministic frequency for a seed, away from the Nyquist edge.
fn frequency_for(seed: u64) -> f32 {
    180.0 + (seed % 620) as f32
}

/// A deterministic duration in seconds, short on purpose.
fn duration_for(seed: u64) -> f32 {
    0.05 + (seed % 5) as f32 * 0.01
}

fn sample_count_for(seed: u64) -> usize {
    ((SAMPLE_RATE as f32) * duration_for(seed)) as usize
}

/// Write a RIFF/WAVE container around 16-bit PCM samples.
fn write_wav(samples: &[i16], sample_rate: u32) -> Vec<u8> {
    let data_len = samples.len() * 2;
    let mut out = Vec::with_capacity(44 + data_len);

    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&((36 + data_len) as u32).to_le_bytes());
    out.extend_from_slice(b"WAVE");

    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes()); // PCM chunk size
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM format
    out.extend_from_slice(&1u16.to_le_bytes()); // mono
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
    out.extend_from_slice(&2u16.to_le_bytes()); // block align
    out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample

    out.extend_from_slice(b"data");
    out.extend_from_slice(&(data_len as u32).to_le_bytes());
    for s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Generate one valid WAV of `kind` from `seed`, at the seed's default short duration.
pub fn generate(seed: u64, kind: Sound) -> Vec<u8> {
    generate_n(seed, kind, sample_count_for(seed))
}

/// The largest sound `generate_n` will produce. Five seconds of 8 kHz 16-bit mono is far more than
/// any fixture needs, and a caller passing an unbounded count must not be able to allocate without
/// limit. It is deliberately above one UDP datagram's payload, so a test can still build a fixture
/// that crosses that bound.
pub const MAX_GENERATED_SAMPLES: usize = 40_000;

/// Generate one valid WAV of `kind` with `samples` PCM frames, capped at
/// [`MAX_GENERATED_SAMPLES`].
///
/// Exposed so a caller can make a sound of a chosen size -- a carrier with a datagram or frame
/// bound needs a fixture that crosses it, and a test should not have to fake a container.
pub fn generate_n(seed: u64, kind: Sound, samples: usize) -> Vec<u8> {
    let n = samples.min(MAX_GENERATED_SAMPLES);
    let mut state = seed ^ 0xA5A5_A5A5_A5A5_A5A5;

    let samples: Vec<i16> = match kind {
        Sound::Silence => vec![0; n],
        Sound::Tone => {
            let freq = frequency_for(seed);
            let amplitude = (i16::MAX as f32) * 0.5;
            (0..n)
                .map(|i| {
                    let t = i as f32 / SAMPLE_RATE as f32;
                    (amplitude * (2.0 * std::f32::consts::PI * freq * t).sin()) as i16
                })
                .collect()
        }
        Sound::Noise => {
            let amplitude = (i16::MAX as f32) * 0.4;
            let mut smoothed = 0.0f32;
            (0..n)
                .map(|_| {
                    let raw = (splitmix64(&mut state) as f32 / u64::MAX as f32) - 0.5;
                    smoothed = 0.6 * smoothed + 0.4 * raw;
                    (amplitude * smoothed) as i16
                })
                .collect()
        }
    };

    write_wav(&samples, SAMPLE_RATE)
}

/// A small, deterministic set of generated sounds, cycling kinds and varying seeds.
///
/// This is what a daemon publishes at startup so that a client which only fetches still receives
/// audio. It is deliberately a handful, not a library.
pub fn sounds(seed: u64, count: usize) -> Vec<Vec<u8>> {
    let kinds = [Sound::Silence, Sound::Tone, Sound::Noise];
    (0..count)
        .map(|i| {
            let item_seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(i as u64 + 1);
            let kind = kinds[i % kinds.len()];
            generate(item_seed, kind)
        })
        .collect()
}

/// A deterministic rendition of the opening of "Twinkle, Twinkle, Little Star".
///
/// This is the canonical ping sound (operator directive, `USER_INPUT_RECORD.md` Entry 3): a
/// liveness answer must be audio, not an empty control pong. The melody is fixed -- C C G G A A G |
/// F F E E D D C -- with a short ramp on each note so the concatenation does not click. No RNG, so
/// every daemon answers a ping with the same bytes and therefore the same content hash.
pub fn twinkle() -> Vec<u8> {
    // (frequency, seconds). The opening phrase -- "twinkle, twinkle, little star" -- as the
    // directive specifies. Quarter notes at 0.35 s and a 0.7 s phrase end. Kept short
    // deliberately: the whole point is that the ping fits the smallest carrier's datagram.
    const MELODY: [(f32, f32); 7] = [
        (261.63, 0.35), // C4
        (261.63, 0.35), // C4
        (392.00, 0.35), // G4
        (392.00, 0.35), // G4
        (440.00, 0.35), // A4
        (440.00, 0.35), // A4
        (392.00, 0.70), // G4
    ];

    let mut samples: Vec<i16> = Vec::new();
    for (freq, seconds) in MELODY {
        let n = ((SAMPLE_RATE as f32) * seconds) as usize;
        let ramp = (((SAMPLE_RATE as f32) * 0.01) as usize).min(n / 2).max(1);
        for i in 0..n {
            let t = i as f32 / SAMPLE_RATE as f32;
            let fade_in = (i as f32 / ramp as f32).min(1.0);
            let fade_out = ((n - i) as f32 / ramp as f32).min(1.0);
            let envelope = fade_in * fade_out;
            let sample = (2.0 * std::f32::consts::PI * freq * t).sin() * 0.5 * envelope;
            samples.push((sample * i16::MAX as f32) as i16);
        }
    }
    write_wav(&samples, SAMPLE_RATE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AudioFormat;

    #[test]
    fn generated_audio_is_recognised_as_wav() {
        for kind in [Sound::Silence, Sound::Tone, Sound::Noise] {
            let bytes = generate(42, kind);
            assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::Wav), "{kind:?} must be a WAV");
        }
    }

    #[test]
    fn generation_is_reproducible_from_the_seed() {
        for kind in [Sound::Silence, Sound::Tone, Sound::Noise] {
            assert_eq!(generate(7, kind), generate(7, kind), "{kind:?} must be deterministic");
            assert_ne!(generate(7, kind), generate(8, kind), "{kind:?} must vary with the seed");
        }
    }

    #[test]
    fn a_tone_is_a_tone_and_silence_is_silence() {
        let silence = generate(1, Sound::Silence);
        let tone = generate(1, Sound::Tone);
        let noise = generate(1, Sound::Noise);
        // All three are the same length and container; only the payload differs.
        assert_eq!(silence.len(), tone.len());
        assert_eq!(silence.len(), noise.len());
        assert_ne!(silence[44..], tone[44..]);
        assert_ne!(tone[44..], noise[44..]);
    }

    #[test]
    fn the_container_header_is_well_formed() {
        let bytes = generate(3, Sound::Tone);
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        assert_eq!(&bytes[12..16], b"fmt ");
        let channels = u16::from_le_bytes([bytes[22], bytes[23]]);
        let bits = u16::from_le_bytes([bytes[34], bytes[35]]);
        assert_eq!(channels, 1);
        assert_eq!(bits, 16);
        assert_eq!(&bytes[36..40], b"data");
    }

    #[test]
    fn the_canonical_ping_is_a_deterministic_wav() {
        let a = twinkle();
        let b = twinkle();
        assert_eq!(a, b, "the ping must be the same bytes on every daemon");
        assert_eq!(AudioFormat::sniff(&a), Some(AudioFormat::Wav));
        assert!(!a.is_empty(), "a ping must never be an empty pong");
        let data_len = u32::from_le_bytes([a[40], a[41], a[42], a[43]]) as usize;
        // Six quarter notes at 0.35 s and one at 0.7 s, at 8 kHz, 16-bit mono.
        let expected_samples = (6.0 * 0.35 * SAMPLE_RATE as f32) as usize
            + (0.70 * SAMPLE_RATE as f32) as usize;
        assert_eq!(data_len, expected_samples * 2);
        assert!(data_len < crate::protocol::MAX_SERVABLE_BYTES, "the ping must fit a frame");
        // It must also fit the smallest carrier's datagram, or UDP cannot carry the music.
        assert!(
            a.len() + 75 <= crate::udp::MAX_DATAGRAM_BYTES,
            "the ping must fit one UDP datagram ({} bytes)",
            a.len()
        );
    }

    #[test]
    fn a_chosen_sample_count_is_capped() {
        let bytes = generate_n(1, Sound::Silence, usize::MAX);
        let data_len = u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]) as usize;
        assert_eq!(data_len, MAX_GENERATED_SAMPLES * 2);
        assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::Wav));
    }

    #[test]
    fn a_chosen_sample_count_is_honoured() {
        let bytes = generate_n(4, Sound::Tone, 10_000);
        assert_eq!(AudioFormat::sniff(&bytes), Some(AudioFormat::Wav));
        let data_len = u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]) as usize;
        assert_eq!(data_len, 10_000 * 2);
    }

    #[test]
    fn the_startup_set_is_bounded_and_varied() {
        let set = sounds(99, 6);
        assert_eq!(set.len(), 6);
        assert!(set.iter().all(|b| AudioFormat::sniff(b) == Some(AudioFormat::Wav)));
        let unique: std::collections::HashSet<&Vec<u8>> = set.iter().collect();
        assert!(unique.len() > 1, "the startup set should not be six copies of one sound");
    }
}
