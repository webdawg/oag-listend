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

/// Generate one valid WAV of `kind` from `seed`.
pub fn generate(seed: u64, kind: Sound) -> Vec<u8> {
    let seconds = duration_for(seed);
    let n = ((SAMPLE_RATE as f32) * seconds) as usize;
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
    fn the_startup_set_is_bounded_and_varied() {
        let set = sounds(99, 6);
        assert_eq!(set.len(), 6);
        assert!(set.iter().all(|b| AudioFormat::sniff(b) == Some(AudioFormat::Wav)));
        let unique: std::collections::HashSet<&Vec<u8>> = set.iter().collect();
        assert!(unique.len() > 1, "the startup set should not be six copies of one sound");
    }
}
