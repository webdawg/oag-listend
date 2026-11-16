//! The IPFS mirror at the surface: accepted audio is offered to IPFS, and a mirror failure is
//! recorded rather than allowed to lose the audio. The stub here mimics Kubo's `/api/v0/add`; this
//! is explicitly not a real-IPFS test.

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use oag_listend::ipfs::IpfsPublisher;
use oag_listend::{wav, AudioFormat, AudioStore, MirrorOutcome};

const BOUND: Duration = Duration::from_secs(20);

/// A one-shot stub that answers like Kubo, and records the request body it was sent.
async fn stub(response: String) -> (String, Arc<Mutex<Vec<u8>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("stub addr");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&seen);
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 64 * 1024];
        let mut got = Vec::new();
        loop {
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
                .await
                .expect("read bound")
                .expect("read");
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
            if got.ends_with(b"--\r\n") {
                break;
            }
        }
        *captured.lock().unwrap() = got.clone();
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write reply");
        stream.flush().await.expect("flush");
        let _ = stream.shutdown().await;
    });
    (format!("127.0.0.1:{port}", port = addr.port()), seen)
}

#[tokio::test]
async fn accepted_audio_is_offered_to_ipfs_and_the_cid_is_recorded() {
    let outcome = tokio::time::timeout(BOUND, mirror_ok()).await;
    outcome.expect("the mirror exchange must complete within the bound")
}

async fn mirror_ok() {
    let (addr, seen) = stub(
        "HTTP/1.1 200 OK\r\nContent-Length: 58\r\n\r\n{\"Hash\":\"bafyaccepted\",\"Name\":\"audio\",\"Size\":\"8\"}\r\n".into(),
    )
    .await;
    let mut store = AudioStore::default();
    store.set_ipfs(Some(IpfsPublisher::new(addr)));

    let tone = wav::generate(5, wav::Sound::Tone);
    let hash = store
        .accept(tone.clone(), Some("audio/wav".into()))
        .await
        .expect("the audio must still be accepted");

    assert_eq!(store.mirror_summary(), (1, 0), "one mirror, no failures");
    assert_eq!(
        store.mirror_log().back(),
        Some(&MirrorOutcome::Mirrored {
            content_hash: hash.clone(),
            cid: "bafyaccepted".into(),
        })
    );

    // The stub really was asked, with these bytes, as a multipart file part.
    let sent_bytes = seen.lock().unwrap().clone();
    let sent = String::from_utf8_lossy(&sent_bytes).into_owned();
    assert!(sent.starts_with("POST /api/v0/add HTTP/1.1"), "{sent}");
    assert!(sent.contains("name=\"file\""), "{sent}");
    assert!(
        sent_bytes.windows(tone.len()).any(|w| w == tone.as_slice()),
        "the posted body must carry the audio"
    );
    assert_eq!(store.len(), 1, "the audio is held locally regardless");
}

#[tokio::test]
async fn a_mirror_failure_is_recorded_and_does_not_lose_the_audio() {
    let outcome = tokio::time::timeout(BOUND, mirror_fails()).await;
    outcome.expect("the failed-mirror exchange must complete within the bound")
}

async fn mirror_fails() {
    let (addr, _seen) =
        stub("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".into()).await;
    let mut store = AudioStore::default();
    store.set_ipfs(Some(IpfsPublisher::new(addr)));

    let noise = wav::generate(6, wav::Sound::Noise);
    let hash = store
        .accept(noise.clone(), None)
        .await
        .expect("an IPFS failure must not refuse the audio");

    assert_eq!(
        store.len(),
        1,
        "the audio is stored despite the mirror failing"
    );
    assert!(
        store.serve(&hash).is_ok(),
        "and it is served, byte for byte"
    );
    assert_eq!(store.mirror_summary(), (0, 1));
    match store.mirror_log().back() {
        Some(MirrorOutcome::Failed {
            content_hash,
            reason,
        }) => {
            assert_eq!(content_hash, &hash);
            assert!(reason.contains("500"), "{reason}");
        }
        other => panic!("expected a recorded failure, got {other:?}"),
    }
}

/// An unreachable API behaves the same way: reported, never fatal.
#[tokio::test]
async fn an_unreachable_api_is_reported_not_fatal() {
    let mut store = AudioStore::default();
    store.set_ipfs(Some(IpfsPublisher::new("127.0.0.1:1")));
    let tone = wav::generate(7, wav::Sound::Silence);
    let hash = store
        .accept(tone.clone(), None)
        .await
        .expect("an unreachable mirror must not refuse the audio");
    assert_eq!(AudioFormat::sniff(&tone), Some(AudioFormat::Wav));
    assert_eq!(store.mirror_summary(), (0, 1));
    assert!(store.serve(&hash).is_ok());
}
