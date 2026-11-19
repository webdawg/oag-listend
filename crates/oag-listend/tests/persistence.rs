//! Persistence: audio accepted before a shutdown is served after it. The store is
//! content-addressed, so a restart is a directory load through the same sniff, not a replay of
//! trusted bytes.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioItem, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "oag-listend-persist-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn fetch(addr: std::net::SocketAddr, hash: &str) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
        content_hash: hash.to_string(),
    }));
    stream.write_all(&frame).await.expect("write");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, stream.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("body");
    protocol::decode_response(&body).expect("decode")
}

async fn publish(addr: std::net::SocketAddr, bytes: Vec<u8>) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes,
    }));
    stream.write_all(&frame).await.expect("write");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(BOUND, stream.read_exact(&mut body))
        .await
        .expect("bound")
        .expect("body");
    match protocol::decode_response(&body).expect("decode") {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    }
}

#[tokio::test]
async fn audio_accepted_before_a_shutdown_is_served_after_it() {
    let dir = temp_dir("restart");
    let outcome = tokio::time::timeout(BOUND, restart(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the restart exchange must complete within the bound")
}

async fn restart(dir: &std::path::Path) {
    let tone = wav::generate(31, wav::Sound::Tone);

    // First process: accept a sound, then stop.
    let (store, report) = AudioStore::with_directory(dir, 1024 * 1024).expect("open store");
    assert_eq!(report.loaded, 0);
    let (server, addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(store)))
        .await
        .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });
    let hash = publish(addr, tone.clone()).await;
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
    assert!(
        dir.join(&hash).exists(),
        "the accepted audio must be on disk"
    );

    // Second process: load the directory and serve the same bytes over the wire.
    let (store, report) = AudioStore::with_directory(dir, 1024 * 1024).expect("reopen store");
    assert_eq!(report.loaded, 1, "the restart must load what was stored");
    assert_eq!(report.skipped, 0);
    let (server, addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(store)))
        .await
        .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    match fetch(addr, &hash).await {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert_eq!(content_hash, hash);
            assert_eq!(
                bytes, tone,
                "the restarted daemon must serve the same audio"
            );
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("expected Audio after restart, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
}

/// The listing a client walks must be the same walk after a restart. Insertion order is not
/// surviveable (a restart rebuilds the store in directory-iteration order), so the listing is
/// sorted by hash.
#[tokio::test]
async fn the_listing_walk_is_the_same_after_a_restart() {
    let dir = temp_dir("listing");
    let outcome = tokio::time::timeout(BOUND, listing(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the listing exchange must complete within the bound")
}

async fn listing(dir: &std::path::Path) {
    let first_walk;
    {
        let (mut store, _) = AudioStore::with_directory(dir, 1024 * 1024).expect("open");
        for seed in [50, 17, 91, 3] {
            store
                .accept(wav::generate(seed, wav::Sound::Tone), None)
                .await
                .expect("accept");
        }
        first_walk = store.hashes();
        let mut sorted = first_walk.clone();
        sorted.sort();
        assert_eq!(first_walk, sorted, "the listing is sorted");
    }
    let (store, report) = AudioStore::with_directory(dir, 1024 * 1024).expect("reopen");
    assert_eq!(report.loaded, 4);
    assert_eq!(
        store.hashes(),
        first_walk,
        "a restart must not change the walk a client sees"
    );
}

/// A sidecar is metadata in an operator-controlled directory, so it is bounded before it is read.
/// An oversized `.json` or `.cid` is ignored rather than pulled into memory -- and a `.cid` that is
/// not a plausible identifier is ignored too -- while the audio itself still loads and serves. The
/// small sidecar in the same test proves the guard is what ignores the large one, not a path that
/// never reads sidecars at all.
#[tokio::test]
async fn oversized_sidecars_are_ignored_and_the_audio_still_loads() {
    let dir = temp_dir("sidecars");
    let outcome = tokio::time::timeout(BOUND, sidecars(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the sidecar exchange must complete within the bound")
}

async fn sidecars(dir: &std::path::Path) {
    // One item with a small, honest media-type sidecar: it must be read.
    let small = wav::generate(77, wav::Sound::Tone);
    let small_item = AudioItem::new(small.clone(), Some("audio/wav".to_string()));
    std::fs::write(dir.join(&small_item.content_hash), &small).expect("write audio");
    std::fs::write(
        dir.join(format!("{}.json", small_item.content_hash)),
        b"audio/wav",
    )
    .expect("write sidecar");

    // One item whose sidecars are both over their bounds, and whose CID sidecar is implausible.
    let big = wav::generate(78, wav::Sound::Tone);
    let big_item = AudioItem::new(big.clone(), Some("audio/wav".to_string()));
    std::fs::write(dir.join(&big_item.content_hash), &big).expect("write audio");
    std::fs::write(
        dir.join(format!("{}.json", big_item.content_hash)),
        vec![b'x'; 4097],
    )
    .expect("write an oversized media sidecar");
    std::fs::write(
        dir.join(format!("{}.cid", big_item.content_hash)),
        vec![b'c'; 129],
    )
    .expect("write an oversized cid sidecar");

    let (store, report) = AudioStore::with_directory(dir, 1024 * 1024).expect("open store");
    assert_eq!(report.loaded, 2, "both audio files must load");
    assert_eq!(report.skipped, 0, "neither is corrupt");

    let served = store.serve(&small_item.content_hash).expect("served");
    assert_eq!(served.bytes, small);
    assert_eq!(
        served.media_type.as_deref(),
        Some("audio/wav"),
        "a small sidecar is read"
    );

    let served = store.serve(&big_item.content_hash).expect("served");
    assert_eq!(served.bytes, big, "the audio is unaffected by its sidecars");
    assert_eq!(
        served.media_type, None,
        "an oversized media-type sidecar is ignored, not read"
    );
    assert_eq!(
        store.content_id(&big_item.content_hash),
        None,
        "an oversized CID sidecar is ignored"
    );
}
