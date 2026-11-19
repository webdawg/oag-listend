//! Eviction: a bounded store drops the oldest audio first, over the wire, and removes the file so
//! the directory agrees with what is served. The store is only a bound if a carrier can observe it.

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
        "oag-listend-evict-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn exchange(addr: std::net::SocketAddr, request: &Request) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = protocol::encode_frame(&protocol::encode_request(request));
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
    match exchange(
        addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes,
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    }
}

async fn fetch(addr: std::net::SocketAddr, hash: &str) -> Response {
    exchange(
        addr,
        &Request::Fetch {
            content_hash: hash.to_string(),
        },
    )
    .await
}

#[tokio::test]
async fn the_oldest_audio_is_dropped_first_and_its_file_is_gone() {
    let dir = temp_dir("oldest");
    let outcome = tokio::time::timeout(BOUND, run(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the eviction exchange must complete within the bound")
}

async fn run(dir: &std::path::Path) {
    // Two tones of different sizes. The bound is exactly the larger one's size, so the larger
    // fits on its own and the smaller still fits, but together they do not.
    let mut sizes: Vec<Vec<u8>> = (0..8u64)
        .map(|seed| wav::generate(seed, wav::Sound::Tone))
        .collect();
    sizes.sort_by_key(|bytes| std::cmp::Reverse(bytes.len()));
    let larger = sizes.first().expect("at least one tone").clone();
    let smaller = sizes.last().expect("at least one tone").clone();
    assert!(
        smaller.len() < larger.len(),
        "the two tones must differ in size for this test to be meaningful"
    );

    let (store, _) = AudioStore::with_directory(dir, larger.len()).expect("open store");
    assert_eq!(store.max_bytes(), larger.len());
    let (server, addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(store)))
        .await
        .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    // The first publish fills the store exactly.
    let old_hash = publish(addr, larger.clone()).await;
    assert!(dir.join(&old_hash).exists(), "the first item is on disk");
    match fetch(addr, &old_hash).await {
        Response::Audio { bytes, .. } => {
            assert!(
                AudioItem::verify(&old_hash, &bytes),
                "the first item verifies"
            );
        }
        other => panic!("expected Audio for the first item, got {other:?}"),
    }

    // The second publish pushes the total over the bound, so the oldest is evicted.
    let new_hash = publish(addr, smaller.clone()).await;
    assert_ne!(old_hash, new_hash);
    match fetch(addr, &old_hash).await {
        Response::NotFound { .. } => {}
        other => panic!("the evicted item must be not found, got {other:?}"),
    }
    match fetch(addr, &new_hash).await {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert_eq!(content_hash, new_hash);
            assert_eq!(bytes, smaller, "the surviving item serves its own bytes");
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("the newest item must survive, got {other:?}"),
    }

    // Eviction is off the lock but awaited before the answer, so by now the file is gone.
    assert!(
        !dir.join(&old_hash).exists(),
        "an evicted item's file must be unlinked, not left behind"
    );
    assert!(dir.join(&new_hash).exists());

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
}

/// A restart must honour the store bound too. Loading a directory whose contents exceed
/// `--max-bytes` drops the surplus and unlinks it, exactly as a runtime publish does -- otherwise an
/// operator who lowers the bound would find the daemon holding more than it advertises, with the
/// surplus files never leaving the disk.
#[tokio::test]
async fn a_restart_over_the_bound_evicts_and_unlinks() {
    let dir = temp_dir("load-evict");
    let outcome = tokio::time::timeout(BOUND, load_evict(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the load exchange must complete within the bound")
}

async fn load_evict(dir: &std::path::Path) {
    let mut sizes: Vec<Vec<u8>> = (0..3u64)
        .map(|seed| wav::generate(seed, wav::Sound::Tone))
        .collect();
    sizes.sort_by_key(|bytes| std::cmp::Reverse(bytes.len()));
    let rest = &sizes[1..];
    let bound = rest.iter().map(|bytes| bytes.len()).sum::<usize>();

    for bytes in &sizes {
        let item = AudioItem::new(bytes.clone(), None);
        std::fs::write(dir.join(&item.content_hash), bytes).expect("write audio");
    }

    let (store, report) = AudioStore::with_directory(dir, bound).expect("open store");

    // The directory is read in filesystem order, so *which* item is dropped is not deterministic --
    // but the invariants are, and they are what an operator depends on.
    assert!(
        store.bytes_held() <= bound,
        "after a load the store must not hold more than the bound (held {}, bound {})",
        store.bytes_held(),
        bound
    );
    assert_eq!(
        report.loaded,
        store.len(),
        "the report counts what is served"
    );
    assert_eq!(
        report.evicted + report.loaded,
        3,
        "every audio file is either loaded or counted as evicted"
    );
    assert!(report.evicted >= 1, "a load over the bound must evict");

    // The directory and the index must agree: nothing served that is not on disk, and no file
    // left behind for an item that was dropped.
    let remaining = std::fs::read_dir(dir)
        .expect("read the store directory")
        .filter_map(|entry| entry.ok())
        .count();
    assert_eq!(
        remaining, report.loaded,
        "the directory must agree with the index after a load eviction"
    );
    for bytes in &sizes {
        let item = AudioItem::new(bytes.clone(), None);
        assert_eq!(
            dir.join(&item.content_hash).exists(),
            store.serve(&item.content_hash).is_ok(),
            "a file on disk and an item in the index must be the same set"
        );
    }
}
