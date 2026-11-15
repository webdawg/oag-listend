//! Concurrency: many clients publishing and fetching at once, against one store. The store is a
//! single mutex, so this is where a lost update or a held-lock bug would show. Bounded: a few
//! hundred small items, one timeout over the whole test.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{wav, AudioItem, AudioStore, ListendServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(30);

async fn publish_and_fetch(addr: std::net::SocketAddr, wav: Vec<u8>) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: wav.clone(),
    }));
    stream.write_all(&frame).await.expect("write publish");
    stream.flush().await.expect("flush publish");
    let hash = match read_response(&mut stream).await {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(AudioItem::verify(&hash, &wav));

    let frame = protocol::encode_frame(&protocol::encode_request(&Request::Fetch {
        content_hash: hash.clone(),
    }));
    stream.write_all(&frame).await.expect("write fetch");
    stream.flush().await.expect("flush fetch");
    match read_response(&mut stream).await {
        Response::Audio {
            content_hash,
            bytes,
        } => {
            assert_eq!(content_hash, hash);
            assert_eq!(bytes, wav, "the bytes must come back exactly");
            hash
        }
        other => panic!("expected Audio, got {other:?}"),
    }
}

async fn read_response(stream: &mut TcpStream) -> Response {
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

#[tokio::test]
async fn concurrent_publishers_all_land_and_verify() {
    let outcome = tokio::time::timeout(BOUND, concurrent_memory()).await;
    outcome.expect("the concurrent exchange must complete within the bound")
}

async fn concurrent_memory() {
    let (server, addr) =
        ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(AudioStore::default())))
            .await
            .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut tasks = Vec::new();
    for task in 0..16u64 {
        let addr = addr;
        tasks.push(tokio::spawn(async move {
            let mut hashes = Vec::new();
            for i in 0..20u64 {
                let wav = wav::generate(task * 1000 + i, wav::Sound::Tone);
                hashes.push(publish_and_fetch(addr, wav).await);
            }
            hashes
        }));
    }

    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.expect("join"));
    }
    assert_eq!(all.len(), 320, "every publish must be answered");
    let unique: std::collections::HashSet<&String> = all.iter().collect();
    assert_eq!(
        unique.len(),
        320,
        "distinct audio must yield distinct hashes"
    );

    let listing = {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let frame = protocol::encode_frame(&protocol::encode_request(&Request::List {
            offset: 0,
            limit: 10_000,
        }));
        stream.write_all(&frame).await.expect("write list");
        stream.flush().await.expect("flush list");
        match read_response(&mut stream).await {
            Response::Listing {
                content_hashes,
                total,
                ..
            } => {
                assert_eq!(total, 320, "the store must hold every item");
                content_hashes
            }
            other => panic!("expected a listing, got {other:?}"),
        }
    };
    assert_eq!(listing.len(), 320);

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");
}

/// The persistent path does a blocking `std::fs::write` while holding the store mutex. Concurrent
/// publishers must not lose an item or deadlock, and a restart must find every one of them.
#[tokio::test]
async fn concurrent_publishers_persist_every_item() {
    let dir = std::env::temp_dir().join(format!(
        "oag-listend-concurrent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let outcome = tokio::time::timeout(BOUND, concurrent_persistent(&dir)).await;
    let _ = std::fs::remove_dir_all(&dir);
    outcome.expect("the concurrent persistent exchange must complete within the bound")
}

async fn concurrent_persistent(dir: &std::path::Path) {
    let (store, _) = AudioStore::with_directory(dir, 64 * 1024 * 1024).expect("open store");
    let (server, addr) = ListendServer::bind_shared("127.0.0.1:0", Arc::new(Mutex::new(store)))
        .await
        .expect("bind");
    let shutdown = server.shutdown_handle();
    let run = tokio::spawn(async move { server.run().await });

    let mut tasks = Vec::new();
    for task in 0..8u64 {
        let addr = addr;
        tasks.push(tokio::spawn(async move {
            let mut hashes = Vec::new();
            for i in 0..10u64 {
                let wav = wav::generate(task * 100 + i, wav::Sound::Noise);
                hashes.push(publish_and_fetch(addr, wav).await);
            }
            hashes
        }));
    }
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.expect("join"));
    }
    assert_eq!(all.len(), 80);

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bound")
        .expect("join")
        .expect("run");

    let (reopened, report) = AudioStore::with_directory(dir, 64 * 1024 * 1024).expect("reopen");
    assert_eq!(
        report.loaded, 80,
        "every concurrently published item must survive"
    );
    assert_eq!(report.skipped, 0);
    assert_eq!(reopened.len(), 80);
    for hash in &all {
        assert!(
            reopened.serve(hash).is_ok(),
            "{hash} must be served after a restart"
        );
    }
}
