//! The UDP carrier over a real socket: one datagram in, one datagram out, same audio, and the
//! same inbound rule.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::udp::{UdpCarrier, MAX_DATAGRAM_BYTES, MAX_DATAGRAM_LIST_LIMIT};
use oag_listend::{wav, AudioItem, AudioStore};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

async fn exchange(socket: &UdpSocket, addr: std::net::SocketAddr, request: &Request) -> Response {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    assert!(
        frame.len() <= MAX_DATAGRAM_BYTES,
        "request must fit a datagram"
    );
    socket.send_to(&frame, addr).await.expect("send datagram");
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
    let (len, from) = tokio::time::timeout(BOUND, socket.recv_from(&mut buf))
        .await
        .expect("datagram within the bound")
        .expect("recv");
    assert_eq!(from, addr, "the reply comes from the daemon");
    protocol::decode_response(protocol::decode_frame(&buf[..len]).expect("frame"))
        .expect("response")
}

#[tokio::test]
async fn udp_carries_the_same_audio() {
    tokio::time::timeout(BOUND, round_trip())
        .await
        .expect("udp exchange within the bound")
}

async fn round_trip() {
    let store = Arc::new(Mutex::new(AudioStore::default()));
    let (carrier, addr) = UdpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&store))
        .await
        .expect("bind udp");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind client");

    assert_eq!(
        exchange(&socket, addr, &Request::Hello).await,
        Response::Hello {
            name: oag_listend::protocol::DAEMON_NAME.to_string(),
            protocol_version: oag_listend::protocol::PROTOCOL_VERSION,
            item_count: 0
        }
    );

    let tone = wav::generate(23, wav::Sound::Tone);
    let hash = match exchange(
        &socket,
        addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: tone.clone(),
        },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(AudioItem::verify(&hash, &tone));

    match exchange(
        &socket,
        addr,
        &Request::Fetch {
            content_hash: hash.clone(),
        },
    )
    .await
    {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert_eq!(content_hash, hash);
            assert_eq!(bytes, tone, "the datagram must carry the same audio");
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    // The musical ping is exposed here too (operator directive: every carrier).
    match exchange(&socket, addr, &Request::Ping).await {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => {
            assert!(!bytes.is_empty(), "a ping must never be an empty pong");
            assert_eq!(
                oag_listend::AudioFormat::sniff(&bytes),
                Some(oag_listend::AudioFormat::Wav)
            );
            assert!(AudioItem::verify(&content_hash, &bytes));
        }
        other => panic!("a ping must answer with audio, got {other:?}"),
    }

    // The inbound rule holds on the datagram carrier too.
    match exchange(
        &socket,
        addr,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: b"<html>".to_vec(),
        },
    )
    .await
    {
        Response::Rejected { .. } => {}
        other => panic!("expected a rejection, got {other:?}"),
    }

    assert_eq!(
        exchange(
            &socket,
            addr,
            &Request::List {
                offset: 0,
                limit: 100
            }
        )
        .await,
        Response::Listing {
            content_hashes: vec![hash],
            total: 1,
            offset: 0
        }
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}

/// The listing page is capped at what a datagram can carry. With more audio than one page holds, a
/// client asking for everything must get exactly the cap -- never a frame too large for a datagram,
/// which the carrier would have to refuse anyway.
#[tokio::test]
async fn the_udp_listing_page_is_capped_at_what_a_datagram_can_carry() {
    tokio::time::timeout(BOUND, capped_listing())
        .await
        .expect("the capped-listing exchange must complete within the bound")
}

async fn capped_listing() {
    // Fill the store directly, and past what one datagram can carry: at 66 bytes a hash, a datagram
    // holds about 990 of them. Doing it in memory keeps the test from spending a thousand round
    // trips proving the same thing.
    let mut store = AudioStore::default();
    let mut hashes = Vec::new();
    for seed in 0..1_100u64 {
        // Noise, not tone: a tone's pitch repeats every 620 seeds, so it would silently publish
        // duplicates and shrink the store under test.
        let wav = wav::generate(seed, wav::Sound::Noise);
        hashes.push(store.accept(wav, None).await.expect("accept"));
    }
    let unique: std::collections::HashSet<&String> = hashes.iter().collect();
    assert_eq!(unique.len(), 1_100, "every fixture must be distinct audio");
    hashes.sort();

    let shared = Arc::new(Mutex::new(store));
    let (carrier, addr) = UdpCarrier::bind_shared("127.0.0.1:0", Arc::clone(&shared))
        .await
        .expect("bind udp");
    let shutdown = carrier.shutdown_handle();
    let run = tokio::spawn(async move { carrier.run().await });

    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind client");
    // Ask for everything, so only the carrier's own cap can bound the page.
    let frame = protocol::encode_frame(&protocol::encode_request(&Request::List {
        offset: 0,
        limit: u32::MAX as usize,
    }));
    assert!(
        frame.len() <= MAX_DATAGRAM_BYTES,
        "request must fit a datagram"
    );
    socket.send_to(&frame, addr).await.expect("send");

    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
    let (len, _) = tokio::time::timeout(BOUND, socket.recv_from(&mut buf))
        .await
        .expect("datagram within the bound")
        .expect("recv");
    assert!(
        len <= MAX_DATAGRAM_BYTES,
        "a datagram reply fits a datagram"
    );

    match protocol::decode_response(protocol::decode_frame(&buf[..len]).expect("frame"))
        .expect("decode")
    {
        Response::Listing {
            content_hashes,
            total,
            offset,
        } => {
            assert_eq!(total, 1_100, "the whole store is reported");
            assert_eq!(offset, 0);
            assert_eq!(
                content_hashes.len(),
                MAX_DATAGRAM_LIST_LIMIT,
                "the page must stop at what a datagram can carry"
            );
            assert_eq!(
                content_hashes,
                hashes[..MAX_DATAGRAM_LIST_LIMIT].to_vec(),
                "the first page is the first of the sorted walk"
            );
        }
        other => panic!("expected a listing, got {other:?}"),
    }

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}
