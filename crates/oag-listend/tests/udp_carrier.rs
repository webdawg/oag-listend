//! The UDP carrier over a real socket: one datagram in, one datagram out, same audio, and the
//! same inbound rule.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::udp::{UdpCarrier, MAX_DATAGRAM_BYTES};
use oag_listend::{AudioItem, AudioStore, wav};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const BOUND: Duration = Duration::from_secs(10);

async fn exchange(socket: &UdpSocket, addr: std::net::SocketAddr, request: &Request) -> Response {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    assert!(frame.len() <= MAX_DATAGRAM_BYTES, "request must fit a datagram");
    socket.send_to(&frame, addr).await.expect("send datagram");
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
    let (len, from) = tokio::time::timeout(BOUND, socket.recv_from(&mut buf))
        .await
        .expect("datagram within the bound")
        .expect("recv");
    assert_eq!(from, addr, "the reply comes from the daemon");
    protocol::decode_response(protocol::decode_frame(&buf[..len]).expect("frame")).expect("response")
}

#[tokio::test]
async fn udp_carries_the_same_audio() {
    tokio::time::timeout(BOUND, round_trip()).await.expect("udp exchange within the bound")
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
        Response::Hello { name: oag_listend::protocol::DAEMON_NAME.to_string(), protocol_version: oag_listend::protocol::PROTOCOL_VERSION, item_count: 0 }
    );

    let tone = wav::generate(23, wav::Sound::Tone);
    let hash = match exchange(
        &socket,
        addr,
        &Request::Publish { declared_media_type: Some("audio/wav".into()), bytes: tone.clone() },
    )
    .await
    {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(AudioItem::verify(&hash, &tone));

    match exchange(&socket, addr, &Request::Fetch { content_hash: hash.clone() }).await {
        Response::Audio { content_hash, bytes } => {
            assert_eq!(content_hash, hash);
            assert_eq!(bytes, tone, "the datagram must carry the same audio");
        }
        other => panic!("expected Audio, got {other:?}"),
    }

    // The inbound rule holds on the datagram carrier too.
    match exchange(
        &socket,
        addr,
        &Request::Publish { declared_media_type: Some("audio/wav".into()), bytes: b"<html>".to_vec() },
    )
    .await
    {
        Response::Rejected { .. } => {}
        other => panic!("expected a rejection, got {other:?}"),
    }

    assert_eq!(
        exchange(&socket, addr, &Request::List { offset: 0, limit: 100 }).await,
        Response::Listing { content_hashes: vec![hash], total: 1, offset: 0 }
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown bound")
        .expect("join")
        .expect("run");
}
