//! Push a generated sound to a running daemon and pull it back, verifying the bytes.
//!
//!     oag-listend --listen 127.0.0.1:7100 --udp 127.0.0.1:7120 &
//!     cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
//!     cargo run -p oag-listend --example push_pull -- --udp --addr 127.0.0.1:7120
//!
//! This is the smallest honest demonstration of the founding sentence: the daemon is on, audio
//! goes in, audio comes back, and the receiver proves it is the same audio. Works over the native
//! TCP carrier and the UDP datagram carrier.

use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::udp::MAX_DATAGRAM_BYTES;
use oag_listend::{AudioItem, wav};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

struct Args {
    addr: String,
    udp: bool,
    seed: u64,
}

fn parse_args() -> Args {
    let mut args = Args { addr: "127.0.0.1:7100".into(), udp: false, seed: 1 };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--addr" => args.addr = it.next().expect("--addr needs an address"),
            "--udp" => args.udp = true,
            "--seed" => args.seed = it.next().expect("--seed needs a u64").parse().expect("u64"),
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let outcome = tokio::time::timeout(Duration::from_secs(10), exchange(&args)).await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("push_pull: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("push_pull: timed out after 10s");
            std::process::exit(1);
        }
    }
}

/// One request/response exchange over either carrier. The frame codec is identical; only the
/// transport differs.
async fn round_trip(args: &Args, request: &Request) -> std::io::Result<Response> {
    let frame = protocol::encode_frame(&protocol::encode_request(request));
    if args.udp {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let addr: std::net::SocketAddr = args.addr.parse().expect("--udp needs a socket address");
        if frame.len() > MAX_DATAGRAM_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("frame is {} bytes, larger than a {} byte datagram", frame.len(), MAX_DATAGRAM_BYTES),
            ));
        }
        socket.send_to(&frame, addr).await?;
        let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
        let (len, _) = tokio::time::timeout(Duration::from_secs(10), socket.recv_from(&mut buf))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "udp read timed out"))??;
        decode_frame_bytes(&buf[..len])
    } else {
        let mut stream = TcpStream::connect(&args.addr).await?;
        stream.write_all(&frame).await?;
        stream.flush().await?;
        let mut prefix = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut prefix))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;
        let len = u32::from_be_bytes(prefix) as usize;
        let mut body = vec![0u8; len];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut body))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;
        decode_body(&body)
    }
}

/// A datagram arrives as a whole frame, prefix included.
fn decode_frame_bytes(frame: &[u8]) -> std::io::Result<Response> {
    let body = protocol::decode_frame(frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    decode_body(body)
}

/// A stream read has already had its prefix stripped.
fn decode_body(body: &[u8]) -> std::io::Result<Response> {
    protocol::decode_response(body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

async fn exchange(args: &Args) -> std::io::Result<()> {
    let hello = round_trip(args, &Request::Hello).await?;
    println!("daemon: {hello:?}");

    let tone = wav::generate(args.seed, wav::Sound::Tone);
    let noise = wav::generate(args.seed + 1, wav::Sound::Noise);

    for (label, bytes) in [("tone", tone), ("noise", noise)] {
        let hash = match round_trip(
            args,
            &Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: bytes.clone(),
            },
        )
        .await?
        {
            Response::Published { content_hash } => content_hash,
            other => {
                eprintln!("push_pull: expected Published, got {other:?}");
                std::process::exit(1);
            }
        };
        println!("published {label}: {} ({} bytes)", &hash[..16], bytes.len());

        match round_trip(args, &Request::Fetch { content_hash: hash.clone() }).await? {
            Response::Audio { content_hash, bytes: fetched } => {
                let verified = AudioItem::verify(&content_hash, &fetched);
                println!(
                    "fetched {label}: {} {} {} bytes verified={}",
                    &content_hash[..16],
                    if content_hash == hash { "same-hash" } else { "HASH-MISMATCH" },
                    fetched.len(),
                    verified
                );
                if !verified || fetched != bytes {
                    eprintln!("push_pull: the bytes came back different");
                    std::process::exit(1);
                }
            }
            other => {
                eprintln!("push_pull: expected Audio, got {other:?}");
                std::process::exit(1);
            }
        }
    }

    // Prove the inbound rule over the wire too.
    let refused = round_trip(
        args,
        &Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: b"<html>not audio</html>".to_vec(),
        },
    )
    .await?;
    println!("non-audio: {refused:?}");
    Ok(())
}
