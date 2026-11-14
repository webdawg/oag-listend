//! Push a generated sound to a running daemon and pull it back, verifying the bytes.
//!
//!     oag-listend --listen 127.0.0.1:7100 &
//!     cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
//!
//! This is the smallest honest demonstration of the founding sentence: the daemon is on, audio
//! goes in, audio comes back, and the receiver proves it is the same audio.

use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::{AudioItem, wav};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Args {
    addr: String,
    seed: u64,
}

fn parse_args() -> Args {
    let mut args = Args { addr: "127.0.0.1:7100".into(), seed: 1 };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--addr" => args.addr = it.next().expect("--addr needs an address"),
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
    let outcome = tokio::time::timeout(Duration::from_secs(10), exchange(&args.addr, args.seed)).await;
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

async fn exchange(addr: &str, seed: u64) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = |req: &Request| protocol::encode_frame(&protocol::encode_request(req));

    stream.write_all(&request(&Request::Hello)).await?;
    stream.flush().await?;
    let hello = read_response(&mut stream).await?;
    println!("daemon: {hello:?}");

    let tone = wav::generate(seed, wav::Sound::Tone);
    let noise = wav::generate(seed + 1, wav::Sound::Noise);

    for (label, bytes) in [("tone", tone), ("noise", noise)] {
        stream
            .write_all(&request(&Request::Publish {
                declared_media_type: Some("audio/wav".into()),
                bytes: bytes.clone(),
            }))
            .await?;
        stream.flush().await?;
        let hash = match read_response(&mut stream).await? {
            Response::Published { content_hash } => content_hash,
            other => {
                eprintln!("push_pull: expected Published, got {other:?}");
                std::process::exit(1);
            }
        };
        println!("published {label}: {} ({} bytes)", &hash[..16], bytes.len());

        stream
            .write_all(&request(&Request::Fetch { content_hash: hash.clone() }))
            .await?;
        stream.flush().await?;
        match read_response(&mut stream).await? {
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
    stream
        .write_all(&request(&Request::Publish {
            declared_media_type: Some("audio/wav".into()),
            bytes: b"<html>not audio</html>".to_vec(),
        }))
        .await?;
    stream.flush().await?;
    println!("non-audio: {:?}", read_response(&mut stream).await?);
    Ok(())
}

async fn read_response(stream: &mut TcpStream) -> std::io::Result<Response> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    protocol::decode_response(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
