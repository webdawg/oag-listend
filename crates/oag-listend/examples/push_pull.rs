//! Push a generated sound to a running daemon and pull it back, verifying the bytes.
//!
//!     oag-listend --listen 127.0.0.1:7100 --udp 127.0.0.1:7120 &
//!     cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
//!     cargo run -p oag-listend --example push_pull -- --udp --addr 127.0.0.1:7120
//!
//! This is the smallest honest demonstration of the founding sentence: the daemon is on, audio
//! goes in, audio comes back, and the receiver proves it is the same audio. Works over the native
//! TCP carrier, the UDP datagram carrier, and a Unix domain socket.

#![forbid(unsafe_code)]

use std::time::Duration;

use oag_listend::protocol::{self, Request, Response};
use oag_listend::udp::MAX_DATAGRAM_BYTES;
use oag_listend::{wav, AudioItem};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

struct Args {
    addr: String,
    udp: bool,
    unix: Option<String>,
    seed: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        addr: "127.0.0.1:7100".into(),
        udp: false,
        unix: None,
        seed: 1,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--addr" => args.addr = flag_value(&mut it, "--addr"),
            "--udp" => args.udp = true,
            "--unix" => args.unix = Some(flag_value(&mut it, "--unix")),
            "--version" | "-V" => {
                println!(
                    "push_pull {} (frame protocol {})",
                    env!("CARGO_PKG_VERSION"),
                    oag_listend::protocol::PROTOCOL_VERSION
                );
                std::process::exit(0);
            }
            "--help" | "-h" => {
                println!(
                    "push_pull -- push audio to a daemon and pull it back\n\n\
                     Usage: push_pull [--addr ADDR] [--udp] [--unix PATH] [--seed U64]\n\
                                      [--version] [--help]\n\n\
                       --addr ADDR   daemon address (default 127.0.0.1:7100)\n\
                       --udp         use the UDP datagram carrier\n\
                       --unix PATH   use the Unix domain socket at PATH instead of a network\n\
                       --seed U64    seed for the generated sounds (default 1)\n\
                       --version     print the version and the frame protocol version\n\
                       --help        print this usage\n\n\
                     Says hello, prints the daemon's musical liveness answer, publishes a tone and
\
                     a noise, fetches both back and verifies their BLAKE3, then proves the inbound
\
                     rule by sending non-audio and printing the rejection."
                );
                std::process::exit(0);
            }
            "--seed" => {
                let raw = flag_value(&mut it, "--seed");
                args.seed = match raw.parse() {
                    Ok(seed) => seed,
                    Err(_) => {
                        eprintln!("--seed must be a u64: {raw}");
                        std::process::exit(2);
                    }
                };
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

fn flag_value(it: &mut impl Iterator<Item = String>, flag: &str) -> String {
    match it.next() {
        Some(value) => value,
        None => {
            eprintln!("{flag} needs a value");
            std::process::exit(2);
        }
    }
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
    if let Some(path) = &args.unix {
        // The same session loop as TCP, over a filesystem socket: identical protocol and bounds.
        let mut stream = tokio::net::UnixStream::connect(path).await?;
        stream.write_all(&frame).await?;
        stream.flush().await?;
        return decode_body(&read_frame(&mut stream).await?);
    }
    if args.udp {
        let addr: std::net::SocketAddr = match args.addr.parse() {
            Ok(addr) => addr,
            Err(_) => {
                eprintln!("--udp needs a socket address: {}", args.addr);
                std::process::exit(2);
            }
        };
        // Bind a socket of the target's family: an IPv6 peer cannot be reached from an IPv4 socket,
        // and the error ("address family not supported") gives no hint that the fix is a v6 bind.
        let local = if addr.is_ipv6() {
            "[::]:0"
        } else {
            "127.0.0.1:0"
        };
        let socket = UdpSocket::bind(local).await?;
        if frame.len() > MAX_DATAGRAM_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "frame is {} bytes, larger than a {} byte datagram",
                    frame.len(),
                    MAX_DATAGRAM_BYTES
                ),
            ));
        }
        socket.send_to(&frame, addr).await?;
        let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
        let (len, _) = tokio::time::timeout(Duration::from_secs(10), socket.recv_from(&mut buf))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "udp read timed out")
            })??;
        decode_frame_bytes(&buf[..len])
    } else {
        let mut stream = TcpStream::connect(&args.addr).await?;
        stream.write_all(&frame).await?;
        stream.flush().await?;
        decode_body(&read_frame(&mut stream).await?)
    }
}

/// Read one length-prefixed frame from a stream.
///
/// The declared length is checked **before** the body is allocated, exactly as the daemon's own
/// reader does. Reading first and allocating afterwards let a hostile or broken daemon declare
/// `u32::MAX` and make the client reserve 4 GiB.
async fn read_frame<S: AsyncReadExt + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
    let mut prefix = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut prefix))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;
    let len = u32::from_be_bytes(prefix) as usize;
    if len > oag_listend::protocol::MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "the daemon declared a {len} byte frame, over the {} byte cap",
                oag_listend::protocol::MAX_FRAME_BYTES
            ),
        ));
    }
    let mut body = vec![0u8; len];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut body))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;
    Ok(body)
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

    // The musical liveness answer (operator directive: even pings are music).
    match round_trip(args, &Request::Ping).await? {
        Response::Audio {
            content_hash,
            bytes,
            cid: _,
        } => println!(
            "ping:    {} ({} bytes, verified={})",
            &content_hash[..16],
            bytes.len(),
            AudioItem::verify(&content_hash, &bytes)
        ),
        other => println!("ping:    {other:?}"),
    }

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

        match round_trip(
            args,
            &Request::Fetch {
                content_hash: hash.clone(),
            },
        )
        .await?
        {
            Response::Audio {
                content_hash,
                bytes: fetched,
                cid,
            } => {
                let verified = AudioItem::verify(&content_hash, &fetched);
                println!(
                    "fetched {label}: {} {} {} bytes verified={}{}",
                    &content_hash[..16],
                    if content_hash == hash {
                        "same-hash"
                    } else {
                        "HASH-MISMATCH"
                    },
                    fetched.len(),
                    verified,
                    match cid {
                        Some(cid) => format!(" ipfs={cid}"),
                        None => String::new(),
                    }
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
