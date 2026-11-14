//! `oag-listend` -- run the daemon.
//!
//!     oag-listend --listen 0.0.0.0:7100 --seed 20261112
//!
//! It binds, publishes a small seeded set of generated sounds, and serves audio until interrupted.
//! Every inbound publish still goes through `AudioStore::accept`, so the daemon accepts audio and
//! nothing else.

use std::time::Duration;

use oag_listend::{AudioStore, ListendServer, DEFAULT_MAX_BYTES};

struct Args {
    listen: String,
    seed: u64,
    max_bytes: usize,
    no_generate: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: "0.0.0.0:7100".to_string(),
        seed: default_seed(),
        max_bytes: DEFAULT_MAX_BYTES,
        no_generate: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" => {
                args.listen = it.next().expect("--listen needs an address");
            }
            "--seed" => {
                args.seed = it
                    .next()
                    .expect("--seed needs a u64")
                    .parse()
                    .expect("--seed must be a u64");
            }
            "--max-bytes" => {
                args.max_bytes = it
                    .next()
                    .expect("--max-bytes needs a byte count")
                    .parse()
                    .expect("--max-bytes must be a byte count");
            }
            "--no-generate" => args.no_generate = true,
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument {other}\n{}", usage());
                std::process::exit(2);
            }
        }
    }
    args
}

fn usage() -> String {
    format!(
        "oag-listend -- the global daemon that serves audio\n\n\
         Usage: oag-listend [--listen ADDR] [--seed U64] [--max-bytes N] [--no-generate]\n\n\
           --listen ADDR    bind address (default 0.0.0.0:7100)\n\
           --seed U64       seed for the startup sounds (default: clock)\n\
           --max-bytes N    store bound in bytes (default {DEFAULT_MAX_BYTES})\n\
           --no-generate    do not publish generated sounds at startup\n\n\
         Only audio is accepted. Everything else is rejected before it is stored."
    )
}

/// A seed from the clock and the process id, so two daemons on one machine do not sound alike.
fn default_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

fn main() {
    let args = parse_args();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let store = AudioStore::with_capacity(args.max_bytes);
        let (server, addr) = ListendServer::bind(&args.listen, store)
            .await
            .unwrap_or_else(|e| {
                eprintln!("oag-listend: cannot bind {}: {e}", args.listen);
                std::process::exit(1);
            });

        if !args.no_generate {
            for hash in server.publish_generated(args.seed, oag_listend::server::STARTUP_SOUNDS).await {
                println!("  generated {}", &hash[..16]);
            }
        }

        println!("oag-listend listening on {addr}");
        println!("  seed {}  max-bytes {}", args.seed, args.max_bytes);
        println!("  only audio is accepted; everything else is rejected before it is stored");

        let run = tokio::spawn(async move { server.run().await });
        let _ = tokio::signal::ctrl_c().await;
        println!("oag-listend shutting down");
        // The spawned task owns the server; give it a moment to stop accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        run.abort();
    });
}
