//! `oag-listend` -- run the daemon.
//!
//!     oag-listend --listen 0.0.0.0:7100 --http 0.0.0.0:7101 --seed 20261112
//!
//! It binds, publishes a small seeded set of generated sounds, and serves audio until interrupted.
//! Both carriers share one store, and every inbound publish still goes through
//! `AudioStore::accept`, so the daemon accepts audio and nothing else.

use std::sync::Arc;
use std::time::Duration;

use oag_listend::http::HttpCarrier;
use oag_listend::udp::UdpCarrier;
use oag_listend::unix::UnixCarrier;
use oag_listend::{AudioStore, ListendServer, DEFAULT_MAX_BYTES};
use tokio::sync::Mutex;

struct Args {
    listen: String,
    http: Option<String>,
    udp: Option<String>,
    unix: Option<String>,
    dir: Option<String>,
    check: bool,
    seed: u64,
    max_bytes: usize,
    no_generate: bool,
    /// Offer every accepted item to this Kubo node's HTTP API as well. Best-effort.
    ipfs_api: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: "0.0.0.0:7100".to_string(),
        http: None,
        udp: None,
        unix: None,
        dir: None,
        check: false,
        seed: default_seed(),
        max_bytes: DEFAULT_MAX_BYTES,
        no_generate: false,
        ipfs_api: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" => args.listen = value(&mut it, "--listen"),
            "--http" => args.http = Some(value(&mut it, "--http")),
            "--udp" => args.udp = Some(value(&mut it, "--udp")),
            "--unix" => args.unix = Some(value(&mut it, "--unix")),
            "--dir" => args.dir = Some(value(&mut it, "--dir")),
            "--check" => args.check = true,
            "--version" | "-V" => {
                println!(
                    "oag-listend {} (frame protocol {})",
                    env!("CARGO_PKG_VERSION"),
                    oag_listend::protocol::PROTOCOL_VERSION
                );
                std::process::exit(0);
            }
            "--seed" => args.seed = parse_value(&mut it, "--seed"),
            "--max-bytes" => args.max_bytes = parse_value(&mut it, "--max-bytes"),
            "--no-generate" => args.no_generate = true,
            "--ipfs-api" => args.ipfs_api = Some(value(&mut it, "--ipfs-api")),
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

/// The value after a flag, or a usage error. A missing value is an operator mistake, not a panic.
fn value(it: &mut impl Iterator<Item = String>, flag: &str) -> String {
    match it.next() {
        Some(value) => value,
        None => {
            eprintln!("{flag} needs a value\n\n{}", usage());
            std::process::exit(2);
        }
    }
}

/// A numeric flag value, or a usage error.
fn parse_value<T: std::str::FromStr>(it: &mut impl Iterator<Item = String>, flag: &str) -> T {
    let raw = value(it, flag);
    match raw.parse() {
        Ok(parsed) => parsed,
        Err(_) => {
            eprintln!("{flag} must be a valid value: {raw}\n\n{}", usage());
            std::process::exit(2);
        }
    }
}

fn usage() -> String {
    // One literal, so the spacing an operator reads is the spacing written here.
    format!(
        "oag-listend -- the global daemon that serves audio

Usage: oag-listend [--listen ADDR] [--http ADDR] [--udp ADDR] [--unix PATH]
                   [--dir PATH] [--seed U64] [--max-bytes N] [--check]
                   [--no-generate] [--ipfs-api ADDR] [--help]

  --listen ADDR    native protocol bind address (default 0.0.0.0:7100)
  --http ADDR      also serve HTTP at ADDR (GET /audio/{{hash}}, POST /audio)
  --udp ADDR       also serve the native protocol over UDP datagrams
  --unix PATH      also serve the native protocol on a Unix domain socket
  --dir PATH       persist the audio content-addressed under PATH
  --check          open the store, print the load report, and exit
  --version        print the version and the frame protocol version
  --seed U64       seed for the startup sounds (default: clock)
  --max-bytes N    store bound in bytes (default {DEFAULT_MAX_BYTES})
  --no-generate    do not publish generated sounds at startup
  --ipfs-api ADDR  also offer every accepted item to a Kubo node at ADDR (best-effort)

Only audio is accepted. Everything else is rejected before it is stored."
    )
}

/// Say plainly when a bind address is not loopback. The default is `0.0.0.0`, which serves every
/// interface; an operator who did not think about that should be told rather than discover it.
fn note_public_bind(addr: std::net::SocketAddr) {
    if !addr.ip().is_loopback() {
        println!("  note: {addr} is not a loopback address -- this serves audio to that interface");
    }
}

/// A seed from the clock and the process id, so two daemons on one machine do not sound alike.
fn default_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let mut store = match &args.dir {
        Some(dir) => {
            let (store, report) =
                AudioStore::with_directory(dir, args.max_bytes).unwrap_or_else(|e| {
                    eprintln!("oag-listend: cannot open store {dir}: {e}");
                    std::process::exit(1);
                });
            println!(
                "store {dir}: loaded {} skipped {} evicted {}{}",
                report.loaded,
                report.skipped,
                report.evicted,
                if report.truncated {
                    " truncated (more entries than the load considers)"
                } else {
                    ""
                }
            );
            store
        }
        None => AudioStore::with_capacity(args.max_bytes),
    };
    if let Some(addr) = &args.ipfs_api {
        store.set_ipfs(Some(oag_listend::ipfs::IpfsPublisher::new(addr.clone())));
        println!("offering accepted audio to IPFS at {addr} (best-effort)");
    }
    let store = Arc::new(Mutex::new(store));

    if args.check {
        let store = store.lock().await;
        println!(
            "store ok: {} items, {} bytes (bound {}, item bound {})",
            store.len(),
            store.bytes_held(),
            args.max_bytes,
            oag_listend::servable_bound(args.max_bytes)
        );
        return;
    }

    let (server, native_addr) = ListendServer::bind_shared(&args.listen, Arc::clone(&store))
        .await
        .unwrap_or_else(|e| {
            eprintln!("oag-listend: cannot bind {}: {e}", args.listen);
            std::process::exit(1);
        });
    let native_shutdown = server.shutdown_handle();

    let http = match &args.http {
        Some(addr) => {
            let (carrier, http_addr) = HttpCarrier::bind_shared(addr, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    eprintln!("oag-listend: cannot bind http {addr}: {e}");
                    std::process::exit(1);
                });
            println!("oag-listend http on {http_addr}");
            note_public_bind(http_addr);
            Some((carrier, http_addr))
        }
        None => None,
    };
    let http_shutdown = http.as_ref().map(|(carrier, _)| carrier.shutdown_handle());

    let udp = match &args.udp {
        Some(addr) => {
            let (carrier, udp_addr) = UdpCarrier::bind_shared(addr, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    eprintln!("oag-listend: cannot bind udp {addr}: {e}");
                    std::process::exit(1);
                });
            println!("oag-listend udp on {udp_addr}");
            note_public_bind(udp_addr);
            Some((carrier, udp_addr))
        }
        None => None,
    };
    let udp_shutdown = udp.as_ref().map(|(carrier, _)| carrier.shutdown_handle());

    let unix = match &args.unix {
        Some(path) => {
            let (carrier, bound) = UnixCarrier::bind_shared(path, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    eprintln!("oag-listend: cannot bind unix {path}: {e}");
                    std::process::exit(1);
                });
            println!("oag-listend unix on {bound:?}");
            Some((carrier, bound))
        }
        None => None,
    };
    let unix_shutdown = unix.as_ref().map(|(carrier, _)| carrier.shutdown_handle());

    if !args.no_generate {
        let generated = server
            .publish_generated(args.seed, oag_listend::server::STARTUP_SOUNDS)
            .await;
        for hash in &generated.accepted {
            println!("  generated {}", &hash[..16]);
        }
        for reason in &generated.rejected {
            eprintln!("  generated sound rejected: {reason}");
        }
        if generated.skipped_non_empty {
            println!("  store already holds audio; not generating a startup set");
        }
    }

    println!("oag-listend listening on {native_addr}");
    note_public_bind(native_addr);
    let held = store.lock().await.bytes_held();
    println!(
        "  seed {}  max-bytes {}  held {held}{}",
        args.seed,
        args.max_bytes,
        args.dir
            .as_deref()
            .map(|d| format!("  store {d}"))
            .unwrap_or_default()
    );
    println!("  only audio is accepted; everything else is rejected before it is stored");

    let native_run = tokio::spawn(async move { server.run().await });
    let http_run = http.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));
    let udp_run = udp.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));
    let unix_run = unix.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));

    // SIGINT (Ctrl+C) and SIGTERM (`systemctl stop`, `kill`) both shut down cleanly, so the
    // Unix socket is removed and the operator's stop command is not a crash.
    #[cfg(unix)]
    let sigterm = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm => {}
    }
    println!("oag-listend shutting down");
    if args.ipfs_api.is_some() {
        let log = store.lock().await.mirror_log().clone();
        let (mirrored, failed) = store.lock().await.mirror_summary();
        println!("  ipfs mirror: {mirrored} mirrored, {failed} failed");
        for outcome in log {
            match outcome {
                oag_listend::MirrorOutcome::Mirrored { content_hash, cid } => {
                    println!("    {} -> {}", &content_hash[..16], cid)
                }
                oag_listend::MirrorOutcome::Failed {
                    content_hash,
                    reason,
                } => println!("    {} failed: {}", &content_hash[..16], reason),
            }
        }
    }
    native_shutdown.shutdown();
    if let Some(handle) = http_shutdown {
        handle.shutdown();
    }
    if let Some(handle) = udp_shutdown {
        handle.shutdown();
    }
    if let Some(handle) = unix_shutdown {
        handle.shutdown();
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), native_run).await;
    if let Some(run) = http_run {
        let _ = tokio::time::timeout(Duration::from_secs(5), run).await;
    }
    if let Some(run) = udp_run {
        let _ = tokio::time::timeout(Duration::from_secs(5), run).await;
    }
    if let Some(run) = unix_run {
        let _ = tokio::time::timeout(Duration::from_secs(5), run).await;
    }
}
