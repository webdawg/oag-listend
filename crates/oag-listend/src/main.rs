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
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" => args.listen = it.next().expect("--listen needs an address"),
            "--http" => args.http = Some(it.next().expect("--http needs an address")),
            "--udp" => args.udp = Some(it.next().expect("--udp needs an address")),
            "--unix" => args.unix = Some(it.next().expect("--unix needs a socket path")),
            "--dir" => args.dir = Some(it.next().expect("--dir needs a directory")),
            "--check" => args.check = true,
            "--version" | "-V" => {
                println!(
                    "oag-listend {} (frame protocol {})",
                    env!("CARGO_PKG_VERSION"),
                    oag_listend::protocol::PROTOCOL_VERSION
                );
                std::process::exit(0);
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
         Usage: oag-listend [--listen ADDR] [--http ADDR] [--seed U64] [--max-bytes N] [--no-generate]\n\n\
           --listen ADDR    native protocol bind address (default 0.0.0.0:7100)\n\
           --http ADDR      also serve HTTP at ADDR (GET /audio/{{hash}}, POST /audio)
           --udp ADDR       also serve the native protocol over UDP datagrams
           --unix PATH      also serve the native protocol on a Unix domain socket
           --dir PATH       persist the audio content-addressed under PATH
           --check          open the store, print the load report, and exit
           --version        print the version and the frame protocol version\n\
           --seed U64       seed for the startup sounds (default: clock)\n\
           --max-bytes N    store bound in bytes (default {DEFAULT_MAX_BYTES})\n\
           --no-generate    do not publish generated sounds at startup\n\n\
         Only audio is accepted. Everything else is rejected before it is stored."
    )
}

/// Say plainly when a bind address is not loopback. The default is `0.0.0.0`, which serves every
/// interface; an operator who did not think about that should be told rather than discover it.
fn note_public_bind(addr: std::net::SocketAddr) {
    if !addr.ip().is_loopback() {
        println!(
            "  note: {addr} is not a loopback address -- this serves audio to that interface"
        );
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
    let store = match &args.dir {
        Some(dir) => {
            let (store, report) =
                AudioStore::with_directory(dir, args.max_bytes).unwrap_or_else(|e| {
                    eprintln!("oag-listend: cannot open store {dir}: {e}");
                    std::process::exit(1);
                });
            println!(
                "store {dir}: loaded {} skipped {} evicted {}",
                report.loaded, report.skipped, report.evicted
            );
            Arc::new(Mutex::new(store))
        }
        None => Arc::new(Mutex::new(AudioStore::with_capacity(args.max_bytes))),
    };

    if args.check {
        let held = store.lock().await.bytes_held();
        println!(
            "store ok: {} items, {} bytes (bound {}, item bound {})",
            store.lock().await.len(),
            held,
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
        let generated =
            server.publish_generated(args.seed, oag_listend::server::STARTUP_SOUNDS).await;
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
        args.dir.as_deref().map(|d| format!("  store {d}")).unwrap_or_default()
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
