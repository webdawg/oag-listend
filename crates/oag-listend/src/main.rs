//! `oag-listend` -- run the daemon.
//!
//!     oag-listend --listen 0.0.0.0:7100 --http 0.0.0.0:7101 --seed 20261112
//!
//! It binds, publishes a small seeded set of generated sounds, and serves audio until interrupted.
//! Both carriers share one store, and every inbound publish still goes through
//! `AudioStore::accept`, so the daemon accepts audio and nothing else.

#![forbid(unsafe_code)]

use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use oag_listend::http::HttpCarrier;
use oag_listend::udp::UdpCarrier;
use oag_listend::unix::UnixCarrier;
use oag_listend::{AudioStore, ListendServer, DEFAULT_MAX_BYTES};
use tokio::sync::Mutex;

/// Write a line to stdout, ignoring a sink that cannot be written. A daemon must not die because
/// its log output has nowhere to go: under systemd stdout is the journal, and a journal that is
/// unavailable or a pipe whose reader exited would otherwise panic the process and stop the audio.
macro_rules! say {
    ($($arg:tt)*) => {{
        let _ = writeln!(std::io::stdout(), $($arg)*);
    }};
}

/// The same for stderr, which carries the operator's errors.
macro_rules! complain {
    ($($arg:tt)*) => {{
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

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
    /// Also serve over a Reticulum TCP interface (feature `reticulum`).
    #[cfg(feature = "reticulum")]
    reticulum: Option<String>,
    /// Identity seed for the Reticulum destination; stable across restarts.
    #[cfg(feature = "reticulum")]
    reticulum_seed: String,
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
        #[cfg(feature = "reticulum")]
        reticulum: None,
        #[cfg(feature = "reticulum")]
        reticulum_seed: oag_listend::reticulum::DEFAULT_IDENTITY_SEED.to_string(),
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
                say!(
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
            #[cfg(feature = "reticulum")]
            "--reticulum" => args.reticulum = Some(value(&mut it, "--reticulum")),
            #[cfg(feature = "reticulum")]
            "--reticulum-seed" => args.reticulum_seed = value(&mut it, "--reticulum-seed"),
            "--help" | "-h" => {
                say!("{}", usage());
                std::process::exit(0);
            }
            other => {
                complain!("unknown argument {other}\n{}", usage());
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
            complain!("{flag} needs a value\n\n{}", usage());
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
            complain!("{flag} must be a valid value: {raw}\n\n{}", usage());
            std::process::exit(2);
        }
    }
}

fn usage() -> String {
    // One literal, so the spacing an operator reads is the spacing written here.
    let mut text = format!(
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
    );
    text.push_str(reticulum_usage_lines());
    text
}

/// The usage lines for the Reticulum flags, which exist only in a build carrying that feature.
fn reticulum_usage_lines() -> &'static str {
    #[cfg(feature = "reticulum")]
    {
        "\n  --reticulum ADDR    also serve over a Reticulum TCP interface (feature 'reticulum')\n\
         --reticulum-seed SEED  identity seed for that destination (default oag-listend)\n"
    }
    #[cfg(not(feature = "reticulum"))]
    {
        ""
    }
}

/// Whether this process can create a file in `dir`.
///
/// Deliberately a probe rather than a reading of the mode bits: a read-only mount and an ACL both
/// look writable, and the failure they cause otherwise surfaces at the first publish rather than
/// at boot.
fn store_is_writable(dir: &std::path::Path) -> bool {
    path_is_writable(&dir.join(".oag-listend-write-probe"))
}

/// Whether a file can be created at `path`. Split out so a test can point it somewhere that is
/// unwritable for any user.
fn path_is_writable(path: &std::path::Path) -> bool {
    match std::fs::write(path, b"") {
        Ok(()) => {
            let _ = std::fs::remove_file(path);
            true
        }
        Err(_) => false,
    }
}

/// Say plainly when a bind address is not loopback. The default is `0.0.0.0`, which serves every
/// interface; an operator who did not think about that should be told rather than discover it.
fn note_public_bind(addr: std::net::SocketAddr) {
    if !addr.ip().is_loopback() {
        say!("  note: {addr} is not a loopback address -- this serves audio to that interface");
    }
}

/// A seed from the clock and the process id, so two daemons on one machine do not sound alike.
/// How long a stop waits for in-flight exchanges to finish before exiting anyway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

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

    // Register the stop signals before anything an operator can observe. The handler used to be
    // installed after the "listening on" line, which left a window in which a Ctrl+C or an early
    // `systemctl stop` was answered by the default terminate action instead of a clean stop --
    // timing-dependent, and only visible under load.
    let sigterm = {
        #[cfg(unix)]
        {
            async {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(mut signal) => {
                        signal.recv().await;
                    }
                    Err(_) => std::future::pending().await,
                }
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>()
        }
    };

    // Same for SIGINT. `tokio::signal::ctrl_c()` must be polled to register, so an early Ctrl+C
    // under load could still hit the default terminate action; `unix::signal` registers eagerly.
    let sigint = {
        #[cfg(unix)]
        {
            async {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
                    Ok(mut signal) => {
                        signal.recv().await;
                    }
                    Err(_) => std::future::pending().await,
                }
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>()
        }
    };
    let mut store = match &args.dir {
        Some(dir) => {
            // `--check` opens the store without changing it. A verification that unlinks an
            // operator's audio because it exceeds `--max-bytes` is not a verification.
            let opened = if args.check {
                AudioStore::check_directory(dir, args.max_bytes)
            } else {
                AudioStore::with_directory(dir, args.max_bytes)
            };
            let (store, report) = opened.unwrap_or_else(|e| {
                // Name the requirement, not just the errno: "Permission denied" alone reads like a
                // broken binary rather than a directory this process may not write.
                complain!(
                    "oag-listend: cannot open store {dir}: {e}; the daemon must be able to \
                     create and write --dir"
                );
                std::process::exit(1);
            });
            let mut notes = Vec::new();
            if report.truncated {
                notes.push("truncated (more entries than the load considers)");
            }
            if report.timed_out {
                notes.push("timed out (the load deadline elapsed with entries unread)");
            }
            say!(
                "store {dir}: loaded {} skipped {} evicted {}{}",
                report.loaded,
                report.skipped,
                report.evicted,
                if notes.is_empty() {
                    String::new()
                } else {
                    format!(" -- {}", notes.join(", "))
                }
            );
            store
        }
        None => AudioStore::with_capacity(args.max_bytes),
    };
    // A store that cannot be written is not fatal -- it can still serve what it holds -- but an
    // operator who boots one should hear it now rather than at the first client. The mode bits do
    // not tell you: a read-only mount and an ACL both look writable. Probing costs one empty file,
    // which is removed again.
    if let Some(dir) = &args.dir {
        if !args.check && !store_is_writable(std::path::Path::new(dir)) {
            say!("  note: store {dir} is not writable; every publish will be refused");
        }
    }

    if args.check {
        // A check is purely a store verification: it must not contact an IPFS node or spend its
        // mirror budget, because operators and scripts call it expecting a quick answer.
        say!(
            "store ok: {} items, {} bytes (bound {}, item bound {})",
            store.len(),
            store.bytes_held(),
            args.max_bytes,
            oag_listend::servable_bound(args.max_bytes)
        );
        return;
    }

    if let Some(addr) = &args.ipfs_api {
        store.set_ipfs(Some(oag_listend::ipfs::IpfsPublisher::new(addr.clone())));
        say!("offering accepted audio to IPFS at {addr} (best-effort)");
    }
    let store = Arc::new(Mutex::new(store));

    let (server, native_addr) = ListendServer::bind_shared(&args.listen, Arc::clone(&store))
        .await
        .unwrap_or_else(|e| {
            complain!("oag-listend: cannot bind {}: {e}", args.listen);
            std::process::exit(1);
        });
    let native_shutdown = server.shutdown_handle();
    let mut active_counters = vec![server.active_handle()];

    let http = match &args.http {
        Some(addr) => {
            let (carrier, http_addr) = HttpCarrier::bind_shared(addr, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    complain!("oag-listend: cannot bind http {addr}: {e}");
                    std::process::exit(1);
                });
            say!("oag-listend http on {http_addr}");
            note_public_bind(http_addr);
            Some((carrier, http_addr))
        }
        None => None,
    };
    let http_shutdown = http.as_ref().map(|(carrier, _)| carrier.shutdown_handle());
    if let Some((carrier, _)) = &http {
        active_counters.push(carrier.active_handle());
    }

    let udp = match &args.udp {
        Some(addr) => {
            let (carrier, udp_addr) = UdpCarrier::bind_shared(addr, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    complain!("oag-listend: cannot bind udp {addr}: {e}");
                    std::process::exit(1);
                });
            say!("oag-listend udp on {udp_addr}");
            note_public_bind(udp_addr);
            Some((carrier, udp_addr))
        }
        None => None,
    };
    let udp_shutdown = udp.as_ref().map(|(carrier, _)| carrier.shutdown_handle());
    if let Some((carrier, _)) = &udp {
        active_counters.push(carrier.active_handle());
    }

    let unix = match &args.unix {
        Some(path) => {
            let (carrier, bound) = UnixCarrier::bind_shared(path, Arc::clone(&store))
                .await
                .unwrap_or_else(|e| {
                    complain!("oag-listend: cannot bind unix {path}: {e}");
                    std::process::exit(1);
                });
            say!("oag-listend unix on {bound:?}");
            Some((carrier, bound))
        }
        None => None,
    };
    let unix_shutdown = unix.as_ref().map(|(carrier, _)| carrier.shutdown_handle());
    #[cfg(feature = "reticulum")]
    let reticulum = match &args.reticulum {
        Some(addr) => {
            let carrier = oag_listend::reticulum::ReticulumCarrier::bind(
                addr.clone(),
                Arc::clone(&store),
                &args.reticulum_seed,
            )
            .await
            .unwrap_or_else(|e| {
                complain!("oag-listend: cannot bind reticulum {addr}: {e}");
                std::process::exit(1);
            });
            say!(
                "oag-listend reticulum on {} at {}",
                carrier.local_addr(),
                carrier.address()
            );
            Some(carrier)
        }
        None => None,
    };
    #[cfg(feature = "reticulum")]
    let reticulum_shutdown = reticulum.as_ref().map(|carrier| carrier.shutdown_handle());
    #[cfg(feature = "reticulum")]
    if let Some(carrier) = &reticulum {
        active_counters.push(carrier.active_handle());
    }
    if let Some((carrier, _)) = &unix {
        active_counters.push(carrier.active_handle());
    }

    if !args.no_generate {
        let generated = server
            .publish_generated(args.seed, oag_listend::server::STARTUP_SOUNDS)
            .await;
        for hash in &generated.accepted {
            say!("  generated {}", &hash[..16]);
        }
        for reason in &generated.rejected {
            complain!("  generated sound rejected: {reason}");
        }
        if generated.skipped_non_empty {
            say!("  store already holds audio; not generating a startup set");
        }
    }

    say!("oag-listend listening on {native_addr}");
    note_public_bind(native_addr);
    let held = store.lock().await.bytes_held();
    say!(
        "  seed {}  max-bytes {}  held {held}{}",
        args.seed,
        args.max_bytes,
        args.dir
            .as_deref()
            .map(|d| format!("  store {d}"))
            .unwrap_or_default()
    );
    say!("  only audio is accepted; everything else is rejected before it is stored");

    let native_run = tokio::spawn(async move { server.run().await });
    let http_run = http.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));
    let udp_run = udp.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));
    let unix_run = unix.map(|(carrier, _)| tokio::spawn(async move { carrier.run().await }));
    #[cfg(feature = "reticulum")]
    let reticulum_run = reticulum.map(|carrier| tokio::spawn(async move { carrier.run().await }));

    // The startup mirror pass runs *after* the carriers are serving. Audio already on disk would
    // otherwise never reach IPFS -- only publishes made after this daemon started would be
    // mirrored -- but running it before the bind meant a slow or stalled node delayed the daemon
    // from serving, which contradicts the mirror being best-effort.
    if args.ipfs_api.is_some() {
        let held = store.lock().await.len();
        if held > 0 {
            let offered =
                oag_listend::mirror_existing_shared(&store, oag_listend::STARTUP_MIRROR_BUDGET)
                    .await;
            say!("  offered {offered} of {held} stored item(s) to IPFS");
        }
    }

    // SIGINT (Ctrl+C) and SIGTERM (`systemctl stop`, `kill`) both shut down cleanly, so the
    // Unix socket is removed and the operator's stop command is not a crash.
    tokio::select! {
        _ = sigint => {}
        _ = sigterm => {}
    }
    say!("oag-listend shutting down");
    if args.ipfs_api.is_some() {
        let log = store.lock().await.mirror_log().clone();
        let (mirrored, failed) = store.lock().await.mirror_summary();
        say!("  ipfs mirror: {mirrored} mirrored, {failed} failed");
        for outcome in log {
            match outcome {
                oag_listend::MirrorOutcome::Mirrored { content_hash, cid } => {
                    say!("    {} -> {}", &content_hash[..16], cid)
                }
                oag_listend::MirrorOutcome::Failed {
                    content_hash,
                    reason,
                } => say!("    {} failed: {}", &content_hash[..16], reason),
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
    #[cfg(feature = "reticulum")]
    if let Some(handle) = reticulum_shutdown {
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
    #[cfg(feature = "reticulum")]
    if let Some(run) = reticulum_run {
        let _ = tokio::time::timeout(Duration::from_secs(5), run).await;
    }

    // A stop should not cut a client off mid-transfer. Stop accepting immediately -- done
    // above -- then give in-flight exchanges up to SHUTDOWN_GRACE to finish, and no longer:
    // a client that never reads must not be able to hold a stop open indefinitely.
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    while active_counters
        .iter()
        .any(|counter| counter.load(Ordering::SeqCst) > 0)
    {
        if Instant::now() >= deadline {
            say!(
                "  shutdown grace of {:?} elapsed with session(s) still active",
                SHUTDOWN_GRACE
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writability is probed, not inferred from the mode bits. The negative case uses a path
    /// whose parent is a regular file, which no user can write through -- a mode-bits test would
    /// be silently bypassed by root, and CI does not always run as one user.
    #[test]
    fn writability_is_probed_not_inferred_from_mode_bits() {
        let tmp = std::env::temp_dir();
        assert!(store_is_writable(&tmp), "a temp dir is writable");

        let blocker = tmp.join(format!("oag-listend-not-a-dir-{}", std::process::id()));
        std::fs::write(&blocker, b"x").expect("write the blocker file");
        let result = path_is_writable(&blocker.join("probe"));
        let _ = std::fs::remove_file(&blocker);
        assert!(
            !result,
            "a path under a regular file is not writable by anyone"
        );
    }
}
