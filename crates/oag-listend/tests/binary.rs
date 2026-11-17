//! The operator's binary, end to end: the exact command an operator runs, and the exit codes for
//! bad input. `main.rs` is the entry point no library or carrier test reaches, so this file covers
//! it. Every wait is bounded; nothing sleeps and hopes on a fixed delay.

#![forbid(unsafe_code)]

use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use oag_listend::protocol::{self, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

const BOUND: Duration = Duration::from_secs(20);

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_oag-listend")
}

/// Reserve a port by binding and dropping it. There is a small race between this and the daemon's
/// own bind, which is why the serving test fails on the daemon's error output rather than hanging.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    listener.local_addr().expect("reserved port").port()
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "oag-listend-binary-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Read the daemon's stdout until it announces its bound address, and give the handle back so a
/// caller can read whatever follows.
///
/// A bind failure exits and prints the reason instead of announcing, so polling for exit turns
/// that into a panic rather than a hang past the deadline.
async fn announce(child: &mut tokio::process::Child) -> (Vec<u8>, tokio::process::ChildStdout) {
    let mut stdout = child.stdout.take().expect("stdout");
    let mut announced = Vec::new();
    let mut chunk = [0u8; 1024];
    let deadline = Instant::now() + BOUND;
    loop {
        assert!(
            Instant::now() < deadline,
            "the daemon never announced its address; output was {}",
            String::from_utf8_lossy(&announced)
        );
        if let Some(status) = child.try_wait().expect("poll the daemon") {
            panic!(
                "the daemon exited before announcing: {status}; output was {}",
                String::from_utf8_lossy(&announced)
            );
        }
        let n = tokio::time::timeout(Duration::from_millis(200), stdout.read(&mut chunk))
            .await
            .expect("a read within the bound")
            .expect("read stdout");
        if n == 0 {
            break;
        }
        announced.extend_from_slice(&chunk[..n]);
        if String::from_utf8_lossy(&announced).contains("listening on") {
            break;
        }
    }
    (announced, stdout)
}

#[test]
fn version_and_help_work_and_usage_errors_exit_two() {
    let version = std::process::Command::new(binary())
        .arg("--version")
        .output()
        .expect("run --version");
    assert!(version.status.success(), "--version must succeed");
    let stdout = String::from_utf8_lossy(&version.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "--version must print the package version: {stdout}"
    );
    assert!(
        stdout.contains(&protocol::PROTOCOL_VERSION.to_string()),
        "--version must print the frame protocol version: {stdout}"
    );

    let help = std::process::Command::new(binary())
        .arg("--help")
        .output()
        .expect("run --help");
    assert!(help.status.success(), "--help must succeed");
    let help = String::from_utf8_lossy(&help.stdout).into_owned();
    // Every flag the daemon accepts must appear in its own help, not just the first one.
    for flag in [
        "--listen",
        "--http",
        "--udp",
        "--unix",
        "--dir",
        "--check",
        "--seed",
        "--max-bytes",
        "--no-generate",
        "--version",
        "--help",
    ] {
        assert!(help.contains(flag), "--help must document {flag}:\n{help}");
    }
    // And each flag's description must sit on its own line: a missing newline escapes the
    // following line into it, which is exactly how --http once swallowed --udp.
    for flag in [
        "--listen ADDR",
        "--http ADDR",
        "--udp ADDR",
        "--unix PATH",
        "--dir PATH",
        "--check",
        "--seed U64",
        "--max-bytes N",
        "--no-generate",
        "--ipfs-api ADDR",
    ] {
        assert!(
            help.lines().any(|line| line.trim_start().starts_with(flag)),
            "--help must put {flag} on its own line:\n{help}"
        );
    }

    // A usage error is a typed exit 2 with a message, never a panic.
    //
    // Note that *omitting* --listen is not one: the documented default is 0.0.0.0:7100, so that case
    // starts the daemon instead of refusing to run.
    for args in [
        vec!["--seed", "not-a-number"],
        vec!["--max-bytes", "nope"],
        vec!["--unknown-flag"],
    ] {
        let output = std::process::Command::new(binary())
            .args(&args)
            .output()
            .expect("run with bad arguments");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?} must exit 2, got {:?}",
            output.status.code()
        );
        assert!(
            !output.stderr.is_empty(),
            "{args:?} must explain itself on stderr"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("panicked"),
            "a usage error must not panic: {stderr}"
        );
    }
}

/// The operator client is a shipped artefact too, and its `--help` has drifted before. Locate the
/// built example relative to this test binary (`target/debug/deps/.. /examples/push_pull`).
fn push_pull() -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("the test binary's path");
    path.pop(); // deps/
    path.pop(); // debug/
    path.push("examples");
    path.push("push_pull");
    assert!(
        path.exists(),
        "the push_pull example must be built to test it: {}",
        path.display()
    );
    path
}

#[test]
fn the_client_help_documents_every_option_and_refuses_unknown_flags() {
    let help = std::process::Command::new(push_pull())
        .arg("--help")
        .output()
        .expect("run push_pull --help");
    assert!(help.status.success());
    let help = String::from_utf8_lossy(&help.stdout);
    for flag in ["--addr", "--udp", "--unix", "--seed", "--version", "--help"] {
        assert!(
            help.contains(flag),
            "push_pull --help must document {flag}:\n{help}"
        );
    }

    let version = std::process::Command::new(push_pull())
        .arg("--version")
        .output()
        .expect("run push_pull --version");
    assert!(version.status.success());

    // A usage error is exit 2, never a panic.
    let bad = std::process::Command::new(push_pull())
        .arg("--nonsense")
        .output()
        .expect("run push_pull --nonsense");
    assert_eq!(bad.status.code(), Some(2));
    assert!(!bad.stderr.is_empty());
}

/// A check must stay a store verification. Passing `--ipfs-api` must not make it contact a node or
/// spend its mirror budget: operators and scripts call `--check` expecting a quick answer.
#[test]
fn check_does_not_contact_the_ipfs_node() {
    let dir = temp_dir("check-ipfs");
    std::fs::create_dir_all(&dir).expect("make the store directory");

    // A port with nothing listening: if the daemon tried to reach it, the check would be slow and
    // the output would say so.
    let output = std::process::Command::new(binary())
        .arg("--check")
        .arg("--dir")
        .arg(&dir)
        .arg("--ipfs-api")
        .arg("127.0.0.1:1")
        .output()
        .expect("run --check --ipfs-api");
    assert!(
        output.status.success(),
        "--check must succeed without a reachable IPFS API: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("store ok"),
        "still reports the store: {stdout}"
    );
    assert!(
        !stdout.contains("offering accepted audio"),
        "a check must not announce a mirror: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `--check` is the operator's way to verify a store without serving it. It must load the
/// content-addressed directory, report what it holds, and treat a file whose bytes no longer match
/// its name as skipped rather than served.
#[test]
fn check_loads_the_store_and_reports_what_it_holds() {
    let dir = temp_dir("check");
    std::fs::create_dir_all(&dir).expect("make the store directory");

    // One genuine item, written under its own hash with its media-type sidecar.
    let tone = oag_listend::wav::generate(5, oag_listend::wav::Sound::Tone);
    let item = oag_listend::AudioItem::new(tone.clone(), Some("audio/wav".to_string()));
    std::fs::write(dir.join(&item.content_hash), &tone).expect("write audio");
    std::fs::write(
        dir.join(format!("{}.json", item.content_hash)),
        b"audio/wav",
    )
    .expect("write sidecar");

    let run_check = || {
        let output = std::process::Command::new(binary())
            .arg("--check")
            .arg("--dir")
            .arg(&dir)
            .arg("--max-bytes")
            .arg("1048576")
            .output()
            .expect("run --check");
        assert!(
            output.status.success(),
            "--check must succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    let stdout = run_check();
    assert!(stdout.contains("store ok"), "reports success: {stdout}");
    assert!(
        stdout.contains("loaded 1"),
        "counts the real item: {stdout}"
    );
    assert!(
        stdout.contains("skipped 0"),
        "nothing was skipped: {stdout}"
    );
    assert!(
        stdout.contains("evicted 0"),
        "nothing was evicted: {stdout}"
    );
    assert!(
        stdout.contains("items, ") && stdout.contains("bytes"),
        "reports the store's size and bound: {stdout}"
    );

    // Corrupt the file out of band. Its name no longer matches its bytes, so it must be skipped,
    // never served.
    std::fs::write(dir.join(&item.content_hash), b"this is not audio").expect("corrupt");
    let stdout = run_check();
    assert!(
        stdout.contains("loaded 0"),
        "a bad file is not loaded: {stdout}"
    );
    assert!(
        stdout.contains("skipped 1"),
        "and is reported as skipped: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Turn the real binary on, exchange audio with it over TCP, then stop it. The daemon announces its
/// bound address on stdout, and the test waits for that line instead of sleeping.
#[tokio::test]
async fn the_binary_serves_audio_until_it_is_stopped() {
    let outcome = tokio::time::timeout(BOUND, serve()).await;
    outcome.expect("the binary exchange must complete within the bound")
}

async fn serve() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(binary())
        .arg("--listen")
        .arg(&addr)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the daemon");
    let (_announced, _stdout) = announce(&mut child).await;

    let mut stream = TcpStream::connect(&addr)
        .await
        .expect("connect to the daemon");
    let hello = protocol::encode_frame(&protocol::encode_request(&Request::Hello));
    stream.write_all(&hello).await.expect("write hello");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    match protocol::decode_response(&body).expect("decode") {
        Response::Hello {
            protocol_version, ..
        } => assert_eq!(protocol_version, protocol::PROTOCOL_VERSION),
        other => panic!("expected Hello, got {other:?}"),
    }

    // The daemon publishes generated sounds at startup, so a fetch-only client receives audio.
    let tone = oag_listend::wav::generate(2026, oag_listend::wav::Sound::Tone);
    let publish = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: tone.clone(),
    }));
    stream.write_all(&publish).await.expect("write publish");
    stream.flush().await.expect("flush");
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    let hash = match protocol::decode_response(&body).expect("decode") {
        Response::Published { content_hash } => content_hash,
        other => panic!("expected Published, got {other:?}"),
    };
    assert!(oag_listend::AudioItem::verify(&hash, &tone));

    child.kill().await.expect("stop the daemon");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("the daemon must exit once killed")
        .expect("wait");
    assert!(
        status.success() || status.code().is_none(),
        "a killed daemon reports the signal, not a clean exit: {status}"
    );
}

/// SIGTERM is what `systemctl stop` sends, so it must be a clean stop: the process exits 0, says it
/// is shutting down, and removes the Unix socket rather than leaving a dead file for the next start.
#[tokio::test]
async fn sigterm_is_a_clean_stop() {
    let outcome = tokio::time::timeout(BOUND, clean_stop()).await;
    outcome.expect("the shutdown exchange must complete within the bound")
}

async fn clean_stop() {
    let dir = temp_dir("sigterm");
    std::fs::create_dir_all(&dir).expect("make the runtime directory");
    let sock = dir.join("audio.sock");

    let mut child = Command::new(binary())
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--unix")
        .arg(&sock)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the daemon");
    let (_announced, mut stdout) = announce(&mut child).await;
    assert!(sock.exists(), "the daemon must create its Unix socket");

    let pid = child.id().expect("the daemon's pid");
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("send SIGTERM");

    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("the daemon must exit after SIGTERM")
        .expect("wait");
    assert_eq!(
        status.code(),
        Some(0),
        "SIGTERM must be a clean exit 0, got {status}"
    );
    assert!(
        !sock.exists(),
        "a clean stop must remove the Unix socket for the next start"
    );

    let mut said = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stdout.read_to_end(&mut said))
        .await
        .expect("reading the rest of stdout after exit must terminate")
        .expect("read stdout");
    let said = String::from_utf8_lossy(&said);
    assert!(
        said.contains("shutting down"),
        "the daemon must say it is stopping: {said}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `--ipfs-api` makes the daemon offer accepted audio to a Kubo node, and report the outcome at
/// shutdown. The stub mimics `/api/v0/add`; this is deliberately not a real-IPFS test.
#[tokio::test]
async fn ipfs_api_mirrors_accepted_audio_and_reports_it() {
    let outcome = tokio::time::timeout(BOUND, ipfs_wiring()).await;
    outcome.expect("the ipfs wiring exchange must complete within the bound")
}

async fn ipfs_wiring() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stub");
    let api_port = listener.local_addr().expect("stub addr").port();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&requests);
    let server = tokio::spawn(async move {
        let Ok(accepted) = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        else {
            return;
        };
        let (mut stream, _) = accepted.expect("accept");
        let mut buf = vec![0u8; 64 * 1024];
        let mut got = Vec::new();
        loop {
            let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await
            {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => n,
                _ => break,
            };
            got.extend_from_slice(&buf[..n]);
            if got.ends_with(b"--\r\n") {
                break;
            }
        }
        counted.fetch_add(1, Ordering::SeqCst);
        let reply = "HTTP/1.1 200 OK\r\nContent-Length: 44\r\n\r\n\
                     {\"Hash\":\"bafywired\",\"Name\":\"audio\"}\r\n";
        let _ = stream.write_all(reply.as_bytes()).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    });

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(binary())
        .arg("--listen")
        .arg(&addr)
        .arg("--no-generate")
        .arg("--ipfs-api")
        .arg(format!("127.0.0.1:{api_port}"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the daemon");
    // Kept: `read_to_end` below resumes where this handle was left, so the startup lines are
    // needed separately to assert on them.
    let (announced, mut stdout) = announce(&mut child).await;

    // An operator's publish.
    let mut stream = TcpStream::connect(&addr).await.expect("connect");
    let tone = oag_listend::wav::generate(3, oag_listend::wav::Sound::Tone);
    let publish = protocol::encode_frame(&protocol::encode_request(&Request::Publish {
        declared_media_type: Some("audio/wav".into()),
        bytes: tone,
    }));
    stream.write_all(&publish).await.expect("write publish");
    stream.flush().await.expect("flush");
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("prefix");
    let len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("body");
    assert!(matches!(
        protocol::decode_response(&body).expect("decode"),
        Response::Published { .. }
    ));
    drop(stream);

    let pid = child.id().expect("the daemon's pid");
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("send SIGTERM");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("exit bound")
        .expect("wait");
    assert_eq!(status.code(), Some(0));

    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stdout.read_to_end(&mut out))
        .await
        .expect("read bound")
        .expect("read stdout");
    let mut text = String::from_utf8_lossy(&announced).into_owned();
    text.push_str(&String::from_utf8_lossy(&out));
    assert!(
        text.contains("offering accepted audio to IPFS"),
        "startup should say what it is mirroring: {text}"
    );
    assert!(
        text.contains("1 mirrored, 0 failed"),
        "the mirror outcome must be reported: {text}"
    );

    server.await.expect("join the stub");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "the accepted audio must have been offered exactly once"
    );
}
