//! The operator's binary, end to end: the exact command an operator runs, and the exit codes for
//! bad input. `main.rs` is the entry point no library or carrier test reaches, so this file covers
//! it. Every wait is bounded; nothing sleeps and hopes on a fixed delay.

#![forbid(unsafe_code)]

use std::process::Stdio;
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
    assert!(
        String::from_utf8_lossy(&help.stdout).contains("--listen"),
        "--help must document --listen"
    );

    // A usage error is a typed exit 2 with a message, never a panic. Note that *omitting* --listen
    // is not one: the documented default is 0.0.0.0:7100, so that case starts the daemon instead of
    // refusing to run.
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
    let mut stdout = child.stdout.take().expect("stdout");

    // Read until the daemon says what it bound. A bind failure exits and prints the reason instead,
    // so this cannot hang past the deadline.
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

/// `--check` is the operator's way to verify a store without serving it. It must load the
/// content-addressed directory, report what it holds, and treat a file whose bytes no longer match
/// its name as skipped rather than served.
#[test]
fn check_loads_the_store_and_reports_what_it_holds() {
    let dir = std::env::temp_dir().join(format!(
        "oag-listend-check-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
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
