//! The Unix domain socket carrier: the same audio surface over a filesystem socket.
//!
//! A local process reaches the daemon by path, not by port. That is a genuinely different way to
//! connect -- no port to allocate or collide, and the file's permissions decide who may connect --
//! and it is why this carrier exists rather than being a fourth TCP listener.
//!
//! The session loop is [`crate::server`]'s, unchanged: same frame protocol, same bounds, same
//! inbound rule. The only thing this file adds is the listener and the socket path.

#![forbid(unsafe_code)]

use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;
use tokio::sync::{watch, Mutex};

use crate::server::{serve_session, ShutdownHandle};
use crate::AudioStore;

pub const READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_CONNECTIONS: usize = 64;

pub struct UnixCarrier {
    store: Arc<Mutex<AudioStore>>,
    listener: UnixListener,
    path: PathBuf,
    /// Sessions in flight, for a bounded shutdown grace period.
    active: Arc<AtomicUsize>,
    shutdown_tx: watch::Sender<bool>,
}

impl UnixCarrier {
    /// Bind `path`. A stale socket file from a killed daemon is removed, because it would otherwise
    /// make startup fail forever -- but a socket that is still being served is refused rather than
    /// stolen: two daemons on one path would silently split clients between them.
    pub async fn bind_shared(
        path: impl Into<PathBuf>,
        store: Arc<Mutex<AudioStore>>,
    ) -> std::io::Result<(Self, PathBuf)> {
        let path = path.into();
        if let Ok(metadata) = std::fs::metadata(&path) {
            // Only a socket is ours to remove. A regular file or directory at the path is not a
            // stale socket, and deleting it would destroy something the operator put there.
            if !metadata.file_type().is_socket() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{path:?} exists and is not a socket"),
                ));
            }
            if tokio::net::UnixStream::connect(&path).await.is_ok() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("{path:?} is already being served"),
                ));
            }
            let _ = std::fs::remove_file(&path);
        }
        let listener = UnixListener::bind(&path)?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok((
            Self {
                store,
                listener,
                path: path.clone(),
                active: Arc::new(AtomicUsize::new(0)),
                shutdown_tx,
            },
            path,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sessions in flight right now.
    pub fn active_sessions(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// The live counter itself, so a caller can wait for sessions to drain.
    pub fn active_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active)
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle::new(self.shutdown_tx.clone())
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                accepted = self.listener.accept() => {
                    let (stream, _peer) = match accepted {
                        Ok(pair) => pair,
                        Err(_) => continue,
                    };
                    let Ok(permit) = semaphore.clone().acquire_owned().await else { break };
                    let active = Arc::clone(&self.active);
                    let store = Arc::clone(&self.store);
                    tokio::spawn(async move {
                        let _permit = permit;
                        active.fetch_add(1, Ordering::SeqCst);
                        // The same session loop as TCP: same protocol, bounds, and inbound rule.
                        let _ = serve_session(
                            stream,
                            store,
                            READ_TIMEOUT,
                            WRITE_TIMEOUT,
                            crate::protocol::MAX_FRAME_BYTES,
                        )
                        .await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            }
        }
        // Do not leave a dead socket file behind for the next start to trip over.
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }
}
