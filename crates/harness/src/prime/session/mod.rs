//! Exclusive native Prime session ownership and normalized control receipts.

mod contract;
mod host;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::json;

use self::contract::ActiveSessionId;
use self::host::SessionHost;
use super::contract::{BridgeResponse, OwnedSessionCleanupStatus};
use super::{
    BootstrapBridge, PrimeDaemon, PrimeDaemonError, checked_deadline, duration_millis, remaining,
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CREATE_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_ATTACH_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);
// The fork's default owner-disconnect grace is 30 seconds. The default poll
// deadline must extend beyond it so a crashed host can still prove settlement.
const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(45);
const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CLEANUP_REQUEST_SLICE: Duration = Duration::from_secs(2);
const MIN_REAP_SLICE: Duration = Duration::from_millis(1);
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);

const REQUIRED_SESSION_CAPABILITIES: &[&str] = &[
    "client_owned_sessions",
    "chunked_snapshot",
    "immutable_snapshot_transfer_v1",
    "authoritative_owned_session_cleanup_v1",
];

/// Host-local configuration for one exclusively owned native Prime session.
///
/// The canonical working directory remains private to the harness process. It
/// is never projected into a synchronized Comet document or normalized event.
#[derive(Clone)]
pub struct PrimeSessionConfig {
    cwd: PathBuf,
    deadlines: SessionDeadlines,
}

#[derive(Clone, Copy)]
struct SessionDeadlines {
    connect: Duration,
    create: Duration,
    attach: Duration,
    snapshot: Duration,
    close: Duration,
    cleanup: Duration,
}

impl PrimeSessionConfig {
    /// Validate and retain an existing canonical working directory.
    pub fn new(cwd: impl AsRef<Path>) -> Result<Self, PrimeDaemonError> {
        let cwd = std::fs::canonicalize(cwd.as_ref())
            .map_err(|error| PrimeDaemonError::io("session working directory", &error))?;
        if !cwd.is_dir() {
            return Err(PrimeDaemonError::Session {
                stage: "session configuration",
                code: "working-directory-is-not-a-directory",
            });
        }
        Ok(Self {
            cwd,
            deadlines: SessionDeadlines {
                connect: DEFAULT_CONNECT_TIMEOUT,
                create: DEFAULT_CREATE_TIMEOUT,
                attach: DEFAULT_ATTACH_TIMEOUT,
                snapshot: DEFAULT_SNAPSHOT_TIMEOUT,
                close: DEFAULT_CLOSE_TIMEOUT,
                cleanup: DEFAULT_CLEANUP_TIMEOUT,
            },
        })
    }

    /// Override the independent connect, create, attach, snapshot, close, and
    /// authoritative-cleanup deadlines, in that order.
    pub fn with_timeouts(
        mut self,
        connect: Duration,
        create: Duration,
        attach: Duration,
        snapshot: Duration,
        close: Duration,
        cleanup: Duration,
    ) -> Self {
        self.deadlines = SessionDeadlines {
            connect,
            create,
            attach,
            snapshot,
            close,
            cleanup,
        };
        self
    }

    fn validate(&self) -> Result<(), PrimeDaemonError> {
        let current = std::fs::canonicalize(&self.cwd)
            .map_err(|error| PrimeDaemonError::io("session working directory", &error))?;
        if current != self.cwd || !current.is_dir() {
            return Err(PrimeDaemonError::Session {
                stage: "session configuration",
                code: "working-directory-changed",
            });
        }
        for (stage, timeout) in [
            ("session connect", self.deadlines.connect),
            ("session creation", self.deadlines.create),
            ("session attachment", self.deadlines.attach),
            ("session snapshot", self.deadlines.snapshot),
            ("session close", self.deadlines.close),
            ("owned session cleanup", self.deadlines.cleanup),
        ] {
            if timeout.is_zero()
                || timeout > MAX_OPERATION_TIMEOUT
                || Instant::now().checked_add(timeout).is_none()
            {
                return Err(PrimeDaemonError::Session {
                    stage,
                    code: "invalid-deadline",
                });
            }
        }
        Ok(())
    }
}

/// Safe receipt for an immutable snapshot transfer. It contains no messages,
/// prompts, paths, cursors, native identifiers, or raw provider data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimeSessionSnapshotReceipt {
    message_count: u64,
    is_streaming: bool,
    has_cursor: bool,
}

impl PrimeSessionSnapshotReceipt {
    pub(super) const fn new(message_count: u64, is_streaming: bool, has_cursor: bool) -> Self {
        Self {
            message_count,
            is_streaming,
            has_cursor,
        }
    }

    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    pub fn is_streaming(&self) -> bool {
        self.is_streaming
    }

    pub fn has_cursor(&self) -> bool {
        self.has_cursor
    }
}

/// Fixed normalized session-host event. Native event payloads and identifiers
/// never cross this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimeSessionEvent {
    Closed,
}

/// Exclusive owner of one client-owned native Prime session.
///
/// This type deliberately implements neither `Clone`, `Debug`, nor
/// serialization. The private daemon, host group, and active native identity
/// move together and are consumed by [`PrimeSessionLease::close`].
pub struct PrimeSessionLease {
    daemon: Option<PrimeDaemon>,
    host: Option<SessionHost>,
    cleanup_obligation: CleanupObligation,
    initial_snapshot: Option<PrimeSessionSnapshotReceipt>,
    deadlines: SessionDeadlines,
}

/// Tracks the create commit window without ever formatting or exposing its
/// eventual private native identity.
enum CleanupObligation {
    None,
    UnknownCreateCommit,
    Known(ActiveSessionId),
}

impl PrimeDaemon {
    /// Create, own, attach, and snapshot exactly one native session.
    ///
    /// Daemon ownership is consumed before any session operation. The public
    /// SDK feature gate is checked before client construction; required daemon
    /// offers are then checked before connect or native session creation.
    pub async fn create_session(
        self,
        config: PrimeSessionConfig,
    ) -> Result<PrimeSessionLease, PrimeDaemonError> {
        config.validate()?;

        let host =
            match SessionHost::spawn(&self.package, &self.paths, &self.session_host_environment) {
                Ok(host) => host,
                Err(error) => {
                    let mut daemon = self;
                    shutdown_after_cleanup(&mut daemon).await;
                    return Err(error);
                }
            };

        // Install the cancellation-safe owner before the first asynchronous
        // host stage. From here on, cancellation always transfers daemon, host,
        // and cleanup obligation to a bounded reaper.
        let mut opening = PrimeSessionLease {
            daemon: Some(self),
            host: Some(host),
            cleanup_obligation: CleanupObligation::None,
            initial_snapshot: None,
            deadlines: config.deadlines,
        };

        if let Err(error) = opening
            .host
            .as_ref()
            .expect("opening owner has a host")
            .load(
                &opening
                    .daemon
                    .as_ref()
                    .expect("opening owner has a daemon")
                    .package
                    .public_entry,
                &opening
                    .daemon
                    .as_ref()
                    .expect("opening owner has a daemon")
                    .package
                    .version,
                config.deadlines.connect,
            )
            .await
        {
            return Err(opening.abort_uncommitted(error).await);
        }

        // The frozen public-root feature token is checked by `load` before a
        // DaemonClient is constructed. Only then intersect the daemon offers,
        // still before connect or any native create request.
        let missing = REQUIRED_SESSION_CAPABILITIES
            .iter()
            .copied()
            .filter(|capability| {
                !opening
                    .daemon
                    .as_ref()
                    .expect("opening owner has a daemon")
                    .server_capabilities
                    .server_offers(capability)
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let error = PrimeDaemonError::IncompatibleHello {
                reason: format!(
                    "missing required session capabilities: {}",
                    missing.join(", ")
                ),
            };
            return Err(opening.abort_uncommitted(error).await);
        }

        if let Err(error) = opening
            .host
            .as_ref()
            .expect("opening owner has a host")
            .connect(
                &opening
                    .daemon
                    .as_ref()
                    .expect("opening owner has a daemon")
                    .paths
                    .socket,
                config.deadlines.connect,
            )
            .await
        {
            return Err(opening.abort_uncommitted(error).await);
        }

        // Mark the commit window before the create future can first yield. If
        // it is cancelled or returns without a validated private ID, exact
        // reconciliation is impossible and the long poison shutdown is used.
        opening.cleanup_obligation = CleanupObligation::UnknownCreateCommit;
        let create = opening
            .host
            .as_ref()
            .expect("opening owner has a host")
            .create(
                &config.cwd,
                &opening
                    .daemon
                    .as_ref()
                    .expect("opening owner has a daemon")
                    .paths
                    .session_dir,
                config.deadlines.create,
            )
            .await;
        let active_session = match create {
            Ok(active_session) => active_session,
            Err(_) => return Err(opening.abort_unknown_create().await),
        };
        // No await occurs between receiving the private active identity and
        // moving it into the cancellation-safe cleanup obligation.
        opening.cleanup_obligation = CleanupObligation::Known(active_session);

        let attach = opening
            .host
            .as_ref()
            .expect("opening owner has a host")
            .attach(config.deadlines.attach, config.deadlines.snapshot)
            .await;
        if let Err(error) = attach {
            return Err(opening.abort_opening(error).await);
        }
        let snapshot = opening
            .host
            .as_ref()
            .expect("opening owner has a host")
            .snapshot(config.deadlines.snapshot)
            .await;
        match snapshot {
            Ok(receipt) => {
                opening.initial_snapshot = Some(receipt);
                Ok(opening)
            }
            Err(error) => Err(opening.abort_opening(error).await),
        }
    }
}

impl PrimeSessionLease {
    /// The validated receipt obtained during the opening snapshot stage.
    pub fn snapshot_receipt(&self) -> PrimeSessionSnapshotReceipt {
        self.initial_snapshot
            .expect("a public session lease always has an opening snapshot")
    }

    /// Request a new immutable snapshot receipt. Raw snapshot data remains in
    /// the host and native daemon.
    pub async fn snapshot(&self) -> Result<PrimeSessionSnapshotReceipt, PrimeDaemonError> {
        self.host
            .as_ref()
            .expect("a live session lease has a host")
            .snapshot(self.deadlines.snapshot)
            .await
    }

    /// Receive the sole normalized unsolicited event. The bounded channel is
    /// closed if the session host terminates or its protocol is poisoned.
    pub async fn next_event(&mut self) -> Option<PrimeSessionEvent> {
        self.host
            .as_mut()
            .expect("a live session lease has a host")
            .next_event()
            .await
    }

    /// Close the owned session and return daemon ownership only after an
    /// authoritative cleanup proof. Any uncertain result consumes and shuts
    /// down the private daemon.
    pub async fn close(mut self) -> Result<PrimeDaemon, PrimeDaemonError> {
        // Keep every owned field armed in `self` across the await. Cancelling
        // this future therefore invokes Drop with the exact known obligation.
        if self.prove_known_cleanup().await {
            self.cleanup_obligation = CleanupObligation::None;
            self.initial_snapshot.take();
            drop(self.host.take());
            Ok(self.daemon.take().expect("a proved lease owns its daemon"))
        } else {
            let cleanup_timeout = self.deadlines.cleanup;
            poison_shutdown(
                self.daemon
                    .as_mut()
                    .expect("an uncertain lease owns its daemon"),
                cleanup_timeout,
            )
            .await;
            self.cleanup_obligation = CleanupObligation::None;
            self.initial_snapshot.take();
            drop(self.host.take());
            drop(self.daemon.take());
            Err(PrimeDaemonError::CleanupUncertain)
        }
    }

    async fn prove_known_cleanup(&mut self) -> bool {
        let active_session = match &self.cleanup_obligation {
            CleanupObligation::Known(active_session) => active_session,
            _ => panic!("a post-create owner has an exact cleanup obligation"),
        };
        prove_cleanup(
            self.daemon
                .as_mut()
                .expect("an armed cleanup owner has its daemon"),
            self.host
                .as_mut()
                .expect("an armed cleanup owner has its host"),
            active_session,
            self.deadlines,
        )
        .await
    }

    async fn abort_uncommitted(mut self, opening_error: PrimeDaemonError) -> PrimeDaemonError {
        self.host
            .as_mut()
            .expect("opening owner owns its host")
            .terminate()
            .await;
        shutdown_after_cleanup(self.daemon.as_mut().expect("opening owner owns its daemon")).await;
        self.disarm_after_shutdown();
        opening_error
    }

    async fn abort_unknown_create(mut self) -> PrimeDaemonError {
        self.host
            .as_mut()
            .expect("opening owner owns its host")
            .terminate()
            .await;
        let cleanup_timeout = self.deadlines.cleanup;
        poison_shutdown(
            self.daemon.as_mut().expect("opening owner owns its daemon"),
            cleanup_timeout,
        )
        .await;
        self.disarm_after_shutdown();
        PrimeDaemonError::CleanupUncertain
    }

    async fn abort_opening(mut self, opening_error: PrimeDaemonError) -> PrimeDaemonError {
        let proved = self.prove_known_cleanup().await;
        if proved {
            // Authoritative proof disarms the native-session obligation. The
            // daemon and host remain in this RAII owner during shutdown.
            self.cleanup_obligation = CleanupObligation::None;
            shutdown_after_cleanup(self.daemon.as_mut().expect("opening owner owns its daemon"))
                .await;
            self.disarm_after_shutdown();
            opening_error
        } else {
            let cleanup_timeout = self.deadlines.cleanup;
            poison_shutdown(
                self.daemon.as_mut().expect("opening owner owns its daemon"),
                cleanup_timeout,
            )
            .await;
            self.disarm_after_shutdown();
            PrimeDaemonError::CleanupUncertain
        }
    }

    fn disarm_after_shutdown(&mut self) {
        self.cleanup_obligation = CleanupObligation::None;
        self.initial_snapshot.take();
        drop(self.host.take());
        drop(self.daemon.take());
    }
}

impl Drop for PrimeSessionLease {
    fn drop(&mut self) {
        let Some(daemon) = self.daemon.take() else {
            return;
        };
        let host = self.host.take();
        let cleanup_obligation =
            std::mem::replace(&mut self.cleanup_obligation, CleanupObligation::None);
        let deadlines = self.deadlines;
        self.initial_snapshot.take();

        // Never bind authoritative cleanup to the caller's Tokio runtime. That
        // runtime may shut down immediately after cancelling the lease future,
        // but the native worker obligation must remain owned through its full
        // bounded cleanup horizon.
        let work = std::sync::Arc::new(std::sync::Mutex::new(Some((
            daemon,
            host,
            cleanup_obligation,
            deadlines,
        ))));
        let thread_work = std::sync::Arc::clone(&work);
        let spawned = std::thread::Builder::new()
            .name("prime-session-reaper".into())
            .spawn(move || {
                let Some((daemon, host, cleanup_obligation, deadlines)) = thread_work
                    .lock()
                    .expect("session reaper mutex poisoned")
                    .take()
                else {
                    return;
                };
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(reap_dropped_lease(
                        daemon,
                        host,
                        cleanup_obligation,
                        deadlines,
                    )),
                    Err(_) => {
                        // Runtime construction failure still drops both exact
                        // group owners on this dedicated thread.
                        drop(host);
                        drop(daemon);
                    }
                }
            });
        if spawned.is_err()
            && let Some((daemon, host, cleanup_obligation, deadlines)) =
                work.lock().expect("session reaper mutex poisoned").take()
        {
            // OS thread creation is fallible. As the last fallback, run the
            // same bounded reaper on a temporary local runtime rather than
            // degrading to visible-group kills only.
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(reap_dropped_lease(
                    daemon,
                    host,
                    cleanup_obligation,
                    deadlines,
                )),
                Err(_) => {
                    drop(host);
                    drop(daemon);
                }
            }
        }
    }
}

async fn reap_dropped_lease(
    mut daemon: PrimeDaemon,
    mut host: Option<SessionHost>,
    cleanup_obligation: CleanupObligation,
    deadlines: SessionDeadlines,
) {
    // A Drop reaper can be entered from any cancelled host or bootstrap await.
    // Do not guess which old-runtime channel still has a response in flight.
    // Disconnect the exact owner host and always establish a fresh nonowner
    // bootstrap connection on this dedicated reaper runtime.
    if let Some(host) = host.as_mut() {
        host.terminate().await;
    }
    let _ = respawn_bootstrap(&mut daemon, deadlines.connect).await;
    if daemon_has_exited(&mut daemon) {
        // A shutdown accepted before cancellation can finish while its fresh
        // nonowner bridge is connecting. Reap that exact supervisor generation
        // now instead of retaining a zombie through later query deadlines. No
        // cleanup proof is inferred.
        drop(host);
        return;
    }

    match cleanup_obligation {
        CleanupObligation::Known(active_session) => {
            let proved = poll_cleanup_status(&mut daemon, &active_session, deadlines.cleanup)
                .await
                .is_ok();
            drop(host);
            if proved {
                shutdown_after_cleanup(&mut daemon).await;
            } else {
                poison_shutdown(&mut daemon, deadlines.cleanup).await;
            }
        }
        CleanupObligation::UnknownCreateCommit => {
            drop(host);
            poison_shutdown(&mut daemon, deadlines.cleanup).await;
        }
        CleanupObligation::None => {
            drop(host);
            shutdown_after_cleanup(&mut daemon).await;
        }
    }
}

async fn respawn_bootstrap(
    daemon: &mut PrimeDaemon,
    configured_timeout: Duration,
) -> Result<(), PrimeDaemonError> {
    // `ChildStdin`/`ChildStdout` registrations belong to the runtime that
    // created them. Kill that stale bridge group and connect an equivalent
    // allowlisted bridge on the dedicated reaper runtime.
    daemon.bridge.kill_now();
    let replacement = BootstrapBridge::spawn(
        &daemon.package,
        &daemon.paths,
        &daemon.session_host_environment,
    )?;
    daemon.bridge = replacement;

    let timeout = configured_timeout.min(Duration::from_secs(5));
    let loaded = daemon
        .bridge
        .request(
            "public API load",
            json!({
                "op": "load",
                "entry": daemon.package.public_entry,
                "manifestVersion": daemon.package.version,
            }),
            timeout,
        )
        .await;
    super::validate_loaded(loaded)?;
    let ready = daemon
        .bridge
        .request(
            "daemon readiness",
            json!({
                "op": "connect",
                "socket": daemon.paths.socket,
                "timeoutMs": duration_millis(timeout),
            }),
            timeout,
        )
        .await;
    super::validate_ready(ready)?;
    Ok(())
}

async fn shutdown_after_cleanup(daemon: &mut PrimeDaemon) {
    let _ = daemon.shutdown_in_place().await;
}

fn poison_shutdown_timeout(configured_cleanup: Duration) -> Duration {
    configured_cleanup.max(DEFAULT_CLEANUP_TIMEOUT)
}

fn daemon_has_exited(daemon: &mut PrimeDaemon) -> bool {
    matches!(daemon.daemon.try_status(), Ok(Some(_)))
}

async fn poison_shutdown(daemon: &mut PrimeDaemon, configured_cleanup: Duration) {
    // A failed, timed-out, or malformed cleanup-status exchange poisons the
    // serialized SDK client behind the bootstrap bridge. Never enqueue daemon
    // shutdown behind that uncertain request. Kill the bridge group and prove
    // a fresh allowlisted nonowner connection first. Callers retain their exact
    // cleanup obligation across both awaits, so cancellation re-enters Drop's
    // fresh-bootstrap reaper rather than disarming the native session.
    let poison_horizon = poison_shutdown_timeout(configured_cleanup);
    daemon.shutdown_timeout = daemon.shutdown_timeout.max(poison_horizon);
    if daemon_has_exited(daemon) {
        return;
    }
    if respawn_bootstrap(daemon, configured_cleanup).await.is_err() {
        // The replacement may itself have an uncertain response in flight. Do
        // not reuse it for shutdown and do not kill the supervisor before its
        // owner-disconnect cleanup grace can finish. The caller's obligation
        // stays armed throughout this bounded wait. Its eventual Drop is the
        // process-group backstop, while cancellation retries through the
        // fresh-bootstrap reaper.
        daemon.bridge.kill_now();
        let horizon_deadline = Instant::now() + poison_horizon;
        if daemon.daemon.wait(poison_horizon).await.is_err() {
            // An immediate wait error is not proof that the supervisor exited.
            // Preserve the full grace horizon before the owning Drop backstop.
            let grace = horizon_deadline.saturating_duration_since(Instant::now());
            if !grace.is_zero() {
                tokio::time::sleep(grace).await;
            }
        }
        return;
    }
    let _ = daemon.shutdown_in_place().await;
}

async fn prove_cleanup(
    daemon: &mut PrimeDaemon,
    host: &mut SessionHost,
    active_session: &ActiveSessionId,
    deadlines: SessionDeadlines,
) -> bool {
    let close_deadline = match checked_deadline(deadlines.close, "session close") {
        Ok(deadline) => deadline,
        Err(_) => {
            host.terminate().await;
            return false;
        }
    };
    let close_timeout = match remaining(close_deadline, "session close", deadlines.close) {
        Ok(timeout) => timeout,
        Err(_) => {
            host.terminate().await;
            return false;
        }
    };
    if host.close_session(close_timeout).await.is_ok() {
        let reap_timeout = remaining(close_deadline, "session host reap", deadlines.close)
            .unwrap_or(MIN_REAP_SLICE);
        host.reap(reap_timeout).await;
        return true;
    }

    // A crashed or poisoned host is killed as one exact process group. Its
    // owner disconnect starts the authoritative daemon cleanup grace.
    host.terminate().await;
    poll_cleanup_status(daemon, active_session, deadlines.cleanup)
        .await
        .is_ok()
}

async fn poll_cleanup_status(
    daemon: &mut PrimeDaemon,
    active_session: &ActiveSessionId,
    timeout: Duration,
) -> Result<(), PrimeDaemonError> {
    let deadline = checked_deadline(timeout, "owned session cleanup")?;
    loop {
        let request_timeout =
            remaining(deadline, "owned session cleanup", timeout)?.min(CLEANUP_REQUEST_SLICE);
        let response = daemon
            .bridge
            .request(
                "owned session cleanup",
                json!({
                    "op": "cleanup-status",
                    "activeSessionId": active_session.as_str(),
                    "timeoutMs": duration_millis(request_timeout),
                }),
                request_timeout,
            )
            .await?;
        match response {
            BridgeResponse::OwnedSessionCleanup {
                status: OwnedSessionCleanupStatus::Settled,
                ..
            } => return Ok(()),
            BridgeResponse::OwnedSessionCleanup {
                status: OwnedSessionCleanupStatus::Active | OwnedSessionCleanupStatus::Stopping,
                ..
            } => {
                let sleep = remaining(deadline, "owned session cleanup", timeout)?
                    .min(CLEANUP_POLL_INTERVAL);
                tokio::time::sleep(sleep).await;
            }
            // Error, malformed, missing, and unrelated responses are never
            // interpreted as cleanup proof.
            _ => return Err(PrimeDaemonError::CleanupUncertain),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_extend_cleanup_beyond_owner_grace() {
        let cwd = tempfile::tempdir().unwrap();
        let config = PrimeSessionConfig::new(cwd.path()).unwrap();
        assert!(config.deadlines.cleanup >= Duration::from_secs(40));
        assert!(config.deadlines.cleanup > Duration::from_secs(30));
    }

    #[test]
    fn poison_shutdown_always_outlives_owner_disconnect_grace() {
        assert_eq!(
            poison_shutdown_timeout(Duration::from_secs(1)),
            DEFAULT_CLEANUP_TIMEOUT
        );
        assert_eq!(
            poison_shutdown_timeout(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn canonical_existing_directory_and_finite_deadlines_are_required() {
        let cwd = tempfile::tempdir().unwrap();
        let config = PrimeSessionConfig::new(cwd.path()).unwrap().with_timeouts(
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(40),
        );
        assert!(config.validate().is_err());

        let file = cwd.path().join("file");
        std::fs::write(&file, b"not a directory").unwrap();
        assert!(PrimeSessionConfig::new(file).is_err());
    }

    #[test]
    fn public_receipt_has_only_safe_bounded_projection() {
        let receipt = PrimeSessionSnapshotReceipt::new(3, true, false);
        assert_eq!(receipt.message_count(), 3);
        assert!(receipt.is_streaming());
        assert!(!receipt.has_cursor());
        assert_eq!(
            format!("{receipt:?}"),
            "PrimeSessionSnapshotReceipt { message_count: 3, is_streaming: true, has_cursor: false }"
        );
    }
}
