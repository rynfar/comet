//! Native Prime Agent daemon foundation.
//!
//! The layers are intentionally narrow:
//! - [`package`] proves the configured executable belongs to the installed
//!   package and resolves only its public root ESM export.
//! - [`paths`] creates private host-only transport and session paths.
//! - [`process`] owns bounded process groups and a bootstrap-only JSONL client.
//!
//! Session events must not reuse the bootstrap client's request/next-line
//! shape. The next session issue must define an asynchronous SDK host that
//! demultiplexes response IDs and normalized unsolicited events.
//!
//! Stock Prime may launch resident session workers as detached processes. The
//! Unix groups here strictly own the bootstrap and daemon leaders plus ordinary
//! descendants, but they are not proof of worker settlement. Before sessions
//! ship, authoritative daemon shutdown and worker/descriptor reconciliation
//! must have separate contract tests.
//! - [`contract`] validates the stock daemon baseline and optional offers.
//!
//! This slice starts no agent session and publishes no synchronized state.

mod contract;
mod error;
mod package;
mod paths;
mod process;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::json;

pub use contract::{
    CORRELATED_PROMPT_LIFECYCLE_CAPABILITY, PrimeServerCapabilities, REQUIRED_DAEMON_CAPABILITIES,
};
pub use error::PrimeDaemonError;
use package::PrimePackage;
use paths::PrimePaths;
use process::{BootstrapBridge, OwnedChild, ProcessEnvironment};

const SHIM_SOURCE: &str = include_str!("shim.mjs");

/// Local-only configuration. Native identifiers and paths stay inside the
/// harness process and must never be copied into Comet documents or RPC data.
#[derive(Clone)]
pub struct PrimeDaemonConfig {
    executable: PathBuf,
    state_dir: PathBuf,
    instance_id: String,
    socket_root: PathBuf,
    environment: Option<ProcessEnvironment>,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

fn default_socket_root() -> PathBuf {
    #[cfg(unix)]
    if let Ok(path) = std::fs::canonicalize("/tmp") {
        return path;
    }
    std::env::temp_dir()
}

impl PrimeDaemonConfig {
    /// Configure an explicit Prime executable, a host-private state root, and a
    /// stable provider instance identifier.
    pub fn new(
        executable: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        instance_id: impl Into<String>,
    ) -> Self {
        Self {
            executable: executable.into(),
            state_dir: state_dir.into(),
            instance_id: instance_id.into(),
            socket_root: default_socket_root(),
            environment: None,
            startup_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    /// Search for `prime-agent` through Comet's normal CLI discovery paths.
    pub fn discover(state_dir: impl Into<PathBuf>, instance_id: impl Into<String>) -> Self {
        Self::new(PathBuf::new(), state_dir, instance_id)
    }

    /// Override the total startup and graceful-shutdown deadlines.
    pub fn with_timeouts(mut self, startup: Duration, shutdown: Duration) -> Self {
        self.startup_timeout = startup;
        self.shutdown_timeout = shutdown;
        self
    }

    /// Inject an immutable source environment. The bootstrap bridge receives
    /// only a fixed non-secret allowlist. The Prime daemon preserves public
    /// provider and extension configuration, including environment-backed
    /// credentials, but drops nested-agent state and loader injection. None of
    /// these values are emitted through the bridge or Comet synchronization.
    ///
    /// This seam also keeps lifecycle tests parallel-safe without global env
    /// mutation.
    pub fn with_environment(
        mut self,
        environment: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Self {
        self.environment = Some(environment.into_iter().collect::<BTreeMap<_, _>>());
        self
    }

    /// Test-only socket-root injection. Production callers should keep the OS
    /// temp root so Unix-domain socket paths stay short.
    #[doc(hidden)]
    pub fn with_socket_root(mut self, socket_root: impl Into<PathBuf>) -> Self {
        self.socket_root = socket_root.into();
        self
    }
}

/// Owns one public-SDK bridge and one captured native daemon process group.
pub struct PrimeDaemon {
    server_capabilities: PrimeServerCapabilities,
    bridge: BootstrapBridge,
    daemon: OwnedChild,
    paths: PrimePaths,
    shutdown_timeout: Duration,
    stopped: bool,
}

impl PrimeDaemon {
    /// Validate the package and public SDK, claim the stable private identity,
    /// retire a compatible crash leftover, then negotiate a fresh daemon.
    pub async fn start(config: PrimeDaemonConfig) -> Result<Self, PrimeDaemonError> {
        #[cfg(not(unix))]
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "native daemon mode requires a verified per-user transport",
        });

        #[cfg(unix)]
        {
            if config.startup_timeout.is_zero() || config.shutdown_timeout.is_zero() {
                return Err(PrimeDaemonError::TransportSecurity {
                    reason: "daemon deadlines must be greater than zero",
                });
            }
            let deadline = checked_deadline(config.startup_timeout, "daemon startup")?;
            let package_timeout =
                remaining(deadline, "package resolution", config.startup_timeout)?;
            let executable = config.executable.clone();
            let package = tokio::time::timeout(
                package_timeout,
                tokio::task::spawn_blocking(move || package::resolve(&executable)),
            )
            .await
            .map_err(|_| PrimeDaemonError::Timeout {
                stage: "package resolution",
                timeout: package_timeout,
            })?
            .map_err(|_| PrimeDaemonError::IncompatiblePackage {
                reason: "the package resolution worker failed",
            })??;
            let paths =
                PrimePaths::prepare(&config.state_dir, &config.instance_id, &config.socket_root)?;
            if let Err(error) = paths::write_shim(&paths.shim, SHIM_SOURCE) {
                paths.cleanup();
                return Err(error);
            }
            let environments = process::make_process_environments(config.environment, &package);

            let mut bridge = match BootstrapBridge::spawn(&package, &paths, &environments.bridge) {
                Ok(bridge) => bridge,
                Err(error) => {
                    paths.cleanup();
                    return Err(error);
                }
            };
            let loaded = bridge
                .request(
                    "public API load",
                    json!({
                        "op": "load",
                        "entry": package.public_entry,
                        "manifestVersion": package.version,
                    }),
                    remaining(deadline, "public API load", config.startup_timeout)?,
                )
                .await;
            if let Err(error) = validate_loaded(loaded) {
                bridge.terminate().await;
                paths.cleanup();
                return Err(error);
            }
            if let Err(error) =
                retire_existing_daemon(&mut bridge, &paths, deadline, config.startup_timeout).await
            {
                bridge.terminate().await;
                paths.cleanup();
                return Err(error);
            }

            let mut daemon = match OwnedChild::spawn_daemon(&package, &paths, &environments.daemon)
            {
                Ok(daemon) => daemon,
                Err(error) => {
                    bridge.terminate().await;
                    paths.cleanup();
                    return Err(error);
                }
            };
            let timeout = match remaining(deadline, "daemon readiness", config.startup_timeout) {
                Ok(timeout) => timeout,
                Err(error) => {
                    emergency_cleanup(&mut bridge, &mut daemon).await;
                    paths.cleanup();
                    return Err(error);
                }
            };
            let readiness = tokio::select! {
                response = bridge.request(
                    "daemon readiness",
                    json!({
                        "op": "connect",
                        "socket": paths.socket,
                        "timeoutMs": duration_millis(timeout),
                    }),
                    timeout,
                ) => validate_ready(response),
                status = daemon.wait(timeout) => match status {
                    Ok(status) => Err(PrimeDaemonError::ProcessExit {
                        process: daemon.process_name(),
                        status: safe_status(status),
                    }),
                    Err(error) => Err(error),
                },
            };
            let capabilities = match readiness {
                Ok(capabilities) => capabilities,
                Err(error) => {
                    emergency_cleanup(&mut bridge, &mut daemon).await;
                    paths.cleanup();
                    return Err(error);
                }
            };
            if paths.socket_identity()?.is_none() {
                emergency_cleanup(&mut bridge, &mut daemon).await;
                return Err(PrimeDaemonError::IncompatibleHello {
                    reason: "the negotiated private socket is missing".into(),
                });
            }

            Ok(Self {
                server_capabilities: capabilities,
                bridge,
                daemon,
                paths,
                shutdown_timeout: config.shutdown_timeout,
                stopped: false,
            })
        }
    }

    /// Return the daemon's validated capability offers. Later attach code must
    /// still negotiate matching client capabilities before using extensions.
    pub fn server_capabilities(&self) -> &PrimeServerCapabilities {
        &self.server_capabilities
    }

    /// Consume the daemon owner, request native shutdown, and bound every wait.
    /// Consuming ownership makes cancellation fail-safe: dropping this future
    /// transfers child handles to their reapers instead of leaving a poisoned
    /// bootstrap request channel available for reuse.
    pub async fn shutdown(mut self) -> Result<(), PrimeDaemonError> {
        let deadline = checked_deadline(self.shutdown_timeout, "daemon shutdown")?;
        let request_timeout = remaining(deadline, "daemon shutdown", self.shutdown_timeout)?;
        let response = self
            .bridge
            .request(
                "daemon shutdown",
                json!({
                    "op": "shutdown",
                    "timeoutMs": duration_millis(request_timeout),
                }),
                request_timeout,
            )
            .await;
        let mut outcome = validate_shutdown(response);

        if outcome.is_ok() {
            let wait_timeout = remaining(deadline, "daemon exit", self.shutdown_timeout);
            outcome = match wait_timeout {
                Ok(timeout) => match self.daemon.wait(timeout).await {
                    Ok(status) if status.success() => Ok(()),
                    Ok(status) => Err(PrimeDaemonError::ProcessExit {
                        process: self.daemon.process_name(),
                        status: safe_status(status),
                    }),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
        }
        if outcome.is_err() {
            emergency_cleanup(&mut self.bridge, &mut self.daemon).await;
        } else {
            let bridge_timeout = remaining(deadline, "bridge exit", self.shutdown_timeout)
                .unwrap_or(Duration::from_millis(1));
            self.bridge.close(bridge_timeout).await;
        }
        self.paths.cleanup();
        self.stopped = true;
        outcome
    }
}

impl Drop for PrimeDaemon {
    fn drop(&mut self) {
        if !self.stopped {
            // Signal both owned groups before removing their socket. Field
            // drops repeat the kill as a final backstop. Persistent sessions
            // are retained; only this unique runtime directory is removed.
            self.bridge.kill_now();
            self.daemon.kill_now();
            self.paths.cleanup();
        }
    }
}

/// Emergency cleanup is bounded by the process owner's TERM/KILL grace and
/// runs both groups concurrently. It may extend a configured graceful deadline
/// by at most 1.5 seconds, never by one grace sequence per process.
async fn emergency_cleanup(bridge: &mut BootstrapBridge, daemon: &mut OwnedChild) {
    tokio::join!(bridge.terminate(), daemon.terminate());
}

#[cfg(unix)]
async fn retire_existing_daemon(
    bridge: &mut BootstrapBridge,
    paths: &PrimePaths,
    deadline: Instant,
    configured_timeout: Duration,
) -> Result<(), PrimeDaemonError> {
    use tokio::net::UnixStream;

    let Some(existing_identity) = paths.socket_identity()? else {
        return Ok(());
    };
    let probe_timeout = remaining(deadline, "existing daemon probe", configured_timeout)?
        .min(Duration::from_millis(500));
    match tokio::time::timeout(probe_timeout, UnixStream::connect(&paths.socket)).await {
        Ok(Ok(stream)) => drop(stream),
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            paths.remove_stale_socket(existing_identity)?;
            return Ok(());
        }
        Ok(Err(error)) => return Err(PrimeDaemonError::io("existing daemon probe", &error)),
        Err(_) => {
            return Err(PrimeDaemonError::Timeout {
                stage: "existing daemon probe",
                timeout: probe_timeout,
            });
        }
    }

    let connect_timeout = remaining(deadline, "existing daemon handshake", configured_timeout)?;
    let response = bridge
        .request(
            "existing daemon handshake",
            json!({
                "op": "connect",
                "socket": paths.socket,
                "timeoutMs": duration_millis(connect_timeout),
            }),
            connect_timeout,
        )
        .await;
    validate_ready(response)?;
    if paths.socket_identity()? != Some(existing_identity) {
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "the existing daemon socket changed during handshake",
        });
    }

    let shutdown_timeout = remaining(deadline, "existing daemon retirement", configured_timeout)?;
    let response = bridge
        .request(
            "existing daemon retirement",
            json!({
                "op": "shutdown",
                "timeoutMs": duration_millis(shutdown_timeout),
            }),
            shutdown_timeout,
        )
        .await;
    validate_shutdown(response)?;
    wait_for_socket_retirement(paths, deadline, configured_timeout).await
}

#[cfg(not(unix))]
async fn retire_existing_daemon(
    _bridge: &mut BootstrapBridge,
    _paths: &PrimePaths,
    _deadline: Instant,
    _configured_timeout: Duration,
) -> Result<(), PrimeDaemonError> {
    Err(PrimeDaemonError::TransportSecurity {
        reason: "native daemon ownership requires Unix socket verification",
    })
}

#[cfg(unix)]
async fn wait_for_socket_retirement(
    paths: &PrimePaths,
    deadline: Instant,
    configured_timeout: Duration,
) -> Result<(), PrimeDaemonError> {
    use tokio::net::UnixStream;

    loop {
        let Some(identity) = paths.socket_identity()? else {
            return Ok(());
        };
        let remaining = remaining(deadline, "existing daemon retirement", configured_timeout)?;
        let probe = remaining.min(Duration::from_millis(100));
        match tokio::time::timeout(probe, UnixStream::connect(&paths.socket)).await {
            Ok(Ok(stream)) => drop(stream),
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                paths.remove_stale_socket(identity)?;
                return Ok(());
            }
            Ok(Err(error)) => {
                return Err(PrimeDaemonError::io("existing daemon retirement", &error));
            }
            Err(_) => {}
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(25))).await;
    }
}

fn validate_loaded(
    response: Result<contract::BridgeResponse, PrimeDaemonError>,
) -> Result<(), PrimeDaemonError> {
    match response? {
        contract::BridgeResponse::Loaded {
            protocol_version, ..
        } if protocol_version >= contract::MIN_PROTOCOL_VERSION => Ok(()),
        contract::BridgeResponse::Loaded { .. } => Err(PrimeDaemonError::IncompatiblePackage {
            reason: "the public daemon protocol is too old",
        }),
        contract::BridgeResponse::Error { code, .. } => Err(PrimeDaemonError::Bridge {
            stage: "public API load",
            code,
        }),
        _ => Err(PrimeDaemonError::InvalidBridgeFrame {
            reason: "unexpected public API load response",
        }),
    }
}

fn validate_ready(
    response: Result<contract::BridgeResponse, PrimeDaemonError>,
) -> Result<PrimeServerCapabilities, PrimeDaemonError> {
    match response? {
        contract::BridgeResponse::Ready {
            protocol_version,
            capabilities,
            ..
        } => PrimeServerCapabilities::validated(protocol_version, capabilities),
        contract::BridgeResponse::Error { code, .. } => Err(PrimeDaemonError::Bridge {
            stage: "daemon readiness",
            code,
        }),
        _ => Err(PrimeDaemonError::InvalidBridgeFrame {
            reason: "unexpected daemon readiness response",
        }),
    }
}

fn validate_shutdown(
    response: Result<contract::BridgeResponse, PrimeDaemonError>,
) -> Result<(), PrimeDaemonError> {
    match response? {
        contract::BridgeResponse::Shutdown {
            acknowledged: true, ..
        } => Ok(()),
        contract::BridgeResponse::Shutdown { .. } => Err(PrimeDaemonError::Bridge {
            stage: "daemon shutdown",
            code: "shutdown-not-acknowledged".into(),
        }),
        contract::BridgeResponse::Error { code, .. } => Err(PrimeDaemonError::Bridge {
            stage: "daemon shutdown",
            code,
        }),
        _ => Err(PrimeDaemonError::InvalidBridgeFrame {
            reason: "unexpected daemon shutdown response",
        }),
    }
}

fn checked_deadline(timeout: Duration, stage: &'static str) -> Result<Instant, PrimeDaemonError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or(PrimeDaemonError::Timeout { stage, timeout })
}

fn remaining(
    deadline: Instant,
    stage: &'static str,
    configured: Duration,
) -> Result<Duration, PrimeDaemonError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(PrimeDaemonError::Timeout {
            stage,
            timeout: configured,
        })
}

fn duration_millis(duration: Duration) -> u64 {
    // JavaScript Number.MAX_SAFE_INTEGER. Larger values lose request deadline
    // precision, so clamp before crossing the bootstrap control boundary.
    const MAX_SAFE_INTEGER: u128 = 9_007_199_254_740_991;
    duration.as_millis().clamp(1, MAX_SAFE_INTEGER) as u64
}

fn safe_status(status: std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit code {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    "unknown exit".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extreme_deadlines_fail_or_clamp_without_panicking() {
        assert!(checked_deadline(Duration::MAX, "test").is_err());
        assert_eq!(duration_millis(Duration::MAX), 9_007_199_254_740_991);
    }
}
