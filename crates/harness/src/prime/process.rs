use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::contract::{BridgeResponse, MAX_FRAME_BYTES};
use super::{PrimeDaemonError, PrimePackage, PrimePaths};

const TERM_GRACE: Duration = Duration::from_millis(750);
// The stock public DaemonClient reader has no line-size option yet
// (pylon-code/prime-agent#13). Keep bootstrap ingress in a small isolated heap
// plus Rust's operation deadline; the long-lived session host remains blocked
// on a true SDK ingress bound.
const BRIDGE_HEAP_MIB: &str = "64";

pub(super) type ProcessEnvironment = BTreeMap<OsString, OsString>;

pub(super) struct ProcessEnvironments {
    pub bridge: ProcessEnvironment,
    pub daemon: ProcessEnvironment,
}

/// Build separate bridge and daemon environments. The bootstrap bridge gets a
/// fixed non-secret allowlist. The Prime daemon keeps public provider and
/// extension configuration, while recursion state and process-loader injection
/// are removed before it becomes a top-level runtime.
pub(super) fn make_process_environments(
    source: Option<ProcessEnvironment>,
    package: &PrimePackage,
) -> ProcessEnvironments {
    let source = source.unwrap_or_else(|| std::env::vars_os().collect());
    let mut bridge = ProcessEnvironment::new();
    let mut daemon = ProcessEnvironment::new();
    for (name, value) in source {
        let text = name.to_string_lossy();
        if bridge_environment_name(&text) {
            bridge.insert(name.clone(), value.clone());
        }
        if daemon_environment_name(&text) {
            daemon.insert(name, value);
        }
    }
    compose_runtime_path(&mut bridge, package);
    compose_runtime_path(&mut daemon, package);
    ProcessEnvironments { bridge, daemon }
}

fn bridge_environment_name(name: &str) -> bool {
    matches!(
        name,
        "HOME"
            | "USER"
            | "LOGNAME"
            | "PATH"
            | "TMPDIR"
            | "TMP"
            | "TEMP"
            | "LANG"
            | "LC_ALL"
            | "NO_COLOR"
            | "FORCE_COLOR"
    ) || name.starts_with("LC_")
}

fn daemon_environment_name(name: &str) -> bool {
    name != "RLM_DEPTH"
        && !name.starts_with("PRIME_AGENT_INTERNAL_")
        && !matches!(
            name,
            "NODE_OPTIONS" | "NODE_PATH" | "LD_PRELOAD" | "LD_LIBRARY_PATH"
        )
        && !name.starts_with("DYLD_")
}

fn compose_runtime_path(environment: &mut ProcessEnvironment, package: &PrimePackage) {
    let mut paths = Vec::new();
    if let Some(directory) = package.node.parent() {
        paths.push(directory.to_path_buf());
    }
    if let Some(existing) = environment.get(OsStr::new("PATH")) {
        paths.extend(std::env::split_paths(existing));
    }
    if let Some(login) = crate::shell_env::login_shell_path() {
        paths.extend(std::env::split_paths(&login));
    }
    let mut seen = std::collections::HashSet::new();
    paths.retain(|path| !path.as_os_str().is_empty() && seen.insert(path.clone()));
    if let Ok(joined) = std::env::join_paths(paths) {
        environment.insert(OsString::from("PATH"), joined);
    }
}

pub(super) struct OwnedChild {
    child: Option<Child>,
    process: &'static str,
    process_group: Option<u32>,
    reaped: bool,
    group_cleaned: bool,
}

impl OwnedChild {
    fn from_child(child: Child, process: &'static str) -> Self {
        let process_group = child.id();
        Self {
            child: Some(child),
            process,
            process_group,
            reaped: false,
            group_cleaned: false,
        }
    }

    fn child_mut(&mut self) -> Result<&mut Child, PrimeDaemonError> {
        self.child.as_mut().ok_or(PrimeDaemonError::ProcessExit {
            process: self.process,
            status: "process handle was transferred to its reaper".into(),
        })
    }

    pub fn spawn_daemon(
        package: &PrimePackage,
        paths: &PrimePaths,
        environment: &ProcessEnvironment,
    ) -> Result<Self, PrimeDaemonError> {
        // Run the validated canonical package bin through the validated
        // canonical Node executable. This closes symlink-retarget and shebang
        // PATH races between package validation and spawn.
        let mut command = Command::new(&package.node);
        command
            .arg(&package.executable)
            .args([
                "--mode",
                "daemon",
                "--daemon-socket",
                &paths.socket.to_string_lossy(),
                "--offline",
                "--session-dir",
                &paths.session_dir.to_string_lossy(),
            ])
            .env_clear()
            .envs(environment)
            .stdin(Stdio::null())
            // Native output can contain prompts, tool arguments, credentials,
            // and paths. Drain to the null sink and never project it.
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_process_group(&mut command);
        let child = command
            .spawn()
            .map_err(|error| PrimeDaemonError::io("daemon spawn", &error))?;
        Ok(Self::from_child(child, "daemon"))
    }

    pub fn try_status(&mut self) -> Result<Option<ExitStatus>, PrimeDaemonError> {
        let status = self
            .child_mut()?
            .try_wait()
            .map_err(|error| PrimeDaemonError::io("process status", &error))?;
        if status.is_some() {
            self.reaped = true;
            self.kill_group_remainders();
        }
        Ok(status)
    }

    pub async fn wait(&mut self, timeout: Duration) -> Result<ExitStatus, PrimeDaemonError> {
        let result = {
            let process = self.process;
            let child = self.child_mut()?;
            tokio::time::timeout(timeout, child.wait())
                .await
                .map_err(|_| PrimeDaemonError::Timeout {
                    stage: if process == "daemon" {
                        "daemon process wait"
                    } else {
                        "bridge process wait"
                    },
                    timeout,
                })?
        };
        match result {
            Ok(status) => {
                self.reaped = true;
                // The group may still contain grandchildren after its leader
                // exits normally or obeys TERM. Always retire those members.
                self.kill_group_remainders();
                Ok(status)
            }
            Err(error) => Err(PrimeDaemonError::io("process wait", &error)),
        }
    }

    pub async fn terminate(&mut self) {
        if self.try_status().ok().flatten().is_some() {
            return;
        }
        send_group_signal(self.process_group, libc_signal_term());
        if self.wait(TERM_GRACE).await.is_ok() {
            return;
        }
        self.kill_group_remainders();
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
        let _ = self.wait(TERM_GRACE).await;
    }

    pub fn process_name(&self) -> &'static str {
        self.process
    }

    pub fn kill_now(&mut self) {
        if !self.reaped {
            let _ = self.try_status();
        }
        self.kill_group_remainders();
        if !self.reaped
            && let Some(child) = self.child.as_mut()
        {
            let _ = child.start_kill();
        }
    }

    fn kill_group_remainders(&mut self) {
        if self.group_cleaned {
            return;
        }
        send_group_signal(self.process_group, libc_signal_kill());
        self.group_cleaned = true;
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.kill_now();
        if self.reaped {
            return;
        }
        let Some(mut child) = self.child.take() else {
            return;
        };
        // Transfer the handle to a non-cancellable reaper. Cancellation of a
        // startup/shutdown future therefore cannot discard the only wait
        // owner and leave a zombie leader behind.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(TERM_GRACE, child.wait()).await;
            });
        } else {
            let _ = child.start_kill();
        }
    }
}

/// Bootstrap-only SDK control process.
///
/// It owns daemon hello and shutdown requests only. It is deliberately not a
/// session/event transport: the later session host must use an asynchronous
/// response/event demultiplexer with separately owned connections.
pub(super) struct BootstrapBridge {
    process: OwnedChild,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl BootstrapBridge {
    pub fn spawn(
        package: &PrimePackage,
        paths: &PrimePaths,
        environment: &ProcessEnvironment,
    ) -> Result<Self, PrimeDaemonError> {
        let mut command = Command::new(&package.node);
        command
            .arg(format!("--max-old-space-size={BRIDGE_HEAP_MIB}"))
            .arg(&paths.shim)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_process_group(&mut command);
        let mut child = command
            .spawn()
            .map_err(|error| PrimeDaemonError::io("bridge spawn", &error))?;
        let stdin = child
            .stdin
            .take()
            .ok_or(PrimeDaemonError::InvalidBridgeFrame {
                reason: "control input was not created",
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(PrimeDaemonError::InvalidBridgeFrame {
                reason: "control output was not created",
            })?;
        Ok(Self {
            process: OwnedChild::from_child(child, "bridge"),
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            next_id: 1,
        })
    }

    pub async fn request(
        &mut self,
        stage: &'static str,
        mut body: Value,
        timeout: Duration,
    ) -> Result<BridgeResponse, PrimeDaemonError> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let object = body
            .as_object_mut()
            .ok_or(PrimeDaemonError::InvalidBridgeFrame {
                reason: "request body is not an object",
            })?;
        object.insert("v".into(), Value::from(super::contract::CONTROL_VERSION));
        object.insert("id".into(), Value::from(id));
        let mut bytes =
            serde_json::to_vec(&body).map_err(|_| PrimeDaemonError::InvalidBridgeFrame {
                reason: "request serialization failed",
            })?;
        if bytes.len().saturating_add(1) > MAX_FRAME_BYTES {
            return Err(PrimeDaemonError::InvalidBridgeFrame {
                reason: "request exceeds the frame bound",
            });
        }
        bytes.push(b'\n');

        let operation = async {
            let stdin = self
                .stdin
                .as_mut()
                .ok_or(PrimeDaemonError::InvalidBridgeFrame {
                    reason: "control input is closed",
                })?;
            stdin
                .write_all(&bytes)
                .await
                .map_err(|error| PrimeDaemonError::io("bridge write", &error))?;
            stdin
                .flush()
                .await
                .map_err(|error| PrimeDaemonError::io("bridge write", &error))?;
            let Some(line) = read_bounded_line(&mut self.stdout).await? else {
                let status = self.process.try_status()?.map(describe_status);
                return Err(PrimeDaemonError::ProcessExit {
                    process: self.process.process_name(),
                    status: status.unwrap_or_else(|| "control channel closed".into()),
                });
            };
            let response = serde_json::from_slice::<BridgeResponse>(&line).map_err(|_| {
                PrimeDaemonError::InvalidBridgeFrame {
                    reason: "response is malformed JSON",
                }
            })?;
            response.validate_meta(id)?;
            Ok(response)
        };

        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| PrimeDaemonError::Timeout { stage, timeout })?
    }

    pub async fn close(&mut self, timeout: Duration) {
        self.stdin.take();
        if self.process.wait(timeout).await.is_err() {
            self.process.terminate().await;
        }
    }

    pub async fn terminate(&mut self) {
        self.stdin.take();
        self.process.terminate().await;
    }

    pub fn kill_now(&mut self) {
        self.stdin.take();
        self.process.kill_now();
    }
}

async fn read_bounded_line<R>(reader: &mut R) -> Result<Option<Vec<u8>>, PrimeDaemonError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| PrimeDaemonError::io("bridge read", &error))?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(consumed) > MAX_FRAME_BYTES {
            return Err(PrimeDaemonError::InvalidBridgeFrame {
                reason: "response exceeds the frame bound",
            });
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

fn describe_status(status: ExitStatus) -> String {
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

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn send_group_signal(group: Option<u32>, signal: i32) {
    if let Some(group) = group {
        unsafe {
            libc::kill(-(group as i32), signal);
        }
    }
}

#[cfg(not(unix))]
fn send_group_signal(_group: Option<u32>, _signal: i32) {}

#[cfg(unix)]
const fn libc_signal_term() -> i32 {
    libc::SIGTERM
}

#[cfg(not(unix))]
const fn libc_signal_term() -> i32 {
    0
}

#[cfg(unix)]
const fn libc_signal_kill() -> i32 {
    libc::SIGKILL
}

#[cfg(not(unix))]
const fn libc_signal_kill() -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_environments_isolate_the_bridge_without_breaking_provider_configuration() {
        for bridge_secret in [
            "ANTHROPIC_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "ACME_CUSTOM_PROVIDER_TOKEN",
        ] {
            assert!(
                !bridge_environment_name(bridge_secret),
                "bridge kept {bridge_secret}"
            );
            assert!(
                daemon_environment_name(bridge_secret),
                "daemon dropped {bridge_secret}"
            );
        }
        for rejected in [
            "RLM_DEPTH",
            "PRIME_AGENT_INTERNAL_SECRET",
            "NODE_OPTIONS",
            "NODE_PATH",
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "DYLD_INSERT_LIBRARIES",
        ] {
            assert!(!daemon_environment_name(rejected), "daemon kept {rejected}");
        }
        for shared in ["HOME", "PATH", "TMPDIR", "LANG", "LC_CTYPE"] {
            assert!(bridge_environment_name(shared), "bridge dropped {shared}");
            assert!(daemon_environment_name(shared), "daemon dropped {shared}");
        }
        assert!(!bridge_environment_name("RLM_MAX_DEPTH"));
        assert!(daemon_environment_name("RLM_MAX_DEPTH"));
        assert!(daemon_environment_name("PRIME_AGENT_CODING_AGENT_DIR"));
    }

    #[tokio::test]
    async fn frame_reader_is_bounded() {
        let bytes = vec![b'x'; MAX_FRAME_BYTES + 1];
        let mut reader = BufReader::new(bytes.as_slice());
        let error = read_bounded_line(&mut reader).await.unwrap_err();
        assert!(matches!(error, PrimeDaemonError::InvalidBridgeFrame { .. }));
    }
}
