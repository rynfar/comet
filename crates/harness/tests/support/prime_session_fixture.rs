use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use zeron_harness::prime::PrimeDaemonConfig;

pub const SESSION_SERVER_CAPABILITIES: &[&str] = &[
    "attach_snapshot",
    "event_sequence",
    "client_owned_sessions",
    "extension_ui",
    "session_input_admission",
    "prompt_admission_cancellation",
    "chunked_snapshot",
    "immutable_snapshot_transfer_v1",
    "authoritative_owned_session_cleanup_v1",
];

pub const REQUIRED_SESSION_SERVER_OFFERS: &[&str] = &[
    "client_owned_sessions",
    "chunked_snapshot",
    "immutable_snapshot_transfer_v1",
    "authoritative_owned_session_cleanup_v1",
];

pub const SDK_FEATURE: &str = "bounded_daemon_ingress_v1";
pub const ACTIVE_SESSION_SENTINEL: &str = "active-DO-NOT-SURFACE-PRIME-ID";
pub const SESSION_ID_SENTINEL: &str = "session-DO-NOT-SURFACE-PRIME-ID";
pub const SESSION_FILE_SENTINEL: &str = "DO-NOT-SURFACE-session-file.jsonl";
pub const RAW_ERROR_SENTINEL: &str = "DO-NOT-SURFACE-RAW-SDK-ERROR";
pub const ENV_SENTINEL: &str = "DO-NOT-SURFACE-ENV-SECRET";

pub struct PrimeSessionFixtureBuilder {
    sdk_features_source: String,
    capabilities: Vec<String>,
    control: Value,
}

impl Default for PrimeSessionFixtureBuilder {
    fn default() -> Self {
        Self {
            sdk_features_source: format!(
                "export const PRIME_AGENT_SDK_FEATURES = Object.freeze([{}]);",
                js_string(SDK_FEATURE)
            ),
            capabilities: SESSION_SERVER_CAPABILITIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            control: json!({"mode":"normal"}),
        }
    }
}

impl PrimeSessionFixtureBuilder {
    pub fn without_sdk_features(mut self) -> Self {
        self.sdk_features_source.clear();
        self
    }

    pub fn sdk_features(mut self, value: Value, frozen: bool) -> Self {
        let encoded = serde_json::to_string(&value).unwrap();
        self.sdk_features_source = if frozen {
            format!("export const PRIME_AGENT_SDK_FEATURES = Object.freeze({encoded});")
        } else {
            format!("export const PRIME_AGENT_SDK_FEATURES = {encoded};")
        };
        self
    }

    pub fn capabilities(mut self, values: &[&str]) -> Self {
        self.capabilities = values.iter().map(|value| (*value).to_owned()).collect();
        self
    }

    pub fn control(mut self, value: Value) -> Self {
        self.control = value;
        self
    }

    pub fn build(self) -> PrimeSessionFixture {
        PrimeSessionFixture::build(self)
    }
}

pub struct PrimeSessionFixture {
    _temp: TempDir,
    _socket_temp: TempDir,
    pub executable: PathBuf,
    pub cli_path: PathBuf,
    pub node_path: PathBuf,
    pub package_root: PathBuf,
    pub public_entry: PathBuf,
    pub state_root: PathBuf,
    pub socket_root: PathBuf,
    pub working_directory: PathBuf,
    pub session_directory: PathBuf,
    pub observation_path: PathBuf,
    pub control_path: PathBuf,
    pub gate_root: PathBuf,
    pub descriptor_path: PathBuf,
    pub trap_path: PathBuf,
}

impl PrimeSessionFixture {
    fn build(builder: PrimeSessionFixtureBuilder) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let socket_temp = tempfile::tempdir_in("/tmp").unwrap();
        let package_root = temp
            .path()
            .join("lib")
            .join("node_modules")
            .join("prime-agent");
        let public_entry = package_root.join("dist/index.mjs");
        let cli_path = package_root.join("dist/bundle/cli.mjs");
        let trap_module = package_root.join("dist/trap.mjs");
        std::fs::create_dir_all(public_entry.parent().unwrap()).unwrap();
        std::fs::create_dir_all(cli_path.parent().unwrap()).unwrap();

        let observation_path = temp.path().join("private-observations.jsonl");
        let control_path = temp.path().join("private-control.json");
        let gate_root = temp.path().join("private-gates");
        let descriptor_path = temp.path().join(SESSION_FILE_SENTINEL);
        let trap_path = temp.path().join("private-export-imported");
        let working_directory = temp.path().join("worktree");
        let session_directory = temp.path().join("sessions-for-raw-host");
        let state_root = temp.path().join("state");
        for directory in [
            &gate_root,
            &working_directory,
            &session_directory,
            &state_root,
        ] {
            std::fs::create_dir(directory).unwrap();
        }
        let state_root = std::fs::canonicalize(state_root).unwrap();
        let socket_root = std::fs::canonicalize(socket_temp.path()).unwrap();
        let working_directory = std::fs::canonicalize(working_directory).unwrap();
        let session_directory = std::fs::canonicalize(session_directory).unwrap();
        write_json_atomic(&control_path, &builder.control);

        let manifest = json!({
            "name":"prime-agent",
            "version":"0.8.1",
            "type":"module",
            "bin":{"prime-agent":"./dist/bundle/cli.mjs"},
            "main":"./dist/trap.mjs",
            "exports":{
                ".":{"import":"./dist/index.mjs"},
                "./private":"./dist/trap.mjs"
            }
        });
        std::fs::write(
            package_root.join("package.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(
            &trap_module,
            format!(
                "import fs from 'node:fs'; fs.writeFileSync({}, 'bad'); throw new Error('private export imported');\n",
                js_string(&trap_path.to_string_lossy())
            ),
        )
        .unwrap();

        let public_source = PUBLIC_MODULE
            .replace("__SDK_FEATURE_EXPORT__", &builder.sdk_features_source)
            .replace(
                "__OBSERVATION_PATH__",
                &js_string(&observation_path.to_string_lossy()),
            )
            .replace(
                "__CONTROL_PATH__",
                &js_string(&control_path.to_string_lossy()),
            )
            .replace("__GATE_ROOT__", &js_string(&gate_root.to_string_lossy()))
            .replace("__ACTIVE_SESSION_ID__", &js_string(ACTIVE_SESSION_SENTINEL))
            .replace("__SESSION_ID__", &js_string(SESSION_ID_SENTINEL))
            .replace("__SESSION_FILE_NAME__", &js_string(SESSION_FILE_SENTINEL))
            .replace("__RAW_ERROR__", &js_string(RAW_ERROR_SENTINEL));
        std::fs::write(&public_entry, public_source).unwrap();

        let daemon_source = FAKE_DAEMON
            .replace(
                "__OBSERVATION_PATH__",
                &js_string(&observation_path.to_string_lossy()),
            )
            .replace(
                "__CONTROL_PATH__",
                &js_string(&control_path.to_string_lossy()),
            )
            .replace("__GATE_ROOT__", &js_string(&gate_root.to_string_lossy()))
            .replace(
                "__DESCRIPTOR_PATH__",
                &js_string(&descriptor_path.to_string_lossy()),
            )
            .replace(
                "__CAPABILITIES__",
                &serde_json::to_string(&builder.capabilities).unwrap(),
            )
            .replace("__ACTIVE_SESSION_ID__", &js_string(ACTIVE_SESSION_SENTINEL))
            .replace("__SESSION_ID__", &js_string(SESSION_ID_SENTINEL))
            .replace("__SESSION_FILE_NAME__", &js_string(SESSION_FILE_SENTINEL))
            .replace("__RAW_ERROR__", &js_string(RAW_ERROR_SENTINEL));
        std::fs::write(&cli_path, daemon_source).unwrap();
        make_executable(&cli_path);

        let bin = temp.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let executable = bin.join("prime-agent");
        symlink_relative(
            Path::new("../lib/node_modules/prime-agent/dist/bundle/cli.mjs"),
            &executable,
        );
        let node_path = find_node();
        symlink_relative(&node_path, &bin.join("node"));

        Self {
            _temp: temp,
            _socket_temp: socket_temp,
            executable,
            cli_path,
            node_path,
            package_root,
            public_entry,
            state_root,
            socket_root,
            working_directory,
            session_directory,
            observation_path,
            control_path,
            gate_root,
            descriptor_path,
            trap_path,
        }
    }

    pub fn config(&self, startup: Duration, shutdown: Duration) -> PrimeDaemonConfig {
        // Never forward the test runner's credentials or provider state.
        // Copy only basic process-locale keys, and reproduce hostile inherited
        // control keys with deterministic sentinels so scrubbing is provable.
        let mut environment = BTreeMap::new();
        for name in [
            "HOME", "USER", "LOGNAME", "PATH", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL",
        ] {
            if let Some(value) = std::env::var_os(name) {
                environment.insert(OsString::from(name), value);
            }
        }
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("LC_") {
                if let Some(value) = std::env::var_os(&name) {
                    environment.insert(name, value);
                }
            } else if name.to_string_lossy().starts_with("PRIME_AGENT_INTERNAL_") {
                environment.insert(name, OsString::from(ENV_SENTINEL));
            }
        }
        environment.insert(
            OsString::from("PRIME_AGENT_INTERNAL_TEST_SENTINEL"),
            OsString::from(ENV_SENTINEL),
        );
        environment.insert(
            OsString::from("PRIME_AGENT_INTERNAL_OTHER"),
            OsString::from("must-be-scrubbed"),
        );
        environment.insert(OsString::from("RLM_DEPTH"), OsString::from("47"));
        environment.insert(OsString::from("RLM_MAX_DEPTH"), OsString::from("9"));
        environment.insert(
            OsString::from("NODE_OPTIONS"),
            OsString::from("--require=/private/injection.cjs"),
        );
        environment.insert(
            OsString::from("NODE_PATH"),
            OsString::from("/private/node-path"),
        );
        environment.insert(OsString::from("FORCE_COLOR"), OsString::from("0"));
        environment.insert(OsString::from("NO_COLOR"), OsString::from("1"));
        environment.insert(OsString::from("CLICOLOR"), OsString::from("0"));
        environment.insert(OsString::from("CLICOLOR_FORCE"), OsString::from("0"));
        environment.insert(
            OsString::from("ANTHROPIC_API_KEY"),
            OsString::from(ENV_SENTINEL),
        );
        PrimeDaemonConfig::new(&self.executable, &self.state_root, "session-fixture")
            .with_socket_root(&self.socket_root)
            .with_environment(environment)
            .with_timeouts(startup, shutdown)
    }

    pub fn control(&self) -> Value {
        serde_json::from_slice(&std::fs::read(&self.control_path).unwrap()).unwrap()
    }

    pub fn set_control(&self, value: Value) {
        write_json_atomic(&self.control_path, &value);
    }

    pub fn update_control(&self, update: impl FnOnce(&mut Value)) {
        let mut control = self.control();
        update(&mut control);
        write_json_atomic(&self.control_path, &control);
    }

    pub fn gate(&self, stage: &str) {
        self.update_control(|control| {
            let gates = control
                .as_object_mut()
                .unwrap()
                .entry("gates")
                .or_insert_with(|| json!([]));
            let gates = gates.as_array_mut().unwrap();
            if !gates.iter().any(|value| value.as_str() == Some(stage)) {
                gates.push(json!(stage));
            }
        });
    }

    pub fn release(&self, stage: &str) {
        assert!(
            stage
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
            "invalid fixture gate name"
        );
        std::fs::write(self.gate_root.join(stage), b"released").unwrap();
    }

    pub fn observations(&self) -> Vec<Value> {
        std::fs::read_to_string(&self.observation_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    pub fn observation_count(&self, kind: &str) -> usize {
        self.observations()
            .into_iter()
            .filter(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
            .count()
    }

    pub async fn wait_for_observation(&self, kind: &str) -> Value {
        self.wait_for_observation_count(kind, 1).await
    }

    pub async fn wait_for_observation_count(&self, kind: &str, count: usize) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let matching = self
                    .observations()
                    .into_iter()
                    .filter(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
                    .collect::<Vec<_>>();
                if matching.len() >= count {
                    return matching.into_iter().last().unwrap();
                }
                // The observation itself is the ordering proof. Yielding only
                // lets the real subprocess publish it; elapsed time is never a
                // correctness condition.
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fake Prime session observation {kind} count {count} arrived: {:?}",
                self.observations()
            )
        })
    }

    pub async fn wait_for_gate(&self, stage: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(value) = self.observations().into_iter().find(|value| {
                    value.get("kind").and_then(Value::as_str) == Some("gate_wait")
                        && value.get("stage").and_then(Value::as_str) == Some(stage)
                }) {
                    return value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fake Prime gate {stage} was reached: {:?}",
                self.observations()
            )
        })
    }

    pub fn latest_spawn_socket(&self) -> PathBuf {
        self.observations()
            .into_iter()
            .rev()
            .find(|value| value.get("kind").and_then(Value::as_str) == Some("daemon_spawn"))
            .and_then(|value| {
                value
                    .get("socket")
                    .and_then(Value::as_str)
                    .map(PathBuf::from)
            })
            .expect("daemon spawn observation")
    }

    pub fn process_observation_pid(&self, kind: &str) -> u32 {
        self.observations()
            .into_iter()
            .rev()
            .find(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
            .and_then(|value| value.get("pid").and_then(Value::as_u64))
            .map(|pid| pid as u32)
            .unwrap_or_else(|| panic!("missing {kind} pid observation"))
    }

    pub fn raw_host_command(&self) -> tokio::process::Command {
        use std::process::Stdio;
        let host = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/prime/session/host.mjs");
        let mut command = tokio::process::Command::new(&self.node_path);
        command
            .arg("--max-old-space-size=512")
            .arg(host)
            .env_clear()
            .env("HOME", std::env::var_os("HOME").unwrap_or_default())
            .env("USER", std::env::var_os("USER").unwrap_or_default())
            .env("LOGNAME", std::env::var_os("LOGNAME").unwrap_or_default())
            .env("PATH", runtime_path(&self.node_path))
            .env("LANG", "C.UTF-8")
            .env("NO_COLOR", "1")
            .env("FORCE_COLOR", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        command
    }

    pub fn assert_scrubbed_real_processes(&self) {
        for observation in self.observations().into_iter().filter(|value| {
            matches!(
                value.get("kind").and_then(Value::as_str),
                Some("daemon_spawn" | "public_module_import")
            )
        }) {
            let env = &observation["env"];
            assert_eq!(env["primeInternal"], json!([]), "{observation:?}");
            assert!(env["rlmDepth"].is_null(), "{observation:?}");
            if observation["kind"] == "daemon_spawn" {
                assert_eq!(env["rlmMaxDepth"], "9", "{observation:?}");
            } else {
                assert!(env["rlmMaxDepth"].is_null(), "{observation:?}");
            }
            assert_eq!(env["nodeOptions"], Value::Null, "{observation:?}");
            assert_eq!(env["nodePath"], Value::Null, "{observation:?}");
            assert_eq!(env["forceColor"], "0", "{observation:?}");
            assert_eq!(env["noColor"], "1", "{observation:?}");
            if observation["kind"] == "daemon_spawn" {
                assert_eq!(env["clicolor"], "0", "{observation:?}");
                assert_eq!(env["clicolorForce"], "0", "{observation:?}");
            } else {
                assert!(env["clicolor"].is_null(), "{observation:?}");
                assert!(env["clicolorForce"].is_null(), "{observation:?}");
            }
        }
    }

    pub fn private_sentinels(&self) -> Vec<String> {
        vec![
            ACTIVE_SESSION_SENTINEL.into(),
            SESSION_ID_SENTINEL.into(),
            SESSION_FILE_SENTINEL.into(),
            RAW_ERROR_SENTINEL.into(),
            ENV_SENTINEL.into(),
            self.package_root.to_string_lossy().into_owned(),
            self.public_entry.to_string_lossy().into_owned(),
            self.observation_path.to_string_lossy().into_owned(),
            self.control_path.to_string_lossy().into_owned(),
            self.descriptor_path.to_string_lossy().into_owned(),
        ]
    }

    pub fn assert_private_rendering(&self, rendered: &str) {
        let mut sentinels = self.private_sentinels();
        if let Some(socket) = self.observations().into_iter().find_map(|value| {
            value
                .get("socket")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }) {
            sentinels.push(socket);
        }
        for private in sentinels {
            assert!(
                !rendered.contains(&private),
                "private Prime sentinel leaked in {rendered:?}"
            );
        }
    }
}

impl Drop for PrimeSessionFixture {
    fn drop(&mut self) {
        // Panic-safe fixture backstop. Product assertions still prove normal
        // worker/descriptor settlement before this runs; this only prevents a
        // failed test from leaking fake detached groups into later cases.
        for observation in self.observations() {
            if let Some(pid) = observation.get("workerPid").and_then(Value::as_u64) {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
            if matches!(
                observation.get("kind").and_then(Value::as_str),
                Some("worker_descendant" | "host_descendant")
            ) && let Some(pid) = observation.get("pid").and_then(Value::as_u64)
            {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
        }
        let _ = std::fs::remove_file(&self.descriptor_path);
    }
}

pub fn process_exists(pid: u32) -> bool {
    if unsafe { libc::kill(pid as i32, 0) } != 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        && stat
            .rsplit_once(") ")
            .is_some_and(|(_, tail)| tail.starts_with('Z'))
    {
        return false;
    }
    true
}

pub async fn wait_for_process_exit(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_exists(pid) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fake Prime process {pid} exited"));
}

pub async fn wait_for_path_absent(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("private Prime path was removed");
}

fn write_json_atomic(path: &Path, value: &Value) {
    let next = path.with_extension("next");
    std::fs::write(&next, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(next, path).unwrap();
}

fn runtime_path(node: &Path) -> OsString {
    let mut values = node
        .parent()
        .map(Path::to_path_buf)
        .into_iter()
        .collect::<Vec<_>>();
    if let Some(path) = std::env::var_os("PATH") {
        values.extend(std::env::split_paths(&path));
    }
    std::env::join_paths(values).unwrap()
}

fn js_string(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

fn find_node() -> PathBuf {
    for directory in std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default()
    {
        let candidate = directory.join("node");
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("node is required for Prime session fixture tests");
}

fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn symlink_relative(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

const PUBLIC_MODULE: &str = r###"
import net from "node:net";
import fs, { appendFileSync, existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const OBSERVATION = __OBSERVATION_PATH__;
const CONTROL = __CONTROL_PATH__;
const GATE_ROOT = __GATE_ROOT__;
const ACTIVE_SESSION_ID = __ACTIVE_SESSION_ID__;
const SESSION_ID = __SESSION_ID__;
const SESSION_FILE_NAME = __SESSION_FILE_NAME__;
const RAW_ERROR = __RAW_ERROR__;
const record = (value) => appendFileSync(OBSERVATION, `${JSON.stringify(value)}\n`);
const readControl = () => JSON.parse(readFileSync(CONTROL, "utf8"));
const role = process.execArgv.some((value) => value === "--max-old-space-size=512")
  ? "session_host"
  : "bootstrap_bridge";
const environmentObservation = () => ({
  primeInternal: Object.keys(process.env).filter((key) => key.startsWith("PRIME_AGENT_INTERNAL_")),
  rlmDepth: process.env.RLM_DEPTH ?? null,
  rlmMaxDepth: process.env.RLM_MAX_DEPTH ?? null,
  nodeOptions: process.env.NODE_OPTIONS ?? null,
  nodePath: process.env.NODE_PATH ?? null,
  forceColor: process.env.FORCE_COLOR ?? null,
  noColor: process.env.NO_COLOR ?? null,
  clicolor: process.env.CLICOLOR ?? null,
  clicolorForce: process.env.CLICOLOR_FORCE ?? null,
});
record({kind:"public_module_import", role, pid:process.pid, env:environmentObservation()});

if (role === "session_host" && readControl().hostDescendant === true) {
  const child = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{}); setInterval(()=>{},1000)"], {
    detached:false,
    stdio:"ignore",
  });
  child.unref();
  record({kind:"host_descendant", pid:child.pid});
}

function gateEnabled(stage) {
  const gates = readControl().gates;
  return Array.isArray(gates) && gates.includes(stage);
}

async function waitGate(stage) {
  if (!gateEnabled(stage)) return;
  record({kind:"gate_wait", stage, role, pid:process.pid});
  const target = path.join(GATE_ROOT, stage);
  while (!existsSync(target)) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  record({kind:"gate_release", stage, role, pid:process.pid});
}

if (role === "session_host") await waitGate("module-load");

export const VERSION = "0.8.1";
export const DAEMON_PROTOCOL_NAME = "prime-agent.daemon";
export const DAEMON_PROTOCOL_VERSION = 7;
__SDK_FEATURE_EXPORT__

function timeoutPromise(stage, timeoutMs, register) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${RAW_ERROR}:${stage}:timeout`)), timeoutMs);
    register(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

export class DaemonClient {
  constructor(socketPath, options = undefined) {
    this.socketPath = socketPath;
    this.options = options;
    this.maxInboundFrameBytes = options?.maxInboundFrameBytes ?? 128 * 1024 * 1024;
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.messageListeners = new Set();
    this.closeListeners = new Set();
    this.requestId = 0;
    this.closed = false;
    this.hello = undefined;
    record({kind:"client_constructor", role, socket:socketPath, options:options ?? null});
  }

  async connect(timeoutMs) {
    record({kind:"sdk_connect_begin", role, timeoutMs});
    await waitGate(role === "session_host" ? "connect" : "bootstrap-connect");
    if (readControl().connectFailure === true) throw new Error(`${RAW_ERROR}:connect`);
    const socket = net.createConnection(this.socketPath);
    this.socket = socket;
    socket.on("data", (chunk) => this.handleData(chunk));
    socket.on("error", (error) => this.notifyClose(error));
    socket.on("close", () => this.notifyClose(new Error(`${RAW_ERROR}:socket-closed`)));
    await timeoutPromise("connect", timeoutMs, (resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    record({kind:"sdk_connect_complete", role});
  }

  handleData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (this.buffer.length > this.maxInboundFrameBytes) {
      const error = new Error(`${RAW_ERROR}:inbound-overflow:${this.socketPath}`);
      this.socket?.destroy();
      this.rejectAll(error);
      this.notifyClose(error);
      return;
    }
    let newline;
    while ((newline = this.buffer.indexOf(0x0a)) >= 0) {
      const line = this.buffer.subarray(0, newline).toString("utf8");
      this.buffer = this.buffer.subarray(newline + 1);
      let message;
      try { message = JSON.parse(line); } catch { continue; }
      if (message?.type === "daemon_hello") this.hello = message;
      const pending = typeof message?.id === "string" ? this.pending.get(message.id) : undefined;
      if (pending) {
        this.pending.delete(message.id);
        pending.resolve(message);
        continue;
      }
      for (const listener of [...this.messageListeners]) {
        try { listener(message); } catch {}
      }
    }
  }

  async waitForHello(timeoutMs) {
    if (this.hello) return this.hello;
    return timeoutPromise("hello", timeoutMs, (resolve, reject) => {
      const offMessage = this.onMessage((message) => {
        if (message?.type === "daemon_hello") {
          offMessage();
          offClose();
          resolve(message);
        }
      });
      const offClose = this.onClose((error) => {
        offMessage();
        offClose();
        reject(error);
      });
    });
  }

  async request(command, timeoutMs) {
    await waitGate(`request-${command?.type ?? "unknown"}`);
    if (!this.socket || this.socket.destroyed) throw new Error(`${RAW_ERROR}:request-without-client`);
    const id = `sdk-${++this.requestId}`;
    record({kind:"sdk_request", role, command, timeoutMs});
    const response = timeoutPromise(`request-${command.type}`, timeoutMs, (resolve, reject) => {
      this.pending.set(id, {resolve, reject});
      this.socket.write(`${JSON.stringify({...command, id})}\n`);
    });
    return await response;
  }

  onMessage(listener) {
    this.messageListeners.add(listener);
    return () => this.messageListeners.delete(listener);
  }

  onClose(listener) {
    this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  supportsServerCapability(capability) {
    return this.hello?.serverCapabilities?.includes(capability) === true;
  }

  notifyClose(error) {
    if (this.closeNotified) return;
    this.closeNotified = true;
    this.rejectAll(error);
    for (const listener of [...this.closeListeners]) {
      try { listener(error); } catch {}
    }
  }

  rejectAll(error) {
    for (const [id, pending] of this.pending) {
      this.pending.delete(id);
      pending.reject(error);
    }
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    record({kind:"client_close", role});
    this.socket?.destroy();
    this.rejectAll(new Error(`${RAW_ERROR}:client-close`));
  }
}

function snapshotFor(activeSessionId, cwd, sessionDir) {
  const control = readControl();
  const sessionId = control.snapshotWrongSession ? `${SESSION_ID}-wrong` : SESSION_ID;
  const active = control.snapshotWrongActive ? `${activeSessionId}-wrong` : activeSessionId;
  const sessionFile = control.snapshotOutsideSessionDir
    ? path.join(path.dirname(sessionDir), SESSION_FILE_NAME)
    : path.join(sessionDir, SESSION_FILE_NAME);
  const snapshot = {
    state: {
      activeSessionId: active,
      cwd: control.snapshotWrongCwd ? path.dirname(cwd) : cwd,
      sessionDir,
      sessionFile,
      sessionId,
      isStreaming: control.snapshotStreaming === true,
      isCompacting: false,
      messageCount: control.snapshotMessageCount ?? 0,
    },
    messages: control.snapshotMessages ?? [],
  };
  if (control.snapshotStreamingMessage === true) snapshot.streamingMessage = {role:"assistant", content:[]};
  if (control.snapshotCursor !== false) {
    snapshot.lastEventCursor = {generation:"fixture-generation", sequence:0};
  }
  return snapshot;
}

function emitConfigured(connection, stage) {
  const control = readControl();
  const configured = control.connectionEvents?.[stage];
  if (!Array.isArray(configured)) return;
  for (const event of configured) connection.emit(event);
}

export class DaemonAgentConnection {
  constructor(client, activeSessionId, options = {}) {
    this.client = client;
    this.activeSessionId = activeSessionId;
    this.options = options;
    this.listeners = new Set();
    this.unsubClientMessage = client.onMessage((message) => {
      if (message?.type === "fixture_connection_event") this.emit(message.event);
    });
    this.unsubClientClose = client.onClose(() => this.emit({type:"closed", error:RAW_ERROR}));
    record({kind:"connection_constructor", activeSessionId, options});
  }

  static async attach(client, activeSessionId, options = {}) {
    record({kind:"static_attach_called", activeSessionId, options});
    const connection = new DaemonAgentConnection(client, activeSessionId, options);
    try {
      await connection.attach();
      return connection;
    } catch (error) {
      await connection.dispose();
      throw error;
    }
  }

  async attach() {
    record({kind:"connection_attach_begin", activeSessionId:this.activeSessionId});
    await waitGate("attach");
    if (readControl().attachFailure === true) throw new Error(`${RAW_ERROR}:attach`);
    const response = await this.client.request({
      type:"fixture_attach",
      activeSessionId:this.activeSessionId,
    }, this.options.snapshotTimeoutMs ?? 30000);
    if (!response?.success) throw new Error(`${RAW_ERROR}:attach-response`);
    this.cwd = response.data.cwd;
    this.sessionDir = response.data.sessionDir;
    this.initialSnapshot = snapshotFor(this.activeSessionId, this.cwd, this.sessionDir);
    emitConfigured(this, "attachBefore");
    record({kind:"connection_attach_complete", activeSessionId:this.activeSessionId});
    emitConfigured(this, "attachAfter");
  }

  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event) {
    for (const listener of [...this.listeners]) {
      try { listener(event); } catch {}
    }
  }

  async getInitialSnapshot() {
    this.snapshotCalls = (this.snapshotCalls ?? 0) + 1;
    record({kind:"snapshot_begin", activeSessionId:this.activeSessionId, call:this.snapshotCalls});
    emitConfigured(this, "snapshotBefore");
    await waitGate(`snapshot-${this.snapshotCalls}`);
    await waitGate("snapshot");
    if (readControl().snapshotFailure === true) throw new Error(`${RAW_ERROR}:snapshot`);
    record({kind:"snapshot_complete", activeSessionId:this.activeSessionId});
    emitConfigured(this, "snapshotAfter");
    return this.initialSnapshot;
  }

  async dispose() {
    record({kind:"connection_dispose_begin", activeSessionId:this.activeSessionId});
    await waitGate("dispose");
    if (this.options.ownedSession === true) {
      await this.client.request({
        type:"complete_owned_session",
        activeSessionId:this.activeSessionId,
      }, this.options.snapshotTimeoutMs ?? 30000).catch(() => undefined);
    }
    this.unsubClientMessage?.();
    this.unsubClientClose?.();
    record({kind:"connection_dispose_complete", activeSessionId:this.activeSessionId});
  }
}
"###;

const FAKE_DAEMON: &str = r###"#!/usr/bin/env node
import fs, { appendFileSync, existsSync, readFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";

const OBSERVATION = __OBSERVATION_PATH__;
const CONTROL = __CONTROL_PATH__;
const GATE_ROOT = __GATE_ROOT__;
const DESCRIPTOR = __DESCRIPTOR_PATH__;
const CAPABILITIES = __CAPABILITIES__;
const ACTIVE_SESSION_ID = __ACTIVE_SESSION_ID__;
const SESSION_ID = __SESSION_ID__;
const SESSION_FILE_NAME = __SESSION_FILE_NAME__;
const RAW_ERROR = __RAW_ERROR__;
const record = (value) => appendFileSync(OBSERVATION, `${JSON.stringify(value)}\n`);
const readControl = () => JSON.parse(readFileSync(CONTROL, "utf8"));
const args = process.argv.slice(2);
const valueAfter = (name) => args[args.indexOf(name) + 1];
const socketPath = valueAfter("--daemon-socket");
const daemonSessionDir = valueAfter("--session-dir");
const environmentObservation = () => ({
  primeInternal: Object.keys(process.env).filter((key) => key.startsWith("PRIME_AGENT_INTERNAL_")),
  rlmDepth: process.env.RLM_DEPTH ?? null,
  rlmMaxDepth: process.env.RLM_MAX_DEPTH ?? null,
  nodeOptions: process.env.NODE_OPTIONS ?? null,
  nodePath: process.env.NODE_PATH ?? null,
  forceColor: process.env.FORCE_COLOR ?? null,
  noColor: process.env.NO_COLOR ?? null,
  clicolor: process.env.CLICOLOR ?? null,
  clicolorForce: process.env.CLICOLOR_FORCE ?? null,
});
record({
  kind:"daemon_spawn",
  pid:process.pid,
  argv:args,
  socket:socketPath,
  daemonSessionDir,
  env:environmentObservation(),
});

function gateEnabled(stage) {
  const gates = readControl().gates;
  return Array.isArray(gates) && gates.includes(stage);
}

async function waitGate(stage) {
  if (!gateEnabled(stage)) return;
  record({kind:"gate_wait", stage, role:"daemon", pid:process.pid});
  const target = path.join(GATE_ROOT, stage);
  while (!existsSync(target)) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  record({kind:"gate_release", stage, role:"daemon", pid:process.pid});
}

let nextClient = 0;
let owned;
let shuttingDown = false;
let shutdownFlight;
let shutdownFinalizeTimer;
let shutdownFinalized = false;
const sockets = new Set();
const reverseResponses = [];

function send(socket, value, callback) {
  if (!socket.destroyed) socket.write(`${JSON.stringify(value)}\n`, callback);
}

function responseFor(request, fields) {
  return {type:"response", id:request.id, ...fields};
}

function emitConfigured(socket, stage) {
  const values = readControl().daemonEvents?.[stage];
  if (!Array.isArray(values)) return;
  for (const value of values) send(socket, value);
}

async function spawnOwnedWorker(config, ownerClientId) {
  const workerSource = `
    const fs = require("node:fs");
    const {spawn} = require("node:child_process");
    const observation = ${JSON.stringify(OBSERVATION)};
    const record = (value) => fs.appendFileSync(observation, JSON.stringify(value) + "\\n");
    let descendant;
    if (${JSON.stringify(readControl().workerDescendant === true)}) {
      descendant = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{}); setInterval(()=>{},1000)"], {stdio:"ignore", detached:false});
      record({kind:"worker_descendant", pid:descendant.pid});
    }
    record({kind:"worker_ready", pid:process.pid});
    process.on("SIGTERM", () => {
      record({kind:"worker_signal", pid:process.pid, signal:"SIGTERM"});
      if (descendant?.pid) { try { process.kill(descendant.pid, "SIGKILL"); } catch {} }
      if (!${JSON.stringify(readControl().workerIgnoreTerm === true)}) process.exit(0);
    });
    setInterval(()=>{},1000);
  `;
  const worker = spawn(process.execPath, ["-e", workerSource], {
    detached:true,
    stdio:"ignore",
  });
  worker.unref();
  const sessionFile = path.join(config.sessionDir, SESSION_FILE_NAME);
  fs.writeFileSync(DESCRIPTOR, JSON.stringify({
    activeSessionId:ACTIVE_SESSION_ID,
    sessionId:SESSION_ID,
    sessionFile,
    ownerClientId,
  }));
  owned = {
    ownerClientId,
    activeSessionId:ACTIVE_SESSION_ID,
    sessionId:SESSION_ID,
    sessionFile,
    cwd:config.cwd,
    sessionDir:config.sessionDir,
    workerPid:worker.pid,
    worker,
    status:"active",
    cleanup:undefined,
  };
  record({
    kind:"session_created",
    ownerClientId,
    activeSessionId:ACTIVE_SESSION_ID,
    sessionId:SESSION_ID,
    sessionFile,
    workerPid:worker.pid,
    descriptor:DESCRIPTOR,
  });
  return owned;
}

function workerExited(session) {
  return new Promise((resolve) => {
    try {
      process.kill(session.workerPid, 0);
    } catch {
      resolve();
      return;
    }
    session.worker.once("exit", resolve);
  });
}

async function cleanupOwned(reason, force = false) {
  const session = owned;
  if (!session || session.status === "settled") return;
  if (session.cleanup && !force) return await session.cleanup;
  const cleanup = (async () => {
    if (session.status === "active") {
      record({kind:"cleanup_state", status:"active", reason, activeSessionId:session.activeSessionId});
    }
    session.status = "stopping";
    record({kind:"cleanup_state", status:"stopping", reason, activeSessionId:session.activeSessionId});
    if (!force) await waitGate("cleanup-stopping");
    const exited = workerExited(session);
    try { process.kill(session.workerPid, "SIGTERM"); } catch {}
    if (readControl().workerIgnoreTerm === true || force) {
      try { process.kill(-session.workerPid, "SIGKILL"); } catch {
        try { process.kill(session.workerPid, "SIGKILL"); } catch {}
      }
    }
    await exited;
    try { fs.rmSync(DESCRIPTOR, {force:true}); } catch {}
    session.status = "settled";
    record({
      kind:"cleanup_state",
      status:"settled",
      reason,
      activeSessionId:session.activeSessionId,
      workerAbsent:true,
      descriptorAbsent:!existsSync(DESCRIPTOR),
    });
  })();
  session.cleanup = cleanup;
  return await cleanup;
}

function sessionSummary(session) {
  const control = readControl();
  return {
    id:session.activeSessionId,
    lifecycle:control.createLifecycle ?? "draft",
    activity:control.createActivity ?? "idle",
    isSessionActive:control.createIsSessionActive ?? false,
    activeSessionId:control.createWrongActive ? `${session.activeSessionId}-wrong` : session.activeSessionId,
    sessionId:control.createWrongSession ? `${session.sessionId}-wrong` : session.sessionId,
    sessionFile:session.sessionFile,
    cwd:control.createWrongCwd ? path.dirname(session.cwd) : session.cwd,
    isStreaming:control.createStreaming ?? false,
    isCompacting:false,
    workerState:control.createWorkerState ?? "ready",
    runtimeKind:control.createRuntimeKind ?? "top-level",
    attachedClients:0,
    messageCount:control.createMessageCount ?? 0,
    sessionActions:{queued:[], running:[]},
  };
}

async function create(request, socket, client) {
  record({kind:"create_received", clientId:client.id, request});
  await waitGate("create-admission");
  if (owned && owned.status !== "settled") {
    send(socket, responseFor(request, {command:"create", success:false, error:RAW_ERROR}));
    return;
  }
  const session = await spawnOwnedWorker(request.config, client.id);
  await waitGate("create-response");
  const control = readControl();
  if (control.createAdmissionUnknown === true) {
    record({kind:"create_response_suppressed", clientId:client.id});
    socket.destroy();
    return;
  }
  emitConfigured(socket, "createBeforeResponse");
  const response = control.createMalformedResponse === true
    ? {type:"response", id:request.id, command:"create", success:true, data:{raw:RAW_ERROR}}
    : responseFor(request, {command:"create", success:true, data:sessionSummary(session)});
  if (control.reverseResponses === true) {
    reverseResponses.push([socket, response]);
    if (reverseResponses.length >= 2) {
      for (const [queuedSocket, queuedResponse] of reverseResponses.splice(0).reverse()) {
        send(queuedSocket, queuedResponse);
      }
    }
  } else {
    send(socket, response);
  }
  emitConfigured(socket, "createAfterResponse");
  record({kind:"create_response_sent", clientId:client.id});
}

async function attach(request, socket, client) {
  record({kind:"attach_received", clientId:client.id, request});
  const success = owned && owned.ownerClientId === client.id && owned.activeSessionId === request.activeSessionId;
  if (!success) {
    send(socket, responseFor(request, {command:"fixture_attach", success:false, error:RAW_ERROR}));
    return;
  }
  send(socket, responseFor(request, {
    command:"fixture_attach",
    success:true,
    data:{cwd:owned.cwd, sessionDir:owned.sessionDir},
  }));
}

async function complete(request, socket, client) {
  record({kind:"complete_received", clientId:client.id, request});
  const control = readControl();
  if (!owned || owned.activeSessionId !== request.activeSessionId) {
    send(socket, responseFor(request, {command:"complete_owned_session", success:false, error:RAW_ERROR}));
    return;
  }
  if (owned.ownerClientId !== client.id) {
    record({kind:"nonowner_complete_rejected", clientId:client.id});
    send(socket, responseFor(request, {command:"complete_owned_session", success:false, error:RAW_ERROR}));
    return;
  }
  await waitGate("complete");
  await cleanupOwned("explicit-complete");
  record({kind:"complete_cleanup_validated", clientId:client.id});
  if (control.completeResponse === "stuck") {
    record({kind:"complete_response_stuck", clientId:client.id});
    return;
  }
  if (control.completeResponse === "malformed") {
    send(socket, {type:"response", id:request.id, command:"complete_owned_session", success:true, data:RAW_ERROR});
    return;
  }
  if (control.completeResponse === "failure") {
    send(socket, responseFor(request, {command:"complete_owned_session", success:false, error:RAW_ERROR}));
    return;
  }
  send(socket, responseFor(request, {
    command:control.completeResponse === "wrong-command" ? "create" : "complete_owned_session",
    success:true,
  }));
  record({kind:"complete_response_sent", clientId:client.id});
}

async function cleanupStatus(request, socket, client) {
  const control = readControl();
  const status = !owned ? "settled" : owned.status;
  record({
    kind:"cleanup_status_query",
    clientId:client.id,
    ownerClientId:owned?.ownerClientId ?? null,
    activeSessionId:request.activeSessionId,
    status,
    workerPresent:owned ? (() => { try { process.kill(owned.workerPid, 0); return true; } catch { return false; } })() : false,
    descriptorPresent:existsSync(DESCRIPTOR),
  });
  await waitGate("cleanup-status");
  if (control.cleanupStatus === "stuck") {
    record({kind:"cleanup_status_stuck", clientId:client.id});
    await new Promise(() => {});
  }
  if (control.cleanupStatus === "malformed") {
    send(socket, responseFor(request, {command:request.type, success:true, data:{status:"raw-unknown", raw:RAW_ERROR}}));
    return;
  }
  if (request.activeSessionId !== ACTIVE_SESSION_ID) {
    send(socket, responseFor(request, {command:request.type, success:false, error:RAW_ERROR}));
    return;
  }
  send(socket, responseFor(request, {command:request.type, success:true, data:{status}}));
}

function finalizeShutdown() {
  if (shutdownFinalized) return;
  shutdownFinalized = true;
  if (shutdownFinalizeTimer) clearTimeout(shutdownFinalizeTimer);
  try { server.close(); } catch {}
  for (const peer of sockets) peer.destroy();
  try { fs.rmSync(socketPath, {force:true}); } catch {}
  process.exit(0);
}

function scheduleShutdownFinalizer() {
  if (shutdownFinalized || shutdownFinalizeTimer) return;
  // Response callbacks finish normal/live requests immediately. This finite
  // fallback owns completion when the initiating channel was cancelled.
  shutdownFinalizeTimer = setTimeout(finalizeShutdown, 200);
}

async function runShutdownFlight(firstClientId) {
  record({kind:"daemon_shutdown_begin", clientId:firstClientId});
  await waitGate("daemon-shutdown");
  await cleanupOwned("daemon-shutdown-drain", true);
  record({
    kind:"daemon_shutdown_drained",
    workerAbsent:!owned || owned.status === "settled",
    descriptorAbsent:!existsSync(DESCRIPTOR),
  });
}

async function shutdown(request, socket, client) {
  if (!shutdownFlight) {
    shuttingDown = true;
    shutdownFlight = runShutdownFlight(client.id);
  } else {
    record({kind:"daemon_shutdown_join", clientId:client.id});
  }
  await shutdownFlight;
  if (!socket.destroyed) {
    try {
      socket.write(
        `${JSON.stringify(responseFor(request, {command:"shutdown", success:true}))}\n`,
        () => finalizeShutdown(),
      );
      record({kind:"daemon_shutdown_response", clientId:client.id});
    } catch {}
  }
  scheduleShutdownFinalizer();
}

async function handle(request, socket, client) {
  record({kind:"daemon_request", clientId:client.id, request});
  switch (request.type) {
    case "create": return await create(request, socket, client);
    case "fixture_attach": return await attach(request, socket, client);
    case "complete_owned_session": return await complete(request, socket, client);
    case "get_owned_session_cleanup":
    case "owned_session_cleanup_status":
    case "cleanup_status":
    case "cleanup-status": return await cleanupStatus(request, socket, client);
    case "shutdown": return await shutdown(request, socket, client);
    default:
      send(socket, responseFor(request, {command:request.type ?? "unknown", success:false, error:RAW_ERROR}));
  }
}

try { fs.rmSync(socketPath, {force:true}); } catch {}
const server = net.createServer((socket) => {
  const client = {id:`client-${++nextClient}`};
  sockets.add(socket);
  record({kind:"daemon_client_connected", clientId:client.id});
  send(socket, {
    type:"daemon_hello",
    socketPath,
    protocol:{name:"prime-agent.daemon", version:7},
    appVersion:"0.8.1",
    serverCapabilities:CAPABILITIES,
  });
  let buffer = "";
  socket.setEncoding("utf8");
  socket.on("data", (chunk) => {
    buffer += chunk;
    let newline;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      let request;
      try { request = JSON.parse(line); } catch { socket.destroy(); continue; }
      void handle(request, socket, client).catch((error) => {
        record({kind:"daemon_handler_error", clientId:client.id, error:String(error)});
        socket.destroy();
      });
    }
  });
  socket.on("error", () => {});
  socket.on("close", () => {
    sockets.delete(socket);
    record({kind:"daemon_client_disconnected", clientId:client.id});
    if (!shuttingDown && owned?.ownerClientId === client.id && owned.status !== "settled") {
      record({kind:"owner_disconnect_cleanup_begin", clientId:client.id, gates:readControl().gates ?? null});
      void (async () => {
        await waitGate("owner-disconnect-cleanup");
        await cleanupOwned("owner-disconnect");
      })().catch((error) => {
        record({kind:"owner_disconnect_cleanup_error", error:String(error)});
      });
    }
  });
});

process.on("SIGTERM", () => {
  void cleanupOwned("daemon-sigterm", true).finally(() => process.exit(143));
});

server.listen(socketPath, () => {
  try { fs.chmodSync(socketPath, 0o600); } catch {}
  record({kind:"daemon_listening", pid:process.pid});
});
"###;
