use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use zeron_harness::prime::PrimeDaemonConfig;

pub const BASELINE_CAPABILITIES: &[&str] = &[
    "attach_snapshot",
    "event_sequence",
    "client_owned_sessions",
    "extension_ui",
    "session_input_admission",
    "prompt_admission_cancellation",
];

pub struct PrimeFixtureBuilder {
    package_name: String,
    manifest_version: String,
    public_version: String,
    public_protocol: u64,
    include_agent_connection: bool,
    bridge_descendant: bool,
    declared_bin: String,
    root_export: Value,
    control: Value,
}

impl Default for PrimeFixtureBuilder {
    fn default() -> Self {
        Self {
            package_name: "prime-agent".into(),
            manifest_version: "0.8.1".into(),
            public_version: "0.8.1".into(),
            public_protocol: 7,
            include_agent_connection: true,
            bridge_descendant: false,
            declared_bin: "./dist/bundle/cli.mjs".into(),
            root_export: json!({"import":"./dist/index.mjs"}),
            control: json!({
                "mode":"normal",
                "protocolName":"prime-agent.daemon",
                "protocolVersion":7,
                "appVersion":"0.8.1",
                "capabilities": BASELINE_CAPABILITIES,
            }),
        }
    }
}

impl PrimeFixtureBuilder {
    pub fn package_name(mut self, value: &str) -> Self {
        self.package_name = value.into();
        self
    }

    pub fn public_version(mut self, value: &str) -> Self {
        self.public_version = value.into();
        self
    }

    pub fn public_protocol(mut self, value: u64) -> Self {
        self.public_protocol = value;
        self
    }

    pub fn without_agent_connection(mut self) -> Self {
        self.include_agent_connection = false;
        self
    }

    pub fn with_bridge_descendant(mut self) -> Self {
        self.bridge_descendant = true;
        self
    }

    pub fn declared_bin(mut self, value: &str) -> Self {
        self.declared_bin = value.into();
        self
    }

    pub fn root_export(mut self, value: Value) -> Self {
        self.root_export = value;
        self
    }

    pub fn control(mut self, value: Value) -> Self {
        self.control = value;
        self
    }

    pub fn build(self) -> PrimeFixture {
        PrimeFixture::build(self)
    }
}

pub struct PrimeFixture {
    _temp: TempDir,
    _socket_temp: TempDir,
    pub executable: PathBuf,
    cli_path: PathBuf,
    node_path: PathBuf,
    control_path: PathBuf,
    pub package_root: PathBuf,
    pub state_root: PathBuf,
    pub socket_root: PathBuf,
    pub observation_path: PathBuf,
    pub trap_path: PathBuf,
    pub private_secret: String,
}

impl PrimeFixture {
    fn build(builder: PrimeFixtureBuilder) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let observation_path = temp.path().join("observations.jsonl");
        let package = temp
            .path()
            .join("lib")
            .join("node_modules")
            .join("prime-agent");
        let cli = package.join("dist").join("bundle").join("cli.mjs");
        let public = package.join("dist").join("index.mjs");
        let trap = package.join("dist").join("trap.mjs");
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::create_dir_all(public.parent().unwrap()).unwrap();

        let manifest = json!({
            "name": builder.package_name,
            "version": builder.manifest_version,
            "type": "module",
            "bin": {"prime-agent":builder.declared_bin},
            "main": "./dist/trap.mjs",
            "exports": {
                ".": builder.root_export,
                "./private": "./dist/trap.mjs"
            }
        });
        std::fs::write(
            package.join("package.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let connection_export = if builder.include_agent_connection {
            "export class DaemonAgentConnection { static async attach() { throw new Error('not used by bootstrap'); } }"
        } else {
            ""
        };
        let bridge_setup = if builder.bridge_descendant {
            format!(
                "const bridgeChild = spawnChild(process.execPath, ['-e', \"process.on('SIGTERM',()=>{{}}); setInterval(()=>{{}},1000)\"], {{stdio:'ignore', detached:false}}); appendFileSync({}, `${{JSON.stringify({{kind:'bridge_descendant', pid:bridgeChild.pid}})}}\\n`);",
                js_string(&observation_path.to_string_lossy())
            )
        } else {
            String::new()
        };
        let public_source = PUBLIC_MODULE
            .replace("__PUBLIC_VERSION__", &js_string(&builder.public_version))
            .replace("__PUBLIC_PROTOCOL__", &builder.public_protocol.to_string())
            .replace("__AGENT_CONNECTION_EXPORT__", connection_export)
            .replace("__BRIDGE_DESCENDANT_SETUP__", &bridge_setup);
        std::fs::write(&public, public_source).unwrap();

        let trap_path = temp.path().join("private-export-imported");
        std::fs::write(
            &trap,
            format!(
                "import fs from 'node:fs'; fs.writeFileSync({}, 'bad'); throw new Error('private export imported');\n",
                js_string(&trap_path.to_string_lossy())
            ),
        )
        .unwrap();
        std::fs::write(&cli, FAKE_CLI).unwrap();
        make_executable(&cli);

        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let executable = bin.join("prime-agent");
        symlink_relative(
            Path::new("../lib/node_modules/prime-agent/dist/bundle/cli.mjs"),
            &executable,
        );
        let node = find_node();
        symlink_relative(&node, &bin.join("node"));

        let state_root = temp.path().join("state");
        let socket_temp = tempfile::tempdir_in("/tmp").unwrap();
        std::fs::create_dir(&state_root).unwrap();
        let state_root = std::fs::canonicalize(state_root).unwrap();
        let socket_root = std::fs::canonicalize(socket_temp.path()).unwrap();
        let control_path = temp.path().join("control.json");
        std::fs::write(&control_path, serde_json::to_vec(&builder.control).unwrap()).unwrap();

        Self {
            _temp: temp,
            _socket_temp: socket_temp,
            executable,
            cli_path: cli,
            node_path: node,
            control_path,
            package_root: package,
            state_root,
            socket_root,
            observation_path,
            trap_path,
            private_secret: "DO-NOT-SURFACE-PRIME-SECRET".into(),
        }
    }

    pub fn config(&self, startup: Duration, shutdown: Duration) -> PrimeDaemonConfig {
        let mut environment = std::env::vars_os().collect::<BTreeMap<_, _>>();
        environment.insert(
            OsString::from("ZERON_PRIME_FAKE_CONTROL"),
            self._temp.path().join("control.json").into_os_string(),
        );
        environment.insert(
            OsString::from("ZERON_PRIME_FAKE_OBSERVATIONS"),
            self.observation_path.clone().into_os_string(),
        );
        environment.insert(
            OsString::from("PRIME_AGENT_INTERNAL_SECRET"),
            OsString::from(&self.private_secret),
        );
        environment.insert(
            OsString::from("PRIME_AGENT_INTERNAL_FUTURE"),
            OsString::from("drop"),
        );
        environment.insert(
            OsString::from("NODE_OPTIONS"),
            OsString::from("--require=/definitely/missing/zeron-prime-injection.cjs"),
        );
        environment.insert(
            OsString::from("NODE_PATH"),
            OsString::from("/private/node-path"),
        );
        environment.insert(
            OsString::from("GITHUB_TOKEN"),
            OsString::from(&self.private_secret),
        );
        environment.insert(
            OsString::from("AWS_SECRET_ACCESS_KEY"),
            OsString::from("private"),
        );
        environment.insert(
            OsString::from("ANTHROPIC_API_KEY"),
            OsString::from("provider-private"),
        );
        environment.insert(
            OsString::from("ACME_CUSTOM_PROVIDER_TOKEN"),
            OsString::from("extension-private"),
        );
        environment.insert(
            OsString::from("DYLD_INSERT_LIBRARIES"),
            OsString::from("/private/dylib"),
        );
        environment.insert(OsString::from("LD_PRELOAD"), OsString::from("/private/so"));
        environment.insert(OsString::from("RLM_DEPTH"), OsString::from("5"));
        environment.insert(OsString::from("RLM_MAX_DEPTH"), OsString::from("9"));
        environment.insert(
            OsString::from("ZERON_PUBLIC_TEST_VALUE"),
            OsString::from("kept"),
        );
        PrimeDaemonConfig::new(&self.executable, &self.state_root, "fixture-provider")
            .with_socket_root(&self.socket_root)
            .with_environment(environment)
            .with_timeouts(startup, shutdown)
    }

    pub fn set_control(&self, control: Value) {
        std::fs::write(&self.control_path, serde_json::to_vec(&control).unwrap()).unwrap();
    }

    pub fn observations(&self) -> Vec<Value> {
        std::fs::read_to_string(&self.observation_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    pub async fn wait_for_observation(&self, kind: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(value) = self
                    .observations()
                    .into_iter()
                    .find(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
                {
                    return value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake Prime observation arrived")
    }

    pub async fn wait_for_observation_count(&self, kind: &str, count: usize) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let matches = self
                    .observations()
                    .into_iter()
                    .filter(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
                    .collect::<Vec<_>>();
                if matches.len() >= count {
                    return matches.into_iter().last().unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake Prime observation count arrived")
    }

    pub fn external_daemon_command(
        &self,
        socket: &Path,
        session_dir: &Path,
    ) -> tokio::process::Command {
        use std::process::Stdio;
        let mut command = tokio::process::Command::new(&self.node_path);
        command
            .arg(&self.cli_path)
            .args([
                "--mode",
                "daemon",
                "--daemon-socket",
                &socket.to_string_lossy(),
                "--offline",
                "--session-dir",
                &session_dir.to_string_lossy(),
            ])
            .env_clear()
            .env("ZERON_PRIME_FAKE_CONTROL", &self.control_path)
            .env("ZERON_PRIME_FAKE_OBSERVATIONS", &self.observation_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        command
    }

    pub fn assert_private_error(&self, error: &impl std::fmt::Debug) {
        let rendered = format!("{error:?}");
        for private in [
            self._temp.path().to_string_lossy().as_ref(),
            self.executable.to_string_lossy().as_ref(),
            self.state_root.to_string_lossy().as_ref(),
            self.private_secret.as_str(),
        ] {
            assert!(
                !rendered.contains(private),
                "private value leaked: {rendered}"
            );
        }
    }
}

pub fn normal_control(capabilities: &[&str]) -> Value {
    json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":capabilities,
    })
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
        // A zombie has terminated; only its external reaper remains. Treat it
        // as exited so PID 1 behavior cannot make lifecycle tests hang.
        return false;
    }
    true
}

pub async fn wait_for_process_exit(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_exists(pid) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fake Prime process exited");
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
    panic!("node is required for Prime daemon fixture tests");
}

fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn symlink_relative(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

const PUBLIC_MODULE: &str = r#"
import net from "node:net";
import { spawn as spawnChild } from "node:child_process";
import { appendFileSync } from "node:fs";
import v8 from "node:v8";

if (v8.getHeapStatistics().heap_size_limit > 256 * 1024 * 1024) {
  throw new Error("bootstrap heap is not bounded");
}
for (const name of [
  "ANTHROPIC_API_KEY", "AWS_SECRET_ACCESS_KEY", "ACME_CUSTOM_PROVIDER_TOKEN",
]) {
  if (process.env[name] !== undefined) {
    throw new Error("provider configuration leaked into bootstrap bridge");
  }
}
__BRIDGE_DESCENDANT_SETUP__
console.log("DO-NOT-MIX-PUBLIC-MODULE-DIAGNOSTICS-INTO-CONTROL");
export const VERSION = __PUBLIC_VERSION__;
export const DAEMON_PROTOCOL_NAME = "prime-agent.daemon";
export const DAEMON_PROTOCOL_VERSION = __PUBLIC_PROTOCOL__;
__AGENT_CONNECTION_EXPORT__

function timed(promise, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout")), timeoutMs);
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

export class DaemonClient {
  constructor(socketPath) {
    this.socketPath = socketPath;
    this.buffer = "";
    this.messages = [];
    this.waiters = [];
  }

  async connect(timeoutMs) {
    this.socket = net.createConnection(this.socketPath);
    this.socket.setEncoding("utf8");
    this.socket.on("data", (chunk) => {
      this.buffer += chunk;
      let newline;
      while ((newline = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, newline);
        this.buffer = this.buffer.slice(newline + 1);
        const value = JSON.parse(line);
        const waiter = this.waiters.shift();
        if (waiter) waiter(value);
        else this.messages.push(value);
      }
    });
    await timed(new Promise((resolve, reject) => {
      this.socket.once("connect", resolve);
      this.socket.once("error", reject);
    }), timeoutMs);
  }

  next(timeoutMs) {
    if (this.messages.length) return Promise.resolve(this.messages.shift());
    return timed(new Promise((resolve) => this.waiters.push(resolve)), timeoutMs);
  }

  waitForHello(timeoutMs) {
    return this.next(timeoutMs);
  }

  async request(command, timeoutMs) {
    this.socket.write(`${JSON.stringify(command)}\n`);
    return await this.next(timeoutMs);
  }

  close() {
    this.socket?.destroy();
  }
}
"#;

const FAKE_CLI: &str = r#"#!/usr/bin/env node
import fs from "node:fs";
import net from "node:net";
import { spawn } from "node:child_process";

const control = JSON.parse(fs.readFileSync(process.env.ZERON_PRIME_FAKE_CONTROL, "utf8"));
const observation = process.env.ZERON_PRIME_FAKE_OBSERVATIONS;
const record = (value) => fs.appendFileSync(observation, `${JSON.stringify(value)}\n`);
const args = process.argv.slice(2);
const valueAfter = (name) => args[args.indexOf(name) + 1];
const socketPath = valueAfter("--daemon-socket");
const sessionDir = valueAfter("--session-dir");
let descendant;

record({
  kind: "spawn",
  pid: process.pid,
  argv: args,
  socket: socketPath,
  session: sessionDir,
  env: {
    internalKeys: Object.keys(process.env).filter((key) => key.startsWith("PRIME_AGENT_INTERNAL_")),
    rlmDepth: process.env.RLM_DEPTH ?? null,
    rlmMaxDepth: process.env.RLM_MAX_DEPTH ?? null,
    publicValue: process.env.ZERON_PUBLIC_TEST_VALUE ?? null,
    providerConfigurationPresent: [
      "ANTHROPIC_API_KEY", "AWS_SECRET_ACCESS_KEY",
    ].every((key) => process.env[key] !== undefined),
    customProviderConfigurationPresent:
      process.env.ACME_CUSTOM_PROVIDER_TOKEN !== undefined,
    forbiddenKeys: [
      "NODE_OPTIONS", "NODE_PATH", "DYLD_INSERT_LIBRARIES", "LD_PRELOAD",
    ].filter((key) => process.env[key] !== undefined),
  },
});

if (control.stderrSecret) process.stderr.write(`${control.stderrSecret}\n`);
if (control.mode === "exit_early") process.exit(control.exitCode ?? 23);

if (control.spawnDescendant) {
  descendant = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{}); setInterval(()=>{},1000)"], {
    stdio: "ignore",
    detached: false,
  });
  record({kind:"descendant", pid: descendant.pid});
}

process.on("SIGTERM", () => {
  record({kind:"signal", signal:"SIGTERM"});
  if (!control.ignoreTerm) process.exit(143);
});

if (control.mode === "never_listen") {
  setInterval(() => {}, 1000);
} else {
  try { fs.rmSync(socketPath, {force:true}); } catch {}
  const server = net.createServer((socket) => {
    socket.on("error", () => {});
    if (control.mode !== "listen_no_hello") {
      socket.write(`${JSON.stringify({
        type: "daemon_hello",
        socketPath: control.socketOverride ?? socketPath,
        protocol: {
          name: control.protocolName,
          version: control.protocolVersion,
        },
        appVersion: control.appVersion,
        schemaRevision: control.schemaRevision,
        serverCapabilities: control.capabilities,
        diagnostic: control.diagnostic,
      })}\n`);
    }
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      let newline;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        const request = JSON.parse(line);
        if (request.type === "shutdown") {
          record({kind:"shutdown"});
          if (!control.ignoreShutdown) {
            socket.write(`${JSON.stringify({type:"response", command:"shutdown", success:true})}\n`);
            server.close(() => process.exit(0));
          }
        }
      }
    });
  });
  server.listen(socketPath, () => {
    try { fs.chmodSync(socketPath, 0o600); } catch {}
    record({kind:"listening"});
  });
}
"#;
