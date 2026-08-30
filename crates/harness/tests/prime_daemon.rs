#![cfg(unix)]

mod support;

use std::path::Path;
use std::time::Duration;

use serde_json::json;
use support::prime_fixture::{
    BASELINE_CAPABILITIES, PrimeFixtureBuilder, normal_control, process_exists,
    wait_for_process_exit,
};
use zeron_harness::prime::{CORRELATED_PROMPT_LIFECYCLE_CAPABILITY, PrimeDaemon, PrimeDaemonError};

const STARTUP: Duration = Duration::from_secs(3);
const SHUTDOWN: Duration = Duration::from_secs(2);

fn assert_no_runtime_directories(root: &Path) {
    fn walk(path: &Path) -> bool {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .any(|entry| {
                let path = entry.path();
                entry.file_name().to_string_lossy().starts_with("run-")
                    || (path.is_dir() && walk(&path))
            })
    }
    assert!(
        !walk(root),
        "per-launch runtime directory leaked below {root:?}"
    );
}

#[test]
fn owner_lock_helper_process() {
    use std::os::fd::AsRawFd;

    let Some(lock_path) = std::env::var_os("ZERON_PRIME_LOCK_HELPER_PATH") else {
        return;
    };
    let ready_path = std::env::var_os("ZERON_PRIME_LOCK_HELPER_READY").unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    std::fs::write(ready_path, b"locked").unwrap();
    // Bounded helper lifetime. The parent normally terminates it immediately
    // after proving cross-process lock contention.
    std::thread::sleep(Duration::from_secs(5));
}

#[tokio::test]
async fn stock_public_package_negotiates_private_daemon_and_shuts_down_cleanly() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = PrimeFixtureBuilder::default().build();
    let daemon = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("stock-compatible daemon starts");
    assert_eq!(daemon.server_capabilities().protocol_version(), 7);
    assert!(
        !daemon
            .server_capabilities()
            .server_offers(CORRELATED_PROMPT_LIFECYCLE_CAPABILITY)
    );
    assert!(
        !fixture.trap_path.exists(),
        "private/main export was imported"
    );

    let spawn = fixture.wait_for_observation("spawn").await;
    let argv = spawn["argv"].as_array().unwrap();
    assert_eq!(argv.len(), 7, "{argv:?}");
    assert_eq!(argv[0], "--mode");
    assert_eq!(argv[1], "daemon");
    assert_eq!(argv[2], "--daemon-socket");
    assert_eq!(argv[4], "--offline");
    assert_eq!(argv[5], "--session-dir");
    assert!(spawn["env"]["internalKeys"].as_array().unwrap().is_empty());
    assert!(spawn["env"]["rlmDepth"].is_null());
    assert_eq!(spawn["env"]["rlmMaxDepth"], "9");
    assert_eq!(spawn["env"]["publicValue"], "kept");
    assert_eq!(spawn["env"]["providerConfigurationPresent"], true);
    assert_eq!(spawn["env"]["customProviderConfigurationPresent"], true);
    assert!(spawn["env"]["forbiddenKeys"].as_array().unwrap().is_empty());

    let socket = Path::new(spawn["socket"].as_str().unwrap());
    let session = Path::new(spawn["session"].as_str().unwrap());
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::symlink_metadata(socket).unwrap();
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.permissions().mode() & 0o077, 0);
    }
    assert_eq!(
        std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(session).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let transport = socket.parent().unwrap();
    let runtime = std::fs::read_dir(transport)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("run-")
        })
        .expect("per-launch runtime directory exists");
    assert_eq!(
        std::fs::metadata(runtime.join("bootstrap-bridge.mjs"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let pid = spawn["pid"].as_u64().unwrap() as u32;
    daemon.shutdown().await.expect("graceful shutdown");
    wait_for_process_exit(pid).await;
    let observations = fixture.observations();
    assert!(observations.iter().any(|value| value["kind"] == "shutdown"));
    assert!(
        !observations.iter().any(|value| value["kind"] == "signal"),
        "graceful shutdown unexpectedly signalled the daemon: {observations:?}"
    );
    assert!(!socket.exists(), "daemon removed the stable socket");
    assert!(transport.exists(), "stable transport identity remains");
    assert!(
        !std::fs::read_dir(transport)
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("run-")),
        "per-launch runtime directory was removed"
    );
}

#[tokio::test]
async fn stable_identity_rejects_a_concurrent_comet_owner() {
    let fixture = PrimeFixtureBuilder::default().build();
    let first = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("first owner starts");
    let error = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .err()
        .expect("second owner is rejected");
    assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
    fixture.assert_private_error(&error);
    first.shutdown().await.unwrap();
}

#[tokio::test]
async fn stable_identity_lock_excludes_a_real_second_process() {
    use std::process::Stdio;

    let fixture = PrimeFixtureBuilder::default().build();
    let first = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("first owner starts");
    let spawn = fixture.wait_for_observation("spawn").await;
    let lock_path = Path::new(spawn["socket"].as_str().unwrap())
        .parent()
        .unwrap()
        .join("owner.lock");
    first.shutdown().await.unwrap();

    let ready = fixture.state_root.join("lock-helper-ready");
    let mut helper = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owner_lock_helper_process", "--nocapture"])
        .env("ZERON_PRIME_LOCK_HELPER_PATH", &lock_path)
        .env("ZERON_PRIME_LOCK_HELPER_READY", &ready)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("lock helper became ready");

    let error = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .err()
        .expect("cross-process second owner is rejected");
    assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
    helper.start_kill().unwrap();
    tokio::time::timeout(Duration::from_secs(2), helper.wait())
        .await
        .expect("lock helper cleanup is bounded")
        .unwrap();
}

#[tokio::test]
async fn stable_socket_is_reused_across_clean_restarts() {
    let fixture = PrimeFixtureBuilder::default().build();
    let first = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("first owner starts");
    let first_spawn = fixture.wait_for_observation_count("spawn", 1).await;
    let first_socket = first_spawn["socket"].as_str().unwrap().to_owned();
    first.shutdown().await.unwrap();

    let second = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("second owner starts");
    let second_spawn = fixture.wait_for_observation_count("spawn", 2).await;
    assert_eq!(second_spawn["socket"], first_socket);
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_retires_a_compatible_daemon_left_by_a_crashed_owner() {
    let fixture = PrimeFixtureBuilder::default().build();
    let first = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("first owner starts");
    let first_spawn = fixture.wait_for_observation_count("spawn", 1).await;
    let socket = Path::new(first_spawn["socket"].as_str().unwrap()).to_path_buf();
    let session = Path::new(first_spawn["session"].as_str().unwrap()).to_path_buf();
    first.shutdown().await.unwrap();

    // This process intentionally does not take Comet's owner lock. It models
    // a native daemon that survived a hard Comet crash after the kernel
    // released the lock.
    let mut leftover = fixture
        .external_daemon_command(&socket, &session)
        .spawn()
        .expect("leftover daemon starts");
    fixture.wait_for_observation_count("spawn", 2).await;
    fixture.wait_for_observation_count("listening", 2).await;

    let replacement = match PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN)).await {
        Ok(replacement) => replacement,
        Err(error) => panic!(
            "new owner failed after leftover retirement: {error:?}; observations={:?}",
            fixture.observations()
        ),
    };
    fixture.wait_for_observation_count("spawn", 3).await;
    let status = tokio::time::timeout(Duration::from_secs(3), leftover.wait())
        .await
        .expect("leftover retirement is bounded")
        .expect("leftover wait succeeds");
    assert!(status.success(), "leftover status: {status:?}");
    replacement.shutdown().await.unwrap();
}

#[tokio::test]
async fn incompatible_live_socket_fails_closed_without_unlinking_it() {
    let fixture = PrimeFixtureBuilder::default().build();
    let first = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("first owner starts");
    let first_spawn = fixture.wait_for_observation_count("spawn", 1).await;
    let socket = Path::new(first_spawn["socket"].as_str().unwrap()).to_path_buf();
    let session = Path::new(first_spawn["session"].as_str().unwrap()).to_path_buf();
    first.shutdown().await.unwrap();

    fixture.set_control(json!({
        "mode":"normal",
        "protocolName":"foreign.incompatible.daemon",
        "protocolVersion":7,
        "appVersion":"foreign",
        "capabilities":BASELINE_CAPABILITIES,
    }));
    let mut foreign = fixture
        .external_daemon_command(&socket, &session)
        .spawn()
        .expect("foreign listener starts");
    fixture.wait_for_observation_count("listening", 2).await;

    let error = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .err()
        .expect("incompatible listener is rejected");
    fixture.assert_private_error(&error);
    assert!(socket.exists(), "live incompatible socket was not unlinked");
    assert!(
        foreign.try_wait().unwrap().is_none(),
        "foreign daemon remains live"
    );

    foreign.start_kill().unwrap();
    tokio::time::timeout(Duration::from_secs(2), foreign.wait())
        .await
        .expect("foreign cleanup is bounded")
        .unwrap();
}

#[tokio::test]
async fn optional_and_unknown_capabilities_are_recorded_only_from_hello() {
    let mut capabilities = BASELINE_CAPABILITIES.to_vec();
    capabilities.extend([
        CORRELATED_PROMPT_LIFECYCLE_CAPABILITY,
        "unknown_future_capability",
    ]);
    let mut control = normal_control(&capabilities);
    control["schemaRevision"] = json!(9999);
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("extended hello starts");
    assert!(
        daemon
            .server_capabilities()
            .server_offers(CORRELATED_PROMPT_LIFECYCLE_CAPABILITY)
    );
    assert!(
        daemon
            .server_capabilities()
            .server_offers("unknown_future_capability")
    );
    daemon.shutdown().await.unwrap();
}

#[tokio::test]
async fn package_and_public_api_mismatches_fail_before_daemon_spawn() {
    let fixtures = [
        PrimeFixtureBuilder::default()
            .package_name("not-prime-agent")
            .build(),
        PrimeFixtureBuilder::default()
            .root_export(json!("../outside.mjs"))
            .build(),
        PrimeFixtureBuilder::default()
            .public_version("0.8.2")
            .build(),
        PrimeFixtureBuilder::default().public_protocol(6).build(),
        PrimeFixtureBuilder::default()
            .without_agent_connection()
            .build(),
        PrimeFixtureBuilder::default()
            .root_export(json!(null))
            .build(),
        PrimeFixtureBuilder::default()
            .declared_bin("./dist/bundle/not-the-cli.mjs")
            .build(),
    ];
    for fixture in fixtures {
        let error = PrimeDaemon::start(fixture.config(Duration::from_secs(1), SHUTDOWN))
            .await
            .err()
            .expect("incompatible package is rejected");
        fixture.assert_private_error(&error);
        assert!(
            !fixture
                .observations()
                .iter()
                .any(|value| value["kind"] == "spawn"),
            "daemon started for incompatible package: {error:?}"
        );
    }
}

#[tokio::test]
async fn public_export_symlink_escape_is_rejected_before_spawn() {
    let fixture = PrimeFixtureBuilder::default()
        .root_export(json!("./dist/escape.mjs"))
        .build();
    let outside = fixture.state_root.join("outside.mjs");
    std::fs::write(&outside, "export const VERSION = 'bad';").unwrap();
    std::os::unix::fs::symlink(&outside, fixture.package_root.join("dist/escape.mjs")).unwrap();

    let error = PrimeDaemon::start(fixture.config(Duration::from_secs(1), SHUTDOWN))
        .await
        .err()
        .expect("escaping public export is rejected");
    fixture.assert_private_error(&error);
    assert!(fixture.observations().is_empty());
}

#[tokio::test]
async fn incompatible_hello_is_sanitized_and_reaps_the_daemon() {
    let mut capabilities = BASELINE_CAPABILITIES.to_vec();
    capabilities.pop();
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":capabilities,
        "diagnostic":"DO-NOT-SURFACE-RAW-HELLO",
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let error = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .err()
        .expect("missing baseline is rejected");
    fixture.assert_private_error(&error);
    let rendered = format!("{error:?}");
    assert!(!rendered.contains("DO-NOT-SURFACE-RAW-HELLO"));
    assert!(
        rendered.contains("prompt_admission_cancellation"),
        "{rendered}"
    );
    let spawn = fixture.wait_for_observation("spawn").await;
    wait_for_process_exit(spawn["pid"].as_u64().unwrap() as u32).await;
}

#[tokio::test]
async fn wrong_socket_hello_never_echoes_private_payloads() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":BASELINE_CAPABILITIES,
        "socketOverride":"/private/wrong/socket",
        "diagnostic":"DO-NOT-SURFACE-RAW-HELLO",
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let error = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .err()
        .expect("wrong socket is rejected");
    fixture.assert_private_error(&error);
    let rendered = format!("{error:?}");
    assert!(!rendered.contains("/private/wrong/socket"));
    assert!(!rendered.contains("DO-NOT-SURFACE-RAW-HELLO"));
}

#[tokio::test]
async fn compatible_newer_daemon_protocol_uses_the_shared_range_rule() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":8,
        "appVersion":"a-distinct-compatible-build",
        "capabilities":BASELINE_CAPABILITIES,
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("newer compatible daemon starts");
    assert_eq!(daemon.server_capabilities().protocol_version(), 8);
    daemon.shutdown().await.unwrap();
}

#[tokio::test]
async fn protocol_mismatch_and_missing_hello_are_rejected() {
    let controls = [
        json!({
            "mode":"normal",
            "protocolName":"other.daemon",
            "protocolVersion":7,
            "appVersion":"0.8.1",
            "capabilities":BASELINE_CAPABILITIES,
        }),
        json!({
            "mode":"normal",
            "protocolName":"prime-agent.daemon",
            "protocolVersion":6,
            "appVersion":"0.8.1",
            "capabilities":BASELINE_CAPABILITIES,
        }),
        json!({"mode":"listen_no_hello"}),
    ];
    for control in controls {
        let fixture = PrimeFixtureBuilder::default().control(control).build();
        let error =
            PrimeDaemon::start(fixture.config(Duration::from_secs(2), Duration::from_secs(1)))
                .await
                .err()
                .expect("incompatible hello is rejected");
        fixture.assert_private_error(&error);
        let spawn = fixture.wait_for_observation("spawn").await;
        wait_for_process_exit(spawn["pid"].as_u64().unwrap() as u32).await;
    }
}

#[tokio::test]
async fn startup_timeout_is_bounded_and_reaps_the_process() {
    let control = json!({"mode":"never_listen"});
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        PrimeDaemon::start(fixture.config(Duration::from_secs(2), SHUTDOWN)),
    )
    .await
    .expect("foundation startup is externally bounded")
    .err()
    .expect("never-listening daemon times out");
    fixture.assert_private_error(&error);
    assert!(matches!(
        error,
        PrimeDaemonError::Timeout { .. } | PrimeDaemonError::Bridge { .. }
    ));
    let spawn = fixture.wait_for_observation("spawn").await;
    wait_for_process_exit(spawn["pid"].as_u64().unwrap() as u32).await;
    assert_no_runtime_directories(&fixture.socket_root);
}

#[tokio::test]
async fn expired_shutdown_deadline_still_drops_and_reaps_the_owner() {
    let fixture = PrimeFixtureBuilder::default().build();
    let daemon =
        PrimeDaemon::start(fixture.config(Duration::from_secs(2), Duration::from_nanos(1)))
            .await
            .expect("tiny-shutdown fixture starts");
    let spawn = fixture.wait_for_observation("spawn").await;
    let pid = spawn["pid"].as_u64().unwrap() as u32;
    tokio::time::timeout(Duration::from_secs(3), daemon.shutdown())
        .await
        .expect("expired shutdown cleanup remains bounded")
        .expect_err("the graceful deadline was already exhausted");
    wait_for_process_exit(pid).await;
    assert_no_runtime_directories(&fixture.socket_root);
}

#[tokio::test]
async fn cancelled_startup_reaps_processes_and_removes_runtime_state() {
    let fixture = PrimeFixtureBuilder::default()
        .control(json!({"mode":"never_listen"}))
        .build();
    let config = fixture.config(Duration::from_secs(30), SHUTDOWN);
    let task = tokio::spawn(async move { PrimeDaemon::start(config).await });
    let spawn = fixture.wait_for_observation("spawn").await;
    let pid = spawn["pid"].as_u64().unwrap() as u32;
    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancelled startup task settled");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));
    wait_for_process_exit(pid).await;
    assert_no_runtime_directories(&fixture.socket_root);
}

#[tokio::test]
async fn cancelled_shutdown_drops_and_kills_the_daemon_owner() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":BASELINE_CAPABILITIES,
        "ignoreShutdown":true,
        "ignoreTerm":true,
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon =
        PrimeDaemon::start(fixture.config(Duration::from_secs(2), Duration::from_secs(30)))
            .await
            .expect("cancellation fixture starts");
    let spawn = fixture.wait_for_observation("spawn").await;
    let pid = spawn["pid"].as_u64().unwrap() as u32;
    let socket = Path::new(spawn["socket"].as_str().unwrap()).to_path_buf();
    let task = tokio::spawn(async move { daemon.shutdown().await });
    fixture.wait_for_observation("shutdown").await;
    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancelled shutdown task settled");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));
    wait_for_process_exit(pid).await;
    assert!(
        socket.parent().unwrap().exists(),
        "stable transport identity remains"
    );
    assert_no_runtime_directories(&fixture.socket_root);

    // A cancelled owner can leave only a stale socket inode. The next owner
    // proves liveness, removes that exact stale inode, and starts cleanly.
    let restarted = PrimeDaemon::start(fixture.config(Duration::from_secs(2), SHUTDOWN))
        .await
        .expect("restart recovers a stale socket after cancellation");
    drop(restarted);
}

#[tokio::test]
async fn early_exit_reports_only_status_not_stderr_or_paths() {
    let control = json!({
        "mode":"exit_early",
        "exitCode":23,
        "stderrSecret":"DO-NOT-SURFACE-STDERR",
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let error = PrimeDaemon::start(fixture.config(Duration::from_secs(2), SHUTDOWN))
        .await
        .err()
        .expect("early exit is rejected");
    fixture.assert_private_error(&error);
    let rendered = format!("{error:?}");
    assert!(rendered.contains("exit code 23"), "{rendered}");
    assert!(!rendered.contains("DO-NOT-SURFACE-STDERR"));
}

#[tokio::test]
async fn graceful_bridge_exit_reaps_ignoring_bridge_descendants() {
    let fixture = PrimeFixtureBuilder::default()
        .with_bridge_descendant()
        .build();
    let daemon = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("bridge descendant fixture starts");
    let descendant = fixture.wait_for_observation("bridge_descendant").await;
    let descendant_pid = descendant["pid"].as_u64().unwrap() as u32;
    daemon
        .shutdown()
        .await
        .expect("daemon and bridge stop cleanly");
    wait_for_process_exit(descendant_pid).await;
}

#[tokio::test]
async fn graceful_leader_exit_still_reaps_ignoring_daemon_descendants() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":BASELINE_CAPABILITIES,
        "spawnDescendant":true,
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon = PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("descendant fixture starts");
    let descendant = fixture.wait_for_observation("descendant").await;
    let descendant_pid = descendant["pid"].as_u64().unwrap() as u32;
    daemon.shutdown().await.expect("leader exits gracefully");
    wait_for_process_exit(descendant_pid).await;
}

#[tokio::test]
async fn term_obeying_leader_still_reaps_ignoring_daemon_descendants() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":BASELINE_CAPABILITIES,
        "ignoreShutdown":true,
        "ignoreTerm":false,
        "spawnDescendant":true,
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon =
        PrimeDaemon::start(fixture.config(Duration::from_secs(2), Duration::from_millis(350)))
            .await
            .expect("TERM fixture starts");
    let descendant = fixture.wait_for_observation("descendant").await;
    let descendant_pid = descendant["pid"].as_u64().unwrap() as u32;
    daemon
        .shutdown()
        .await
        .expect_err("public shutdown was deliberately not acknowledged");
    wait_for_process_exit(descendant_pid).await;
}

#[tokio::test]
async fn forced_shutdown_kills_the_owned_process_group() {
    let control = json!({
        "mode":"normal",
        "protocolName":"prime-agent.daemon",
        "protocolVersion":7,
        "appVersion":"0.8.1",
        "capabilities":BASELINE_CAPABILITIES,
        "ignoreShutdown":true,
        "ignoreTerm":true,
        "spawnDescendant":true,
    });
    let fixture = PrimeFixtureBuilder::default().control(control).build();
    let daemon =
        PrimeDaemon::start(fixture.config(Duration::from_secs(2), Duration::from_millis(350)))
            .await
            .expect("forced-shutdown fixture starts");
    let spawn = fixture.wait_for_observation("spawn").await;
    let descendant = fixture.wait_for_observation("descendant").await;
    let daemon_pid = spawn["pid"].as_u64().unwrap() as u32;
    let descendant_pid = descendant["pid"].as_u64().unwrap() as u32;
    assert!(process_exists(daemon_pid));
    assert!(process_exists(descendant_pid));

    tokio::time::timeout(Duration::from_secs(4), daemon.shutdown())
        .await
        .expect("forced cleanup remains bounded")
        .expect_err("native shutdown was not acknowledged");
    wait_for_process_exit(daemon_pid).await;
    wait_for_process_exit(descendant_pid).await;
    assert!(
        fixture
            .observations()
            .iter()
            .any(|value| value["kind"] == "signal" && value["signal"] == "SIGTERM")
    );
}

#[tokio::test]
#[ignore = "requires an installed stock-compatible prime-agent package"]
async fn installed_prime_discovery_load_connect_shutdown_smoke() {
    let state = tempfile::tempdir().unwrap();
    let sockets = tempfile::tempdir_in("/tmp").unwrap();
    let state = std::fs::canonicalize(state.path()).unwrap();
    let sockets = std::fs::canonicalize(sockets.path()).unwrap();
    let config = zeron_harness::prime::PrimeDaemonConfig::discover(state, "installed-smoke")
        .with_socket_root(sockets)
        .with_timeouts(Duration::from_secs(10), Duration::from_secs(5));
    let daemon = PrimeDaemon::start(config)
        .await
        .expect("installed Prime daemon starts through validated discovery");
    assert!(daemon.server_capabilities().protocol_version() >= 7);
    daemon
        .shutdown()
        .await
        .expect("installed Prime daemon stops");
}
