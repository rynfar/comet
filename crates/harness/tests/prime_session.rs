#![cfg(unix)]

mod support;

use std::time::Duration;

use serde_json::{Value, json};
use support::prime_session_fixture::{
    ACTIVE_SESSION_SENTINEL, ENV_SENTINEL, PrimeSessionFixture, PrimeSessionFixtureBuilder,
    RAW_ERROR_SENTINEL, REQUIRED_SESSION_SERVER_OFFERS, SDK_FEATURE, SESSION_SERVER_CAPABILITIES,
    wait_for_path_absent, wait_for_process_exit,
};
use zeron_harness::prime::{PrimeDaemon, PrimeDaemonError, PrimeSessionConfig, PrimeSessionEvent};

const STARTUP: Duration = Duration::from_secs(3);
const SHUTDOWN: Duration = Duration::from_secs(3);
const OPERATION: Duration = Duration::from_secs(3);
const CLEANUP: Duration = Duration::from_secs(4);

fn session_config(fixture: &PrimeSessionFixture) -> PrimeSessionConfig {
    PrimeSessionConfig::new(&fixture.working_directory)
        .unwrap()
        .with_timeouts(
            OPERATION, OPERATION, OPERATION, OPERATION, OPERATION, CLEANUP,
        )
}

async fn start(fixture: &PrimeSessionFixture) -> PrimeDaemon {
    PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN))
        .await
        .expect("fake session daemon starts")
}

fn observation_index(observations: &[Value], kind: &str) -> usize {
    observations
        .iter()
        .position(|value| value.get("kind").and_then(Value::as_str) == Some(kind))
        .unwrap_or_else(|| panic!("missing {kind} observation: {observations:?}"))
}

fn session_constructor_observations(fixture: &PrimeSessionFixture) -> Vec<Value> {
    fixture
        .observations()
        .into_iter()
        .filter(|value| {
            value.get("kind").and_then(Value::as_str) == Some("client_constructor")
                && value.get("role").and_then(Value::as_str) == Some("session_host")
        })
        .collect()
}

fn assert_no_session_create(fixture: &PrimeSessionFixture) {
    assert!(session_constructor_observations(fixture).is_empty());
    assert_eq!(fixture.observation_count("create_received"), 0);
    assert_eq!(fixture.observation_count("session_created"), 0);
}

fn assert_error_private(fixture: &PrimeSessionFixture, error: &PrimeDaemonError) {
    fixture.assert_private_rendering(&format!("{error:?}"));
    fixture.assert_private_rendering(&error.to_string());
}

#[tokio::test]
async fn fresh_session_uses_exact_bounded_public_contract_and_authoritative_close_order() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("fresh client-owned session attaches");

    let receipt = lease.snapshot_receipt();
    assert_eq!(receipt.message_count(), 0);
    assert!(!receipt.is_streaming());
    assert!(receipt.has_cursor());
    fixture.assert_private_rendering(&format!("{receipt:?}"));

    let constructors = session_constructor_observations(&fixture);
    assert_eq!(constructors.len(), 1, "{constructors:?}");
    assert_eq!(
        constructors[0]["options"],
        json!({"maxInboundFrameBytes":67_108_864})
    );
    let socket = fixture.latest_spawn_socket();
    assert_eq!(constructors[0]["socket"], socket.to_string_lossy().as_ref());

    let create = fixture
        .observations()
        .into_iter()
        .find(|value| {
            value.get("kind").and_then(Value::as_str) == Some("sdk_request")
                && value["role"] == "session_host"
                && value["command"]["type"] == "create"
        })
        .expect("create request observation");
    let session_dir = create["command"]["config"]["sessionDir"].as_str().unwrap();
    assert_eq!(
        create["command"],
        json!({
            "type":"create",
            "lifecycle":"client_owned",
            "config":{
                "cwd":fixture.working_directory,
                "sessionDir":session_dir,
                "noTools":true,
                "noExtensions":true,
                "noSkills":true,
                "noPromptTemplates":true,
                "noThemes":true,
                "noContextFiles":true,
            }
        })
    );

    let connection = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "connection_constructor")
        .expect("connection constructor observation");
    assert_eq!(connection["activeSessionId"], ACTIVE_SESSION_SENTINEL);
    assert_eq!(
        connection["options"],
        json!({
            "ownedSession":true,
            "supportsExtensionUi":false,
            "closeClientOnDispose":false,
            "snapshotTimeoutMs":OPERATION.as_millis() as u64,
        })
    );
    assert_eq!(fixture.observation_count("static_attach_called"), 0);
    assert!(
        !fixture.trap_path.exists(),
        "private package export was imported"
    );

    let host_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .and_then(|value| value["pid"].as_u64())
        .unwrap() as u32;
    let daemon = lease.close().await.expect("authoritative close succeeds");
    wait_for_process_exit(host_pid).await;
    let observations = fixture.observations();
    let cleanup = observation_index(&observations, "complete_cleanup_validated");
    let dispose = observation_index(&observations, "connection_dispose_begin");
    let client_close = observations
        .iter()
        .position(|value| value["kind"] == "client_close" && value["role"] == "session_host")
        .expect("session client close observation");
    assert!(cleanup < dispose, "{observations:?}");
    assert!(dispose < client_close, "{observations:?}");
    let settled = observations
        .iter()
        .find(|value| value["kind"] == "cleanup_state" && value["status"] == "settled")
        .expect("settled observation");
    assert_eq!(settled["workerAbsent"], true);
    assert_eq!(settled["descriptorAbsent"], true);
    assert!(!fixture.descriptor_path.exists());
    // session_created stores workerPid rather than pid.
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;
    wait_for_process_exit(worker_pid).await;

    fixture.assert_scrubbed_real_processes();
    daemon
        .shutdown()
        .await
        .expect("daemon shutdown after lease");
}

#[tokio::test]
async fn bounded_ingress_feature_registry_is_explicit_frozen_and_forward_compatible() {
    let rejected = [
        PrimeSessionFixtureBuilder::default()
            .without_sdk_features()
            .build(),
        PrimeSessionFixtureBuilder::default()
            .sdk_features(json!(SDK_FEATURE), true)
            .build(),
        PrimeSessionFixtureBuilder::default()
            .sdk_features(json!([SDK_FEATURE]), false)
            .build(),
        PrimeSessionFixtureBuilder::default()
            .sdk_features(json!(["wrong_bounded_ingress_v1"]), true)
            .build(),
        PrimeSessionFixtureBuilder::default()
            .sdk_features(json!([SDK_FEATURE, SDK_FEATURE]), true)
            .build(),
    ];
    for fixture in rejected {
        let daemon = start(&fixture).await;
        let error = daemon
            .create_session(session_config(&fixture))
            .await
            .err()
            .expect("unsupported public feature proof is rejected");
        assert_error_private(&fixture, &error);
        assert_no_session_create(&fixture);
        fixture
            .wait_for_observation("daemon_shutdown_drained")
            .await;
    }

    let fixture = PrimeSessionFixtureBuilder::default()
        .sdk_features(json!(["future_reviewed_feature_v1", SDK_FEATURE]), true)
        .build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("unknown future public feature degrades locally");
    let daemon = lease.close().await.unwrap();
    daemon.shutdown().await.unwrap();
}

#[tokio::test]
async fn every_required_session_server_offer_is_checked_before_create() {
    for missing in REQUIRED_SESSION_SERVER_OFFERS {
        let capabilities = SESSION_SERVER_CAPABILITIES
            .iter()
            .copied()
            .filter(|candidate| candidate != missing)
            .collect::<Vec<_>>();
        let fixture = PrimeSessionFixtureBuilder::default()
            .capabilities(&capabilities)
            .build();
        match PrimeDaemon::start(fixture.config(STARTUP, SHUTDOWN)).await {
            Ok(daemon) => {
                let error = daemon
                    .create_session(session_config(&fixture))
                    .await
                    .err()
                    .expect("missing session offer is rejected");
                assert_error_private(&fixture, &error);
            }
            Err(error) => {
                // client_owned_sessions is also a foundation capability. Its
                // absence is rejected even earlier, during daemon hello.
                assert_eq!(*missing, "client_owned_sessions");
                assert_error_private(&fixture, &error);
            }
        }
        assert_no_session_create(&fixture);
    }
}

#[tokio::test]
async fn fresh_draft_is_accepted_but_live_create_summary_is_rejected_and_cleaned() {
    let fixture = PrimeSessionFixtureBuilder::default()
        .control(json!({"mode":"normal", "createLifecycle":"live"}))
        .build();
    let daemon = start(&fixture).await;
    let error = daemon
        .create_session(session_config(&fixture))
        .await
        .err()
        .expect("non-fresh live summary is rejected");
    assert_error_private(&fixture, &error);
    assert!(
        fixture
            .observations()
            .iter()
            .any(|value| value["kind"] == "complete_cleanup_validated")
    );
    assert!(
        fixture
            .observations()
            .iter()
            .any(|value| value["kind"] == "cleanup_state" && value["status"] == "settled")
    );
    assert!(!fixture.descriptor_path.exists());
}

#[tokio::test]
async fn attach_and_initial_snapshot_failures_validate_owner_completion_before_dispose() {
    for control in [
        json!({"mode":"normal", "attachFailure":true}),
        json!({"mode":"normal", "snapshotFailure":true}),
    ] {
        let fixture = PrimeSessionFixtureBuilder::default()
            .control(control)
            .build();
        let daemon = start(&fixture).await;
        let error = daemon
            .create_session(session_config(&fixture))
            .await
            .err()
            .expect("opening failure is surfaced");
        assert_error_private(&fixture, &error);

        let observations = fixture.observations();
        let validated = observation_index(&observations, "complete_cleanup_validated");
        let response = observation_index(&observations, "complete_response_sent");
        let dispose = observation_index(&observations, "connection_dispose_begin");
        assert!(
            validated < response && response < dispose,
            "{observations:?}"
        );
        assert_eq!(fixture.observation_count("static_attach_called"), 0);
        assert!(
            observations
                .iter()
                .any(|value| value["kind"] == "cleanup_state"
                    && value["status"] == "settled"
                    && value["workerAbsent"] == true
                    && value["descriptorAbsent"] == true),
            "{observations:?}"
        );
        assert!(!fixture.descriptor_path.exists());
    }
}

#[tokio::test]
async fn host_crash_with_known_selector_polls_active_stopping_settled_as_nonowner() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("session opens");
    fixture.gate("owner-disconnect-cleanup");
    fixture.gate("cleanup-stopping");

    let host_import = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .expect("session host import observation");
    let host_pid = host_import["pid"].as_u64().unwrap() as u32;
    let session = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .unwrap();
    let worker_pid = session["workerPid"].as_u64().unwrap() as u32;
    let owner_client = session["ownerClientId"].as_str().unwrap().to_owned();
    assert_eq!(unsafe { libc::kill(host_pid as i32, libc::SIGKILL) }, 0);
    fixture
        .wait_for_observation("owner_disconnect_cleanup_begin")
        .await;
    fixture.wait_for_gate("owner-disconnect-cleanup").await;

    let close = tokio::spawn(async move { lease.close().await });
    let active = fixture
        .wait_for_observation_count("cleanup_status_query", 1)
        .await;
    assert_eq!(active["status"], "active", "{active:?}");
    assert_ne!(active["clientId"], owner_client);
    assert_eq!(active["ownerClientId"], owner_client);

    fixture.release("owner-disconnect-cleanup");
    fixture.wait_for_gate("cleanup-stopping").await;
    let stopping = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = fixture.observations().into_iter().find(|value| {
                value["kind"] == "cleanup_status_query" && value["status"] == "stopping"
            }) {
                return value;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("nonowner observed stopping");
    assert_ne!(stopping["clientId"], owner_client);

    fixture.release("cleanup-stopping");
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = fixture.observations().into_iter().find(|value| {
                value["kind"] == "cleanup_status_query" && value["status"] == "settled"
            }) {
                return value;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("nonowner observed settled");
    assert_ne!(settled["clientId"], owner_client);

    let daemon = tokio::time::timeout(Duration::from_secs(6), close)
        .await
        .expect("known-selector close is bounded")
        .expect("close task did not panic")
        .expect("cleanup proof returns daemon ownership");
    wait_for_process_exit(worker_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
    let authoritative = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "cleanup_state" && value["status"] == "settled")
        .unwrap();
    assert_eq!(authoritative["workerAbsent"], true);
    assert_eq!(authoritative["descriptorAbsent"], true);
    daemon.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_or_stuck_cleanup_status_is_exact_cleanup_uncertain_and_drains_daemon() {
    for cleanup_status in ["malformed", "stuck"] {
        let fixture = PrimeSessionFixtureBuilder::default().build();
        let daemon = start(&fixture).await;
        let lease = daemon
            .create_session(
                PrimeSessionConfig::new(&fixture.working_directory)
                    .unwrap()
                    .with_timeouts(
                        OPERATION,
                        OPERATION,
                        OPERATION,
                        OPERATION,
                        Duration::from_millis(250),
                        Duration::from_secs(3),
                    ),
            )
            .await
            .unwrap();
        fixture.update_control(|control| control["cleanupStatus"] = json!(cleanup_status));
        let host_pid = fixture
            .observations()
            .into_iter()
            .find(|value| {
                value["kind"] == "public_module_import" && value["role"] == "session_host"
            })
            .and_then(|value| value["pid"].as_u64())
            .unwrap() as u32;
        let daemon_pid = fixture.process_observation_pid("daemon_spawn");
        let worker_pid = fixture
            .observations()
            .into_iter()
            .find(|value| value["kind"] == "session_created")
            .and_then(|value| value["workerPid"].as_u64())
            .unwrap() as u32;
        assert_eq!(unsafe { libc::kill(host_pid as i32, libc::SIGKILL) }, 0);

        let error = tokio::time::timeout(Duration::from_secs(7), lease.close())
            .await
            .expect("uncertain cleanup is externally bounded")
            .err()
            .expect("unverifiable cleanup fails closed");
        assert!(matches!(error, PrimeDaemonError::CleanupUncertain));
        assert_error_private(&fixture, &error);
        fixture
            .wait_for_observation("daemon_shutdown_drained")
            .await;
        wait_for_process_exit(worker_pid).await;
        wait_for_process_exit(daemon_pid).await;
        wait_for_path_absent(&fixture.descriptor_path).await;
    }
}

#[tokio::test]
async fn create_timeout_in_unknown_admission_window_is_cleanup_uncertain_and_daemon_drains() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    fixture.gate("create-response");
    let daemon = start(&fixture).await;
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let config = PrimeSessionConfig::new(&fixture.working_directory)
        .unwrap()
        .with_timeouts(
            OPERATION,
            Duration::from_millis(300),
            OPERATION,
            OPERATION,
            OPERATION,
            Duration::from_secs(45),
        );
    let result = tokio::time::timeout(Duration::from_secs(7), daemon.create_session(config))
        .await
        .expect("unknown create admission is externally bounded");
    let error = result.err().expect("unknown admission fails closed");
    assert!(
        matches!(error, PrimeDaemonError::CleanupUncertain),
        "{error:?}"
    );
    assert_error_private(&fixture, &error);
    let created = fixture.wait_for_observation("session_created").await;
    let worker_pid = created["workerPid"].as_u64().unwrap() as u32;
    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    wait_for_process_exit(worker_pid).await;
    wait_for_process_exit(daemon_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
}

#[tokio::test]
async fn cancelling_post_create_unknown_id_transfers_worker_and_daemon_to_reaper() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    fixture.gate("create-response");
    let daemon = start(&fixture).await;
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let config = PrimeSessionConfig::new(&fixture.working_directory)
        .unwrap()
        .with_timeouts(
            OPERATION,
            Duration::from_secs(30),
            OPERATION,
            OPERATION,
            OPERATION,
            Duration::from_secs(45),
        );
    let opening = tokio::spawn(async move { daemon.create_session(config).await });
    fixture.wait_for_gate("create-response").await;
    let created = fixture.wait_for_observation("session_created").await;
    let worker_pid = created["workerPid"].as_u64().unwrap() as u32;
    opening.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), opening)
        .await
        .expect("cancelled opener settles");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));

    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    wait_for_process_exit(worker_pid).await;
    wait_for_process_exit(daemon_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
}

#[test]
fn dropping_a_live_lease_without_a_tokio_runtime_uses_the_owned_reaper() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let lease = runtime.block_on(async {
        let daemon = start(&fixture).await;
        daemon
            .create_session(session_config(&fixture))
            .await
            .expect("session opens inside temporary runtime")
    });
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;
    drop(runtime);
    drop(lease);

    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(value) = fixture.observations().into_iter().find(|value| {
                    value["kind"] == "cleanup_state"
                        && value["status"] == "settled"
                        && value["workerAbsent"] == true
                        && value["descriptorAbsent"] == true
                }) {
                    return value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("out-of-runtime reaper proves native settlement");
        assert_eq!(settled["workerAbsent"], true);
        fixture
            .wait_for_observation("daemon_shutdown_drained")
            .await;
        wait_for_process_exit(worker_pid).await;
        wait_for_process_exit(daemon_pid).await;
        wait_for_path_absent(&fixture.descriptor_path).await;
    });
}

#[test]
fn dropping_a_live_lease_inside_an_immediately_stopped_runtime_keeps_cleanup_owned() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let lease = runtime.block_on(async {
        let daemon = start(&fixture).await;
        daemon
            .create_session(session_config(&fixture))
            .await
            .expect("session opens inside temporary runtime")
    });
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;

    // Drop while the runtime is current, then stop that runtime immediately.
    // The cleanup owner must live on its dedicated reaper thread rather than a
    // task that this runtime can cancel before its first poll.
    runtime.block_on(async move { drop(lease) });
    drop(runtime);

    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        fixture
            .wait_for_observation("daemon_shutdown_drained")
            .await;
        wait_for_process_exit(worker_pid).await;
        wait_for_process_exit(daemon_pid).await;
        wait_for_path_absent(&fixture.descriptor_path).await;
    });
}

#[tokio::test]
async fn unknown_and_correlated_sdk_events_are_dropped_and_closed_event_is_private() {
    use futures::FutureExt;

    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let mut lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("session opens");
    fixture.update_control(|control| {
        control["connectionEvents"] = json!({
            "snapshotBefore":[
                {
                    "type":"correlated_prompt_lifecycle",
                    "activeSessionId":ACTIVE_SESSION_SENTINEL,
                    "raw":RAW_ERROR_SENTINEL,
                },
                {
                    "type":"unknown_future_sdk_event",
                    "sessionFile":fixture.descriptor_path,
                    "environment":ENV_SENTINEL,
                }
            ]
        });
    });
    lease
        .snapshot()
        .await
        .expect("unknown SDK events are local");
    assert!(lease.next_event().now_or_never().is_none());

    fixture.update_control(|control| {
        control["connectionEvents"] = json!({
            "snapshotAfter":[{
                "type":"closed",
                "error":RAW_ERROR_SENTINEL,
                "activeSessionId":ACTIVE_SESSION_SENTINEL,
                "sessionFile":fixture.descriptor_path,
            }]
        });
    });
    if let Err(error) = lease.snapshot().await {
        assert_error_private(&fixture, &error);
    }
    let event = tokio::time::timeout(Duration::from_secs(3), lease.next_event())
        .await
        .expect("normalized close event is bounded")
        .expect("normalized close event exists");
    assert_eq!(event, PrimeSessionEvent::Closed);
    fixture.assert_private_rendering(&format!("{event:?}"));

    let daemon = lease
        .close()
        .await
        .expect("owner disconnect cleanup is authoritative");
    daemon.shutdown().await.unwrap();
}

async fn cancel_opening_at(stage: &str, expects_worker: bool) {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    fixture.gate(stage);
    let daemon = start(&fixture).await;
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let config = PrimeSessionConfig::new(&fixture.working_directory)
        .unwrap()
        .with_timeouts(
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(45),
        );
    let opening = tokio::spawn(async move { daemon.create_session(config).await });
    fixture.wait_for_gate(stage).await;
    let host_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .and_then(|value| value["pid"].as_u64())
        .unwrap() as u32;
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .map(|pid| pid as u32);
    assert_eq!(worker_pid.is_some(), expects_worker, "stage {stage}");

    opening.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), opening)
        .await
        .expect("cancelled opening settles");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));
    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    if let Some(worker_pid) = worker_pid {
        wait_for_process_exit(worker_pid).await;
        wait_for_path_absent(&fixture.descriptor_path).await;
    }
    wait_for_process_exit(host_pid).await;
    wait_for_process_exit(daemon_pid).await;
}

#[tokio::test]
async fn cancellation_at_connect_precreate_attach_and_both_opening_snapshots_reaps_every_owner() {
    for (stage, expects_worker) in [
        ("connect", false),
        ("request-create", false),
        ("attach", true),
        ("snapshot-1", true),
        ("snapshot-2", true),
    ] {
        cancel_opening_at(stage, expects_worker).await;
    }
}

#[tokio::test]
async fn cancelling_authoritative_complete_transfers_session_host_worker_and_daemon_to_reaper() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("session opens");
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let host_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .and_then(|value| value["pid"].as_u64())
        .unwrap() as u32;
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;
    fixture.gate("complete");
    let closing = tokio::spawn(async move { lease.close().await });
    fixture.wait_for_gate("complete").await;
    closing.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("cancelled close settles");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));

    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    wait_for_process_exit(worker_pid).await;
    wait_for_process_exit(host_pid).await;
    wait_for_process_exit(daemon_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
}

#[tokio::test]
async fn normal_close_reaps_session_host_and_detached_worker_descendants() {
    let fixture = PrimeSessionFixtureBuilder::default()
        .control(json!({
            "mode":"normal",
            "hostDescendant":true,
            "workerDescendant":true,
        }))
        .build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("descendant fixture opens");
    let host_descendant = fixture.wait_for_observation("host_descendant").await["pid"]
        .as_u64()
        .unwrap() as u32;
    let worker_descendant = fixture.wait_for_observation("worker_descendant").await["pid"]
        .as_u64()
        .unwrap() as u32;
    let daemon = lease.close().await.expect("descendant fixture closes");
    wait_for_process_exit(host_descendant).await;
    wait_for_process_exit(worker_descendant).await;
    daemon.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ZERON_PRIME_7238_EXECUTABLE and exact reviewed artifact"]
async fn reviewed_7238_installed_artifact_no_model_session_smoke() {
    use sha2::{Digest, Sha256};

    const EXPECTED_ARTIFACT_SHA256: &str =
        "d6065af7b7eb0bf8bb945d84618c4d06fecea66cf3d07ed75737310b447913cc";
    let executable = std::env::var_os("ZERON_PRIME_7238_EXECUTABLE")
        .expect("set ZERON_PRIME_7238_EXECUTABLE to the installed reviewed package bin");
    let artifact = std::env::var_os("ZERON_PRIME_7238_ARTIFACT")
        .expect("set ZERON_PRIME_7238_ARTIFACT to prime-agent-0.8.1.tgz");
    let artifact_bytes = std::fs::read(artifact).expect("reviewed artifact is readable");
    assert_eq!(
        format!("{:x}", Sha256::digest(&artifact_bytes)),
        EXPECTED_ARTIFACT_SHA256,
        "reviewed artifact hash changed"
    );

    let state = tempfile::tempdir().unwrap();
    let sockets = tempfile::tempdir_in("/tmp").unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let daemon = PrimeDaemon::start(
        zeron_harness::prime::PrimeDaemonConfig::new(
            executable,
            std::fs::canonicalize(state.path()).unwrap(),
            "reviewed-7238-no-model-smoke",
        )
        .with_socket_root(std::fs::canonicalize(sockets.path()).unwrap())
        .with_timeouts(Duration::from_secs(15), Duration::from_secs(10)),
    )
    .await
    .expect("reviewed Prime daemon starts");
    for offer in REQUIRED_SESSION_SERVER_OFFERS {
        assert!(daemon.server_capabilities().server_offers(offer), "{offer}");
    }
    let lease = daemon
        .create_session(PrimeSessionConfig::new(cwd.path()).unwrap().with_timeouts(
            Duration::from_secs(10),
            Duration::from_secs(15),
            Duration::from_secs(15),
            Duration::from_secs(15),
            Duration::from_secs(15),
            Duration::from_secs(45),
        ))
        .await
        .expect("reviewed fresh client_owned session attaches");
    let receipt = lease.snapshot_receipt();
    assert_eq!(receipt.message_count(), 0);
    assert!(!receipt.is_streaming());
    let rendered = format!("{receipt:?}");
    assert!(!rendered.contains("sessionFile"));
    assert!(!rendered.contains("activeSessionId"));
    let daemon = lease
        .close()
        .await
        .expect("reviewed owning completion is authoritative");
    daemon.shutdown().await.expect("reviewed daemon drains");
}

#[tokio::test]
#[ignore = "requires ZERON_PRIME_STOCK_081_EXECUTABLE"]
async fn installed_stock_081_fails_session_gate_without_creating_a_session() {
    let executable = std::env::var_os("ZERON_PRIME_STOCK_081_EXECUTABLE")
        .expect("set ZERON_PRIME_STOCK_081_EXECUTABLE to an installed stock 0.8.1 bin");
    let state = tempfile::tempdir().unwrap();
    let sockets = tempfile::tempdir_in("/tmp").unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let daemon = PrimeDaemon::start(
        zeron_harness::prime::PrimeDaemonConfig::new(
            executable,
            std::fs::canonicalize(state.path()).unwrap(),
            "stock-081-session-gate",
        )
        .with_socket_root(std::fs::canonicalize(sockets.path()).unwrap())
        .with_timeouts(Duration::from_secs(15), Duration::from_secs(10)),
    )
    .await
    .expect("stock daemon foundation remains compatible");
    let error = daemon
        .create_session(PrimeSessionConfig::new(cwd.path()).unwrap())
        .await
        .err()
        .expect("stock public SDK has no bounded-ingress feature proof");
    let rendered = format!("{error:?}\n{error}");
    assert!(!rendered.contains(&cwd.path().to_string_lossy().to_string()));
    assert!(
        matches!(
            error,
            PrimeDaemonError::Session {
                stage: "session host load",
                code: "host-rejected-request",
            }
        ),
        "stock must fail the public-root feature gate before client construction: {rendered}"
    );
}

#[tokio::test]
async fn cancelling_cleanup_status_poll_keeps_known_owner_armed_until_reaper_shutdown() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("session opens");
    fixture.gate("cleanup-status");
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let host_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .and_then(|value| value["pid"].as_u64())
        .unwrap() as u32;
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;
    assert_eq!(unsafe { libc::kill(host_pid as i32, libc::SIGKILL) }, 0);
    let closing = tokio::spawn(async move { lease.close().await });
    fixture.wait_for_gate("cleanup-status").await;
    closing.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("cancelled cleanup poll settles");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));
    fixture.release("cleanup-status");

    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    wait_for_process_exit(worker_pid).await;
    wait_for_process_exit(host_pid).await;
    wait_for_process_exit(daemon_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
}

#[tokio::test]
async fn cancelling_poison_daemon_shutdown_keeps_cleanup_armed_until_drain() {
    let fixture = PrimeSessionFixtureBuilder::default().build();
    let daemon = start(&fixture).await;
    let lease = daemon
        .create_session(session_config(&fixture))
        .await
        .expect("session opens");
    fixture.update_control(|control| control["cleanupStatus"] = json!("malformed"));
    fixture.gate("daemon-shutdown");
    let daemon_pid = fixture.process_observation_pid("daemon_spawn");
    let host_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "public_module_import" && value["role"] == "session_host")
        .and_then(|value| value["pid"].as_u64())
        .unwrap() as u32;
    let worker_pid = fixture
        .observations()
        .into_iter()
        .find(|value| value["kind"] == "session_created")
        .and_then(|value| value["workerPid"].as_u64())
        .unwrap() as u32;
    assert_eq!(unsafe { libc::kill(host_pid as i32, libc::SIGKILL) }, 0);
    let closing = tokio::spawn(async move { lease.close().await });
    fixture.wait_for_gate("daemon-shutdown").await;
    closing.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("cancelled poison shutdown settles");
    assert!(matches!(joined, Err(error) if error.is_cancelled()));
    fixture.release("daemon-shutdown");

    fixture
        .wait_for_observation("daemon_shutdown_drained")
        .await;
    wait_for_process_exit(worker_pid).await;
    wait_for_process_exit(host_pid).await;
    wait_for_process_exit(daemon_pid).await;
    wait_for_path_absent(&fixture.descriptor_path).await;
}

async fn raw_host_exchange(
    fixture: &PrimeSessionFixture,
    input: &[u8],
) -> (std::process::ExitStatus, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut child = fixture
        .raw_host_command()
        .spawn()
        .expect("raw session host starts");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        stdin.write_all(input).await.unwrap();
        stdin.flush().await.unwrap();
    })
    .await
    .expect("bounded raw control write");
    drop(stdin);
    let mut output = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        stdout.take(64 * 1024).read_to_end(&mut output),
    )
    .await
    .expect("bounded raw control read")
    .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("raw host exit is bounded")
        .unwrap();
    (status, output)
}

fn padded_control_frame(fixture: &PrimeSessionFixture, bytes: usize) -> Vec<u8> {
    let mut frame = json!({
        "v":1,
        "id":"1",
        "op":"load",
        "entry":fixture.public_entry,
        "manifestVersion":"0.8.1",
        "pad":"",
    });
    let empty = serde_json::to_vec(&frame).unwrap();
    assert!(empty.len() <= bytes);
    frame["pad"] = json!("x".repeat(bytes - empty.len()));
    let encoded = serde_json::to_vec(&frame).unwrap();
    assert_eq!(encoded.len(), bytes);
    encoded
}

#[tokio::test]
async fn raw_host_enforces_exact_16k_plus_one_no_lf_and_partial_eof() {
    use tokio::io::AsyncWriteExt;

    let fixture = PrimeSessionFixtureBuilder::default().build();
    let mut exact = padded_control_frame(&fixture, 16 * 1024);
    exact.push(b'\n');
    let (status, output) = raw_host_exchange(&fixture, &exact).await;
    assert!(!status.success());
    let response: Value = serde_json::from_slice(
        output
            .split(|byte| *byte == b'\n')
            .find(|line| !line.is_empty())
            .expect("exact-bound request produces a fixed rejection"),
    )
    .unwrap();
    assert_eq!(response["id"], "1");
    assert_eq!(response["op"], "load");
    assert_eq!(response["ok"], false);

    let oversized = vec![b' '; 16 * 1024 + 1];
    let (status, output) = raw_host_exchange(&fixture, &oversized).await;
    assert!(!status.success());
    assert!(output.is_empty());

    let mut child = fixture
        .raw_host_command()
        .spawn()
        .expect("raw no-LF host starts");
    let mut stdin = child.stdin.take().unwrap();
    let exact_no_lf = vec![b' '; 16 * 1024];
    tokio::time::timeout(Duration::from_secs(3), async {
        stdin.write_all(&exact_no_lf).await.unwrap();
        stdin.flush().await.unwrap();
    })
    .await
    .expect("exact no-LF write is bounded");
    assert!(
        child.try_wait().unwrap().is_none(),
        "exact no-LF is not a frame"
    );
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("partial EOF host exits")
        .unwrap();
    assert!(!status.success());

    let (status, output) = raw_host_exchange(&fixture, br#"{"#).await;
    assert!(!status.success());
    assert!(output.is_empty());
}

#[tokio::test]
async fn raw_host_pending_eight_is_bounded_and_close_overtakes_blocked_loads() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fixture = PrimeSessionFixtureBuilder::default().build();
    fixture.gate("module-load");
    let mut child = fixture
        .raw_host_command()
        .spawn()
        .expect("raw session host starts");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut bytes = Vec::new();
    for id in 1..=9 {
        bytes.extend_from_slice(
            &serde_json::to_vec(&json!({
                "v":1,
                "id":id.to_string(),
                "op":"load",
                "entry":fixture.public_entry,
                "manifestVersion":"0.8.1",
            }))
            .unwrap(),
        );
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(
        &serde_json::to_vec(&json!({
            "v":1,
            "id":"10",
            "op":"close",
            "timeoutMs":3000,
        }))
        .unwrap(),
    );
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(3), async {
        stdin.write_all(&bytes).await.unwrap();
        stdin.flush().await.unwrap();
    })
    .await
    .expect("pending-bound write is bounded");
    drop(stdin);

    let mut output = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        stdout.take(64 * 1024).read_to_end(&mut output),
    )
    .await
    .expect("close-priority output is bounded")
    .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("close overtakes blocked imports")
        .unwrap();
    assert!(status.success(), "{status:?}");
    let frames = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(
        frames.iter().any(|frame| {
            frame["id"] == "9"
                && frame["op"] == "load"
                && frame["ok"] == false
                && frame["code"] == "too-many-in-flight"
        }),
        "{frames:?}"
    );
    assert!(
        frames
            .iter()
            .any(|frame| { frame["id"] == "10" && frame["op"] == "close" && frame["ok"] == true }),
        "{frames:?}"
    );
    assert!(
        fixture
            .observations()
            .iter()
            .any(|value| value["kind"] == "gate_wait" && value["stage"] == "module-load")
    );
}
