use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::PrimeSessionEvent;
use super::contract::{
    EVENT_QUEUE_CAPACITY, HostReply, Inbound, MAX_CONTROL_ID_DIGITS, MAX_FRAME_BYTES,
    MAX_PENDING_REQUESTS, Operation, RETIRED_ID_CAPACITY, invalid, parse_inbound,
};
use crate::prime::package::PrimePackage;
use crate::prime::paths::PrimePaths;
use crate::prime::process::{self, OwnedChild, ProcessEnvironment};
use crate::prime::{PrimeDaemonError, duration_millis};

const MAX_CONTROL_ID: u64 = 9_999_999_999_999_999;
const FORCED_REAP_TIMEOUT: Duration = Duration::from_millis(1_600);

type ReplySender = oneshot::Sender<Result<HostReply, PrimeDaemonError>>;
type ReplyReceiver = oneshot::Receiver<Result<HostReply, PrimeDaemonError>>;
type ReservedRequest = (String, ReplyReceiver, PendingGuard);

struct PendingRequest {
    operation: Operation,
    sender: ReplySender,
}

struct DemuxState {
    pending: HashMap<String, PendingRequest>,
    retired: VecDeque<(String, Operation)>,
    next_id: Option<u64>,
    poisoned: bool,
    closed_event_seen: bool,
    event_sender: Option<mpsc::Sender<PrimeSessionEvent>>,
}

struct Shared {
    state: Mutex<DemuxState>,
}

impl Shared {
    fn reserve(
        self: &Arc<Self>,
        operation: Operation,
    ) -> Result<ReservedRequest, PrimeDaemonError> {
        let (sender, receiver) = oneshot::channel();
        let id = {
            let mut state = self.state.lock().expect("session demux mutex poisoned");
            if state.poisoned {
                return Err(channel_poisoned(operation.stage()));
            }
            if state.pending.len() >= MAX_PENDING_REQUESTS {
                return Err(PrimeDaemonError::Session {
                    stage: operation.stage(),
                    code: "too-many-pending-requests",
                });
            }
            let Some(number) = state.next_id else {
                drop(state);
                self.poison();
                return Err(invalid("session control id space is exhausted"));
            };
            let id = number.to_string();
            debug_assert!(id.len() <= MAX_CONTROL_ID_DIGITS);
            state.next_id = number.checked_add(1).filter(|next| *next <= MAX_CONTROL_ID);
            state
                .pending
                .insert(id.clone(), PendingRequest { operation, sender });
            id
        };
        let guard = PendingGuard {
            shared: Arc::clone(self),
            id: Some(id.clone()),
        };
        Ok((id, receiver, guard))
    }

    fn cancel_pending(&self, id: &str) {
        let mut state = self.state.lock().expect("session demux mutex poisoned");
        let Some(pending) = state.pending.remove(id) else {
            return;
        };
        if state.poisoned {
            return;
        }
        if state.retired.len() == RETIRED_ID_CAPACITY {
            state.retired.pop_front();
        }
        state.retired.push_back((id.to_owned(), pending.operation));
    }

    fn is_poisoned(&self) -> bool {
        self.state
            .lock()
            .expect("session demux mutex poisoned")
            .poisoned
    }

    fn accept(&self, inbound: Inbound) -> Result<(), PrimeDaemonError> {
        match inbound {
            Inbound::Response {
                id,
                operation,
                reply,
            } => {
                let pending = {
                    let mut state = self.state.lock().expect("session demux mutex poisoned");
                    if state.poisoned {
                        return Err(channel_poisoned(operation.stage()));
                    }
                    if let Some(index) = state
                        .retired
                        .iter()
                        .position(|(retired_id, _)| retired_id == &id)
                    {
                        let (_, expected) = state
                            .retired
                            .remove(index)
                            .expect("the located retired response remains present");
                        if expected != operation {
                            drop(state);
                            self.poison();
                            return Err(invalid("retired session response operation mismatch"));
                        }
                        return Ok(());
                    }
                    let Some(expected) = state.pending.get(&id).map(|pending| pending.operation)
                    else {
                        drop(state);
                        self.poison();
                        return Err(invalid("unknown or duplicate session response id"));
                    };
                    if expected != operation {
                        drop(state);
                        self.poison();
                        return Err(invalid("session response operation mismatch"));
                    }
                    state
                        .pending
                        .remove(&id)
                        .expect("the validated pending response remains present")
                };
                let _ = pending.sender.send(Ok(reply));
                Ok(())
            }
            Inbound::Event(event) => {
                {
                    let mut state = self.state.lock().expect("session demux mutex poisoned");
                    if state.poisoned {
                        return Err(channel_poisoned("session event"));
                    }
                    if state.closed_event_seen {
                        drop(state);
                        self.poison();
                        return Err(invalid("duplicate session closed event"));
                    }
                    state.closed_event_seen = true;
                    let Some(sender) = state.event_sender.take() else {
                        drop(state);
                        self.poison();
                        return Err(invalid("session event queue is unavailable"));
                    };
                    if sender.try_send(event).is_err() {
                        drop(state);
                        self.poison();
                        return Err(invalid("session event queue is unavailable"));
                    }
                }
                Ok(())
            }
        }
    }

    fn poison(&self) {
        let pending = {
            let mut state = self.state.lock().expect("session demux mutex poisoned");
            if state.poisoned {
                return;
            }
            state.poisoned = true;
            state.event_sender.take();
            state
                .pending
                .drain()
                .map(|(_, pending)| pending)
                .collect::<Vec<_>>()
        };
        for pending in pending {
            let _ = pending
                .sender
                .send(Err(channel_poisoned(pending.operation.stage())));
        }
    }
}

struct PendingGuard {
    shared: Arc<Shared>,
    id: Option<String>,
}

impl PendingGuard {
    fn complete(&mut self) {
        self.id.take();
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.shared.cancel_pending(&id);
        }
    }
}

pub(super) struct SessionHost {
    shared: Arc<Shared>,
    stdin: Arc<tokio::sync::Mutex<Option<ChildStdin>>>,
    process: Arc<tokio::sync::Mutex<OwnedChild>>,
    reader: Option<JoinHandle<()>>,
    events: mpsc::Receiver<PrimeSessionEvent>,
}

impl SessionHost {
    pub(super) fn spawn(
        package: &PrimePackage,
        paths: &PrimePaths,
        environment: &ProcessEnvironment,
    ) -> Result<Self, PrimeDaemonError> {
        let process::SessionHostProcess {
            process,
            stdin,
            stdout,
        } = process::spawn_session_host(package, paths, environment)?;
        let (event_sender, events) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let shared = Arc::new(Shared {
            state: Mutex::new(DemuxState {
                pending: HashMap::with_capacity(MAX_PENDING_REQUESTS),
                retired: VecDeque::with_capacity(RETIRED_ID_CAPACITY),
                next_id: Some(1),
                poisoned: false,
                closed_event_seen: false,
                event_sender: Some(event_sender),
            }),
        });
        let process = Arc::new(tokio::sync::Mutex::new(process));
        let reader_shared = Arc::clone(&shared);
        let reader_process = Arc::clone(&process);
        let reader = tokio::spawn(async move {
            read_loop(stdout, reader_shared).await;
            reap_observed_exit(reader_process).await;
        });
        Ok(Self {
            shared,
            stdin: Arc::new(tokio::sync::Mutex::new(Some(stdin))),
            process,
            reader: Some(reader),
            events,
        })
    }

    pub(super) async fn load(
        &self,
        entry: &Path,
        manifest_version: &str,
        timeout: Duration,
    ) -> Result<(), PrimeDaemonError> {
        let reply = self
            .request(
                Operation::Load,
                json!({"entry": entry, "manifestVersion": manifest_version}),
                timeout,
            )
            .await?;
        expect_reply(reply, Operation::Load, |reply| {
            matches!(reply, HostReply::Loaded)
        })
    }

    pub(super) async fn connect(
        &self,
        socket: &Path,
        timeout: Duration,
    ) -> Result<(), PrimeDaemonError> {
        let reply = self
            .request(
                Operation::Connect,
                json!({"socket": socket, "timeoutMs": duration_millis(timeout)}),
                timeout,
            )
            .await?;
        expect_reply(reply, Operation::Connect, |reply| {
            matches!(reply, HostReply::Connected)
        })
    }

    pub(super) async fn create(
        &self,
        cwd: &Path,
        session_dir: &Path,
        timeout: Duration,
    ) -> Result<super::contract::ActiveSessionId, PrimeDaemonError> {
        match self
            .request(
                Operation::Create,
                json!({
                    "cwd": cwd,
                    "sessionDir": session_dir,
                    "timeoutMs": duration_millis(timeout),
                }),
                timeout,
            )
            .await?
        {
            HostReply::Created(id) => Ok(id),
            other => unexpected_reply(other, Operation::Create),
        }
    }

    pub(super) async fn attach(
        &self,
        timeout: Duration,
        snapshot_timeout: Duration,
    ) -> Result<(), PrimeDaemonError> {
        let reply = self
            .request(
                Operation::Attach,
                json!({
                    "timeoutMs": duration_millis(timeout),
                    "snapshotTimeoutMs": duration_millis(snapshot_timeout),
                }),
                timeout,
            )
            .await?;
        expect_reply(reply, Operation::Attach, |reply| {
            matches!(reply, HostReply::Attached)
        })
    }

    pub(super) async fn snapshot(
        &self,
        timeout: Duration,
    ) -> Result<super::PrimeSessionSnapshotReceipt, PrimeDaemonError> {
        match self
            .request(
                Operation::Snapshot,
                json!({"timeoutMs": duration_millis(timeout)}),
                timeout,
            )
            .await?
        {
            HostReply::Snapshot(receipt) => Ok(receipt),
            other => unexpected_reply(other, Operation::Snapshot),
        }
    }

    /// A successful fixed close response is the host's projection of the raw
    /// authoritative owning-cleanup completion. No other response is accepted.
    pub(super) async fn close_session(&self, timeout: Duration) -> Result<(), PrimeDaemonError> {
        let reply = self
            .request(
                Operation::Close,
                json!({"timeoutMs": duration_millis(timeout)}),
                timeout,
            )
            .await?;
        expect_reply(reply, Operation::Close, |reply| {
            matches!(reply, HostReply::Closed)
        })
    }

    pub(super) async fn next_event(&mut self) -> Option<PrimeSessionEvent> {
        self.events.recv().await
    }

    /// Close control input and reap the exact host group. If it does not exit
    /// in the remaining operation deadline, force the owned group down.
    pub(super) async fn reap(&mut self, timeout: Duration) {
        self.stdin.lock().await.take();
        {
            let mut process = self.process.lock().await;
            if process.wait(timeout).await.is_err() {
                process.terminate().await;
            }
        }
        self.finish_reader().await;
    }

    /// Force the exact session-host process group down. Owner disconnect then
    /// starts the daemon's authoritative owned-session cleanup grace.
    pub(super) async fn terminate(&mut self) {
        self.stdin.lock().await.take();
        self.process.lock().await.terminate().await;
        self.finish_reader().await;
    }

    async fn finish_reader(&mut self) {
        if let Some(mut reader) = self.reader.take()
            && tokio::time::timeout(FORCED_REAP_TIMEOUT, &mut reader)
                .await
                .is_err()
        {
            reader.abort();
        }
    }

    pub(super) fn kill_now(&mut self) {
        if let Ok(mut stdin) = self.stdin.try_lock() {
            stdin.take();
        }
        if let Ok(mut process) = self.process.try_lock() {
            process.kill_now();
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
        self.shared.poison();
    }

    async fn request(
        &self,
        operation: Operation,
        body: Value,
        timeout: Duration,
    ) -> Result<HostReply, PrimeDaemonError> {
        let (id, receiver, mut guard) = self.shared.reserve(operation)?;
        let bytes = match encode_request(id, operation, body) {
            Ok(bytes) => bytes,
            Err(error) => return Err(error),
        };
        let operation_future = async {
            {
                let mut stdin = self.stdin.lock().await;
                if self.shared.is_poisoned() {
                    return Err(channel_poisoned(operation.stage()));
                }
                let stdin = stdin.as_mut().ok_or(PrimeDaemonError::Session {
                    stage: operation.stage(),
                    code: "control-input-closed",
                })?;
                if let Err(error) = stdin.write_all(&bytes).await {
                    self.shared.poison();
                    return Err(PrimeDaemonError::io("session host write", &error));
                }
                if let Err(error) = stdin.flush().await {
                    self.shared.poison();
                    return Err(PrimeDaemonError::io("session host write", &error));
                }
            }
            receiver
                .await
                .unwrap_or_else(|_| Err(channel_poisoned(operation.stage())))
        };
        let result = tokio::time::timeout(timeout, operation_future)
            .await
            .map_err(|_| PrimeDaemonError::Timeout {
                stage: operation.stage(),
                timeout,
            })?;
        guard.complete();
        result.and_then(map_rejection)
    }
}

impl Drop for SessionHost {
    fn drop(&mut self) {
        self.kill_now();
    }
}

fn encode_request(
    id: String,
    operation: Operation,
    body: Value,
) -> Result<Vec<u8>, PrimeDaemonError> {
    let mut object = match body {
        Value::Object(object) => object,
        _ => return Err(invalid("session request body is not an object")),
    };
    object.insert("v".into(), Value::from(super::contract::CONTROL_VERSION));
    object.insert("id".into(), Value::from(id));
    object.insert("op".into(), Value::from(operation.as_str()));
    let mut bytes = serde_json::to_vec(&Value::Object(object))
        .map_err(|_| invalid("session request serialization failed"))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(PrimeDaemonError::Session {
            stage: operation.stage(),
            code: "request-exceeds-frame-bound",
        });
    }
    bytes.push(b'\n');
    Ok(bytes)
}

fn map_rejection(reply: HostReply) -> Result<HostReply, PrimeDaemonError> {
    match reply {
        HostReply::Rejected { operation } => Err(PrimeDaemonError::Session {
            stage: operation.stage(),
            // The host's bounded raw code is deliberately not projected into
            // a public error. Its operation stage is the safe classification.
            code: "host-rejected-request",
        }),
        reply => Ok(reply),
    }
}

fn expect_reply(
    reply: HostReply,
    operation: Operation,
    accepted: impl FnOnce(&HostReply) -> bool,
) -> Result<(), PrimeDaemonError> {
    if accepted(&reply) {
        Ok(())
    } else {
        unexpected_reply(reply, operation)
    }
}

fn unexpected_reply<T>(_reply: HostReply, operation: Operation) -> Result<T, PrimeDaemonError> {
    Err(invalid(match operation {
        Operation::Load => "invalid load response",
        Operation::Connect => "invalid connect response",
        Operation::Create => "invalid create response",
        Operation::Attach => "invalid attach response",
        Operation::Snapshot => "invalid snapshot response",
        Operation::Close => "invalid close response",
    }))
}

fn channel_poisoned(stage: &'static str) -> PrimeDaemonError {
    PrimeDaemonError::Session {
        stage,
        code: "control-channel-poisoned",
    }
}

async fn reap_observed_exit(process: Arc<tokio::sync::Mutex<OwnedChild>>) {
    let mut process = process.lock().await;
    match process.try_status() {
        Ok(Some(_)) | Err(_) => {}
        Ok(None) => {
            // EOF normally means the leader has exited, but waitpid status can
            // trail pipe closure briefly. Bound the child reap so an external
            // SIGKILL cannot leave a zombie until lease.close is called.
            let _ = process.wait(Duration::from_millis(250)).await;
        }
    }
}

async fn read_loop(stdout: ChildStdout, shared: Arc<Shared>) {
    let mut stdout = BufReader::new(stdout);
    loop {
        let frame = match read_bounded_frame(&mut stdout).await {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) => {
                shared.poison();
                return;
            }
        };
        let inbound = match parse_inbound(&frame) {
            Ok(inbound) => inbound,
            Err(_) => {
                shared.poison();
                return;
            }
        };
        if shared.accept(inbound).is_err() {
            return;
        }
    }
}

/// Read one LF-delimited JSON frame. The size cap applies to raw bytes before
/// LF. EOF with any partial bytes is terminal rather than an implicit frame.
async fn read_bounded_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, PrimeDaemonError>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| PrimeDaemonError::io("session host read", &error))?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(invalid("partial session frame at end of stream"))
            };
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if frame.len().saturating_add(newline) > MAX_FRAME_BYTES {
                return Err(invalid("session response exceeds the frame bound"));
            }
            frame.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
        if frame.len().saturating_add(available.len()) > MAX_FRAME_BYTES {
            return Err(invalid("session response exceeds the frame bound"));
        }
        frame.extend_from_slice(available);
        let consumed = available.len();
        reader.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> (Arc<Shared>, mpsc::Receiver<PrimeSessionEvent>) {
        let (sender, receiver) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        (
            Arc::new(Shared {
                state: Mutex::new(DemuxState {
                    pending: HashMap::new(),
                    retired: VecDeque::new(),
                    next_id: Some(1),
                    poisoned: false,
                    closed_event_seen: false,
                    event_sender: Some(sender),
                }),
            }),
            receiver,
        )
    }

    #[test]
    fn pending_is_bounded_and_reverse_responses_demultiplex() {
        let (shared, _events) = shared();
        let mut requests = Vec::new();
        for _ in 0..MAX_PENDING_REQUESTS {
            requests.push(shared.reserve(Operation::Snapshot).unwrap());
        }
        assert!(shared.reserve(Operation::Snapshot).is_err());
        for (id, _, _) in requests.iter().rev() {
            shared
                .accept(Inbound::Response {
                    id: id.clone(),
                    operation: Operation::Snapshot,
                    reply: HostReply::Snapshot(super::super::PrimeSessionSnapshotReceipt::new(
                        0, false, false,
                    )),
                })
                .unwrap();
        }
        for (_, mut receiver, mut guard) in requests {
            assert!(matches!(
                receiver.try_recv(),
                Ok(Ok(HostReply::Snapshot(_)))
            ));
            guard.complete();
        }
    }

    #[test]
    fn only_one_known_retired_late_response_is_ignored() {
        let (shared, _events) = shared();
        let (id, receiver, guard) = shared.reserve(Operation::Attach).unwrap();
        drop(receiver);
        drop(guard);
        shared
            .accept(Inbound::Response {
                id: id.clone(),
                operation: Operation::Attach,
                reply: HostReply::Attached,
            })
            .unwrap();
        assert!(
            shared
                .accept(Inbound::Response {
                    id,
                    operation: Operation::Attach,
                    reply: HostReply::Attached,
                })
                .is_err()
        );
        assert!(shared.is_poisoned());
    }

    #[test]
    fn retired_response_still_requires_its_exact_operation() {
        let (shared, _events) = shared();
        let (id, receiver, guard) = shared.reserve(Operation::Attach).unwrap();
        drop(receiver);
        drop(guard);
        assert!(
            shared
                .accept(Inbound::Response {
                    id,
                    operation: Operation::Snapshot,
                    reply: HostReply::Snapshot(super::super::PrimeSessionSnapshotReceipt::new(
                        0, false, false
                    ),),
                })
                .is_err()
        );
        assert!(shared.is_poisoned());
    }

    #[test]
    fn wrong_operation_terminally_poisons_all_waiters() {
        let (shared, _events) = shared();
        let (first_id, mut first, mut first_guard) = shared.reserve(Operation::Attach).unwrap();
        let (_second_id, mut second, mut second_guard) =
            shared.reserve(Operation::Snapshot).unwrap();
        assert!(
            shared
                .accept(Inbound::Response {
                    id: first_id,
                    operation: Operation::Close,
                    reply: HostReply::Closed,
                })
                .is_err()
        );
        assert!(matches!(first.try_recv(), Ok(Err(_))));
        assert!(matches!(second.try_recv(), Ok(Err(_))));
        first_guard.complete();
        second_guard.complete();
    }

    #[tokio::test]
    async fn frame_bound_counts_raw_bytes_before_lf_and_rejects_partial_eof() {
        let exact = vec![b' '; MAX_FRAME_BYTES];
        let mut bytes = exact.clone();
        bytes.push(b'\n');
        let mut reader = BufReader::new(bytes.as_slice());
        assert_eq!(read_bounded_frame(&mut reader).await.unwrap(), Some(exact));

        let mut oversize = vec![b'x'; MAX_FRAME_BYTES + 1];
        oversize.push(b'\n');
        let mut reader = BufReader::new(oversize.as_slice());
        assert!(read_bounded_frame(&mut reader).await.is_err());

        let partial = b"{\"v\":1}";
        let mut reader = BufReader::new(partial.as_slice());
        assert!(read_bounded_frame(&mut reader).await.is_err());
    }
}
