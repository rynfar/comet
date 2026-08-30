// Comet-owned long-lived Prime Agent session SDK host.
//
// Rust supervises this process and is the only consumer of this private JSONL
// protocol. Prime payloads, errors, paths, and identifiers are discarded here;
// only the active session handle needed by the private Rust owner may cross.

import { isAbsolute, normalize, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const CONTROL_VERSION = 1;
const MAX_CONTROL_FRAME_BYTES = 16 * 1024;
const MAX_IN_FLIGHT = 8;
const MAX_EVENT_BACKLOG = 32;
const MAX_CONTROL_ID_DIGITS = 16;
const MAX_ERROR_CODE_BYTES = 64;
const MAX_NATIVE_ID_BYTES = 256;
const MAX_NATIVE_PATH_BYTES = 4096;
const MAX_NATIVE_STRING_BYTES = 16 * 1024;
const MAX_NATIVE_TREE_DEPTH = 16;
const MAX_NATIVE_TREE_NODES = 4096;
const MAX_TIMEOUT_MS = 10 * 60 * 1000;
const LOAD_TIMEOUT_MS = 10 * 1000;
const WRITE_TIMEOUT_MS = 5 * 1000;
const DAEMON_INBOUND_FRAME_BYTES = 64 * 1024 * 1024;
const PROTOCOL_NAME = "prime-agent.daemon";
const MIN_PROTOCOL_VERSION = 7;
const SDK_FEATURE = "bounded_daemon_ingress_v1";
const REQUIRED_SERVER_CAPABILITIES = Object.freeze([
  "client_owned_sessions",
  "chunked_snapshot",
  "immutable_snapshot_transfer_v1",
  "authoritative_owned_session_cleanup_v1",
]);
const OPERATIONS = Object.freeze(new Set(["load", "connect", "create", "attach", "snapshot", "close"]));

// Keep the real control writer private before imported provider code runs.
const controlWrite = process.stdout.write.bind(process.stdout);
const mutedWrite = (_chunk, encoding, callback) => {
  const done = typeof encoding === "function" ? encoding : callback;
  if (typeof done === "function") queueMicrotask(() => done());
  return true;
};
process.stdout.write = mutedWrite;
process.stderr.write = mutedWrite;
for (const name of Object.getOwnPropertyNames(console)) {
  try {
    if (typeof console[name] === "function") console[name] = () => {};
  } catch {}
}

let api;
let client;
let openingClient;
let unsubscribeClientClose;
let connection;
let unsubscribeConnection;
let activeSessionId;
let createdSessionId;
let canonicalCwd;
let canonicalSessionDir;
let createMayBeCommitted = false;
let createState = "idle";
let attachState = "idle";
let loadState = "idle";
let connectState = "idle";
let createOperationPromise;
let cleanupPromise;
let cleanupProved = false;
let closing = false;
let inputTerminal = false;
let processEnding = false;
let unexpectedCloseEmitted = false;
let input = Buffer.alloc(0);
let inFlight = 0;
let nextControlId = 1n;

const outboundQueue = [];
let queuedResponses = 0;
let queuedEvents = 0;
let writerRunning = false;
let writerIdleWaiters = [];

class FixedTimeout extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function byteLength(value) {
  return Buffer.byteLength(value, "utf8");
}

function isPlainRecord(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasExactKeys(value, expected) {
  if (!isPlainRecord(value)) return false;
  const keys = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return keys.length === wanted.length && keys.every((key, index) => key === wanted[index]);
}

function validControlId(id) {
  return typeof id === "string" && /^[1-9][0-9]{0,15}$/.test(id);
}

function validTimeout(value) {
  return Number.isSafeInteger(value) && value > 0 && value <= MAX_TIMEOUT_MS;
}

function validBoundedString(value, maximum, allowEmpty = false) {
  return (
    typeof value === "string" &&
    (allowEmpty || value.length > 0) &&
    byteLength(value) <= maximum &&
    !value.includes("\0")
  );
}

function validNativeId(value) {
  return (
    validBoundedString(value, MAX_NATIVE_ID_BYTES) &&
    !/[\u0000-\u0020\u007f]/u.test(value)
  );
}

function validCanonicalPath(value) {
  return (
    validBoundedString(value, MAX_NATIVE_PATH_BYTES) &&
    isAbsolute(value) &&
    normalize(value) === value &&
    resolve(value) === value
  );
}

function pathIsBelow(directory, file) {
  if (!validCanonicalPath(directory) || !validCanonicalPath(file)) return false;
  const child = relative(directory, file);
  return child.length > 0 && child !== ".." && !child.startsWith(`..${process.platform === "win32" ? "\\" : "/"}`) && !isAbsolute(child);
}

function validErrorCode(code) {
  return (
    typeof code === "string" &&
    code.length > 0 &&
    byteLength(code) <= MAX_ERROR_CODE_BYTES &&
    /^[a-z0-9-]+$/.test(code)
  );
}

function boundedNativeTree(root) {
  const budget = { nodes: 0 };
  const visit = (value, depth) => {
    budget.nodes += 1;
    if (budget.nodes > MAX_NATIVE_TREE_NODES || depth > MAX_NATIVE_TREE_DEPTH) return false;
    if (value === null || typeof value === "boolean") return true;
    if (typeof value === "string") return byteLength(value) <= MAX_NATIVE_STRING_BYTES;
    if (typeof value === "number") return Number.isFinite(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER;
    if (Array.isArray(value)) {
      if (value.length > MAX_NATIVE_TREE_NODES) return false;
      return value.every((entry) => visit(entry, depth + 1));
    }
    if (!isPlainRecord(value)) return false;
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(descriptors);
    if (keys.length > 256) return false;
    for (const key of keys) {
      const descriptor = descriptors[key];
      if (!descriptor || !("value" in descriptor) || byteLength(key) > 256 || !visit(descriptor.value, depth + 1)) {
        return false;
      }
    }
    return true;
  };
  try {
    return visit(root, 0);
  } catch {
    return false;
  }
}

function validDaemonResponseEnvelope(response, command) {
  return (
    isPlainRecord(response) &&
    boundedNativeTree(response) &&
    response.type === "response" &&
    response.command === command &&
    typeof response.success === "boolean" &&
    (response.id === undefined || validNativeId(response.id))
  );
}

function validPublicApi(candidate, expectedVersion) {
  try {
    const features = candidate?.PRIME_AGENT_SDK_FEATURES;
    const clientPrototype = candidate?.DaemonClient?.prototype;
    const connectionPrototype = candidate?.DaemonAgentConnection?.prototype;
    return (
      candidate &&
      typeof candidate === "object" &&
      typeof candidate.VERSION === "string" &&
      candidate.VERSION === expectedVersion &&
      candidate.DAEMON_PROTOCOL_NAME === PROTOCOL_NAME &&
      Number.isSafeInteger(candidate.DAEMON_PROTOCOL_VERSION) &&
      candidate.DAEMON_PROTOCOL_VERSION >= MIN_PROTOCOL_VERSION &&
      Array.isArray(features) &&
      Object.isFrozen(features) &&
      features.length > 0 &&
      features.length <= 64 &&
      features.every(
        (feature, index) =>
          Object.hasOwn(features, index) &&
          typeof feature === "string" &&
          feature.length > 0 &&
          byteLength(feature) <= 128 &&
          /^[a-z0-9_.-]+$/.test(feature),
      ) &&
      new Set(features).size === features.length &&
      features.includes(SDK_FEATURE) &&
      typeof candidate.DaemonClient === "function" &&
      typeof clientPrototype?.connect === "function" &&
      typeof clientPrototype?.waitForHello === "function" &&
      typeof clientPrototype?.request === "function" &&
      typeof clientPrototype?.close === "function" &&
      typeof clientPrototype?.onMessage === "function" &&
      typeof clientPrototype?.onClose === "function" &&
      typeof clientPrototype?.supportsServerCapability === "function" &&
      typeof candidate.DaemonAgentConnection === "function" &&
      typeof connectionPrototype?.attach === "function" &&
      typeof connectionPrototype?.subscribe === "function" &&
      typeof connectionPrototype?.getInitialSnapshot === "function" &&
      typeof connectionPrototype?.dispose === "function"
    );
  } catch {
    return false;
  }
}

function validHello(hello, socket) {
  try {
    const capabilities = hello?.serverCapabilities;
    if (
      !isPlainRecord(hello) ||
      !boundedNativeTree(hello) ||
      hello.type !== "daemon_hello" ||
      hello.socketPath !== socket ||
      hello.protocol?.name !== PROTOCOL_NAME ||
      !Number.isSafeInteger(hello.protocol?.version) ||
      hello.protocol.version < MIN_PROTOCOL_VERSION ||
      !Array.isArray(capabilities) ||
      capabilities.length > 128 ||
      new Set(capabilities).size !== capabilities.length ||
      capabilities.some(
        (capability) =>
          typeof capability !== "string" ||
          byteLength(capability) > 128 ||
          !/^[a-z0-9_.-]+$/.test(capability),
      )
    ) {
      return false;
    }
    for (const required of REQUIRED_SERVER_CAPABILITIES) {
      if (!capabilities.includes(required) || clientSupportsCapability(required) !== true) return false;
    }
    return true;
  } catch {
    return false;
  }
}

function clientSupportsCapability(capability) {
  try {
    return openingClient?.supportsServerCapability(capability) === true;
  } catch {
    return false;
  }
}

function remaining(deadline) {
  return Math.max(0, deadline - Date.now());
}

function withTimeout(promise, timeoutMs, code) {
  if (!validTimeout(timeoutMs)) return Promise.reject(new FixedTimeout(code));
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new FixedTimeout(code)), timeoutMs);
    timer.unref?.();
  });
  return Promise.race([Promise.resolve(promise), timeout]).finally(() => clearTimeout(timer));
}

function quietClose(target) {
  try {
    target?.close();
  } catch {}
}

function stopInput() {
  if (inputTerminal) return;
  inputTerminal = true;
  input = Buffer.alloc(0);
  try {
    process.stdin.pause();
    process.stdin.destroy();
  } catch {}
}

function serializeOutbound(frame) {
  let line;
  try {
    line = `${JSON.stringify(frame)}\n`;
  } catch {
    return undefined;
  }
  return byteLength(line) <= MAX_CONTROL_FRAME_BYTES + 1 ? line : undefined;
}

function resolveWriterIdle() {
  if (writerRunning || outboundQueue.length > 0) return;
  const waiters = writerIdleWaiters;
  writerIdleWaiters = [];
  for (const resolveWaiter of waiters) resolveWaiter();
}

function waitForWriterIdle() {
  if (!writerRunning && outboundQueue.length === 0) return Promise.resolve();
  return new Promise((resolveWaiter) => writerIdleWaiters.push(resolveWaiter));
}

function writeOne(line) {
  return new Promise((resolveWrite, rejectWrite) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      rejectWrite(new Error("control-write-timeout"));
    }, WRITE_TIMEOUT_MS);
    timer.unref?.();
    const done = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) rejectWrite(error);
      else resolveWrite();
    };
    try {
      controlWrite(line, done);
    } catch (error) {
      done(error);
    }
  });
}

async function pumpWriter() {
  if (writerRunning) return;
  writerRunning = true;
  try {
    while (outboundQueue.length > 0) {
      const item = outboundQueue.shift();
      if (!item) break;
      try {
        await writeOne(item.line);
        item.resolve(true);
      } catch {
        item.resolve(false);
        for (const queued of outboundQueue.splice(0)) queued.resolve(false);
        void finishProcess(1);
        break;
      } finally {
        if (item.kind === "response") queuedResponses -= 1;
        else queuedEvents -= 1;
      }
    }
  } finally {
    writerRunning = false;
    resolveWriterIdle();
  }
}

function enqueueOutbound(frame, kind) {
  if (processEnding) return Promise.resolve(false);
  const line = serializeOutbound(frame);
  if (!line) return Promise.resolve(false);
  if (kind === "response") {
    if (queuedResponses >= MAX_IN_FLIGHT + 1) return Promise.resolve(false);
    queuedResponses += 1;
  } else {
    if (queuedEvents >= MAX_EVENT_BACKLOG) return Promise.resolve(false);
    queuedEvents += 1;
  }
  const completion = new Promise((resolveItem) => outboundQueue.push({ line, kind, resolve: resolveItem }));
  void pumpWriter();
  return completion;
}

function successFrame(frame, data) {
  const response = {
    v: CONTROL_VERSION,
    kind: "response",
    id: frame.id,
    op: frame.op,
    ok: true,
  };
  if (data !== undefined) response.data = data;
  return response;
}

function failureFrame(frame, code) {
  const safeCode = validErrorCode(code) ? code : "host-operation-failed";
  return {
    v: CONTROL_VERSION,
    kind: "response",
    id: frame.id,
    op: frame.op,
    ok: false,
    code: safeCode,
  };
}

async function finishProcess(exitCode) {
  if (processEnding) return;
  processEnding = true;
  stopInput();
  closing = true;
  try {
    unsubscribeConnection?.();
  } catch {}
  unsubscribeConnection = undefined;
  try {
    unsubscribeClientClose?.();
  } catch {}
  unsubscribeClientClose = undefined;
  quietClose(openingClient);
  if (client !== openingClient) quietClose(client);
  openingClient = undefined;
  client = undefined;
  await withTimeout(waitForWriterIdle(), WRITE_TIMEOUT_MS, "control-write-timeout").catch(() => undefined);
  process.exit(exitCode);
}

function terminalProtocolFailure(frame, code) {
  if (inputTerminal) return;
  stopInput();
  void (async () => {
    if (frame && validControlId(frame.id) && typeof frame.op === "string" && byteLength(frame.op) <= 64) {
      await enqueueOutbound(failureFrame(frame, code), "response");
    }
    await finishProcess(1);
  })();
}

function emitUnexpectedClose() {
  if (closing || inputTerminal || cleanupProved || unexpectedCloseEmitted) return;
  unexpectedCloseEmitted = true;
  stopInput();
  void (async () => {
    await enqueueOutbound(
      { v: CONTROL_VERSION, kind: "event", event: "closed", code: "client-closed" },
      "event",
    );
    await finishProcess(1);
  })();
}

function validateCreateSummary(data, cwd, sessionDir) {
  if (!isPlainRecord(data) || !boundedNativeTree(data)) return false;
  if (!validNativeId(data.activeSessionId) || !validNativeId(data.id) || !validNativeId(data.sessionId)) return false;
  if (data.id !== data.activeSessionId) return false;
  if (data.cwd !== cwd || !validCanonicalPath(data.cwd)) return false;
  if (data.sessionFile !== undefined && !pathIsBelow(sessionDir, data.sessionFile)) return false;
  return (
    data.lifecycle === "draft" &&
    (data.activity === "idle" || data.activity === "working") &&
    data.workerState === "ready" &&
    data.runtimeKind === "top-level" &&
    data.isSessionActive === false &&
    data.isStreaming === false &&
    data.isCompacting === false &&
    data.messageCount === 0 &&
    data.streamingMessage === undefined &&
    (data.isBashRunning === undefined || data.isBashRunning === false) &&
    (data.hasRunningRlmChildren === undefined || data.hasRunningRlmChildren === false) &&
    (data.unfinishedActionCount === undefined || data.unfinishedActionCount === 0)
  );
}

function snapshotReceipt(snapshot) {
  if (!isPlainRecord(snapshot) || !boundedNativeTree(snapshot)) return undefined;
  const state = snapshot.state;
  if (!isPlainRecord(state) || !Array.isArray(snapshot.messages) || snapshot.messages.length !== 0) return undefined;
  if (
    !validNativeId(state.sessionId) ||
    state.sessionId !== createdSessionId ||
    (state.activeSessionId !== undefined && state.activeSessionId !== activeSessionId) ||
    state.cwd !== canonicalCwd ||
    (state.sessionDir !== undefined && state.sessionDir !== canonicalSessionDir) ||
    (state.sessionFile !== undefined && !pathIsBelow(canonicalSessionDir, state.sessionFile)) ||
    state.messageCount !== 0 ||
    state.isStreaming !== false ||
    state.isCompacting !== false ||
    (state.isBashRunning !== undefined && state.isBashRunning !== false) ||
    snapshot.streamingMessage !== undefined ||
    (snapshot.children !== undefined && (!Array.isArray(snapshot.children) || snapshot.children.length !== 0))
  ) {
    return undefined;
  }
  if (snapshot.lastEventCursor !== undefined) {
    const cursor = snapshot.lastEventCursor;
    if (
      !isPlainRecord(cursor) ||
      !validNativeId(cursor.generation) ||
      !Number.isSafeInteger(cursor.sequence) ||
      cursor.sequence < 0
    ) {
      return undefined;
    }
  }
  if (
    snapshot.lastEventSequence !== undefined &&
    (!Number.isSafeInteger(snapshot.lastEventSequence) || snapshot.lastEventSequence < 0)
  ) {
    return undefined;
  }
  return {
    messageCount: 0,
    isStreaming: false,
    hasCursor: snapshot.lastEventCursor !== undefined,
  };
}

async function cleanupOwned(timeoutMs) {
  if (cleanupProved) return true;
  if (cleanupPromise) return withTimeout(cleanupPromise, timeoutMs, "cleanup-timeout").catch(() => false);
  if (!client || !validNativeId(activeSessionId)) return false;

  cleanupPromise = (async () => {
    let response;
    try {
      response = await withTimeout(
        client.request({ type: "complete_owned_session", activeSessionId }, timeoutMs),
        timeoutMs,
        "cleanup-timeout",
      );
    } catch {
      return false;
    }
    if (
      !validDaemonResponseEnvelope(response, "complete_owned_session") ||
      response.success !== true ||
      response.data !== undefined
    ) {
      return false;
    }

    cleanupProved = true;
    createMayBeCommitted = false;
    try {
      unsubscribeConnection?.();
    } catch {}
    unsubscribeConnection = undefined;
    const ownedConnection = connection;
    if (ownedConnection) {
      try {
        await withTimeout(Promise.resolve().then(() => ownedConnection.dispose()), timeoutMs, "dispose-timeout");
      } catch {
        // The raw supervisor response above is the cleanup proof. Disposal is
        // still bounded and attempted only after that proof.
      }
    }
    connection = undefined;
    activeSessionId = undefined;
    createdSessionId = undefined;
    attachState = "closed";
    createState = "closed";
    return true;
  })();
  return cleanupPromise;
}

async function loadOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "entry", "manifestVersion"])) {
    return { ok: false, code: "invalid-load-request", terminal: true };
  }
  if (
    loadState !== "idle" ||
    !validCanonicalPath(frame.entry) ||
    !validBoundedString(frame.manifestVersion, 256)
  ) {
    return { ok: false, code: "invalid-load-request" };
  }
  loadState = "loading";
  let candidate;
  try {
    candidate = await withTimeout(import(pathToFileURL(frame.entry).href), LOAD_TIMEOUT_MS, "public-api-load-timeout");
  } catch (error) {
    loadState = "failed";
    return {
      ok: false,
      code: error instanceof FixedTimeout ? error.code : "public-api-load-failed",
    };
  }
  if (!validPublicApi(candidate, frame.manifestVersion)) {
    loadState = "failed";
    return { ok: false, code: "incompatible-public-api" };
  }
  api = candidate;
  loadState = "ready";
  return { ok: true };
}

async function connectOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "socket", "timeoutMs"])) {
    return { ok: false, code: "invalid-connect-request", terminal: true };
  }
  if (!validCanonicalPath(frame.socket) || !validTimeout(frame.timeoutMs)) {
    return { ok: false, code: "invalid-connect-request", terminal: true };
  }
  if (loadState !== "ready" || !api) return { ok: false, code: "public-api-not-loaded" };
  if (connectState !== "idle" || client || openingClient) {
    return { ok: false, code: "invalid-connect-request" };
  }

  connectState = "connecting";
  const socket = frame.socket;
  const deadline = Date.now() + frame.timeoutMs;
  try {
    openingClient = new api.DaemonClient(socket, { maxInboundFrameBytes: DAEMON_INBOUND_FRAME_BYTES });
  } catch {
    connectState = "failed";
    openingClient = undefined;
    return { ok: false, code: "daemon-client-create-failed" };
  }

  try {
    await withTimeout(openingClient.connect(Math.max(1, remaining(deadline))), Math.max(1, remaining(deadline)), "daemon-connect-timeout");
  } catch (error) {
    quietClose(openingClient);
    openingClient = undefined;
    connectState = "failed";
    return { ok: false, code: error instanceof FixedTimeout ? error.code : "daemon-connect-failed" };
  }

  let hello;
  try {
    const timeout = remaining(deadline);
    if (timeout <= 0) throw new FixedTimeout("daemon-hello-timeout");
    hello = await withTimeout(openingClient.waitForHello(timeout), timeout, "daemon-hello-timeout");
  } catch (error) {
    quietClose(openingClient);
    openingClient = undefined;
    connectState = "failed";
    return { ok: false, code: error instanceof FixedTimeout ? error.code : "daemon-hello-failed" };
  }

  if (!validHello(hello, socket)) {
    quietClose(openingClient);
    openingClient = undefined;
    connectState = "failed";
    return { ok: false, code: "incompatible-daemon-hello" };
  }

  client = openingClient;
  try {
    unsubscribeClientClose = client.onClose(() => {
      try {
        emitUnexpectedClose();
      } catch {}
    });
  } catch {
    quietClose(client);
    client = undefined;
    openingClient = undefined;
    connectState = "failed";
    return { ok: false, code: "daemon-close-subscribe-failed" };
  }
  connectState = "ready";
  return { ok: true };
}

function startCreateOperation(frame) {
  createOperationPromise = createOperation(frame);
  return createOperationPromise;
}

async function createOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "cwd", "sessionDir", "timeoutMs"])) {
    return { ok: false, code: "invalid-create-request", terminal: true };
  }
  if (!validCanonicalPath(frame.cwd) || !validCanonicalPath(frame.sessionDir) || !validTimeout(frame.timeoutMs)) {
    return { ok: false, code: "invalid-create-request", terminal: true };
  }
  if (connectState !== "ready" || !client) return { ok: false, code: "daemon-not-connected" };
  if (createState !== "idle") return { ok: false, code: "invalid-create-request" };

  createState = "creating";
  canonicalCwd = frame.cwd;
  canonicalSessionDir = frame.sessionDir;
  createMayBeCommitted = true;
  let response;
  try {
    response = await withTimeout(
      client.request(
        {
          type: "create",
          lifecycle: "client_owned",
          config: {
            cwd: frame.cwd,
            sessionDir: frame.sessionDir,
            noTools: true,
            noExtensions: true,
            noSkills: true,
            noPromptTemplates: true,
            noThemes: true,
            noContextFiles: true,
          },
        },
        frame.timeoutMs,
      ),
      frame.timeoutMs,
      "session-create-timeout",
    );
  } catch (error) {
    createState = "uncertain";
    return {
      ok: false,
      code: "cleanup-uncertain",
      terminal: !closing,
    };
  }

  const possibleId = response?.data?.activeSessionId;
  if (validNativeId(possibleId)) activeSessionId = possibleId;
  if (!validDaemonResponseEnvelope(response, "create")) {
    createState = "invalid";
    if (!activeSessionId) {
      return { ok: false, code: "cleanup-uncertain", terminal: !closing };
    }
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return {
      ok: false,
      code: cleaned ? "invalid-create-response" : "cleanup-uncertain",
      terminal: !closing,
    };
  }
  if (response.success !== true) {
    createMayBeCommitted = response.errorInfo?.code === "command_result_uncertain";
    createState = createMayBeCommitted ? "uncertain" : "failed";
    return {
      ok: false,
      code: createMayBeCommitted ? "cleanup-uncertain" : "session-create-failed",
      terminal: createMayBeCommitted && !closing,
    };
  }
  if (!validateCreateSummary(response.data, frame.cwd, frame.sessionDir)) {
    createState = "invalid";
    if (!activeSessionId) {
      return { ok: false, code: "cleanup-uncertain", terminal: !closing };
    }
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return {
      ok: false,
      code: cleaned ? "invalid-create-response" : "cleanup-uncertain",
      terminal: !closing,
    };
  }

  activeSessionId = response.data.activeSessionId;
  createdSessionId = response.data.sessionId;
  createMayBeCommitted = false;
  createState = "ready";
  return { ok: true, data: { activeSessionId } };
}

async function attachOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "timeoutMs", "snapshotTimeoutMs"])) {
    return { ok: false, code: "invalid-attach-request", terminal: true };
  }
  if (!validTimeout(frame.timeoutMs) || !validTimeout(frame.snapshotTimeoutMs)) {
    return { ok: false, code: "invalid-attach-request", terminal: true };
  }
  if (createState !== "ready" || !client || !validNativeId(activeSessionId)) {
    return { ok: false, code: "session-not-created" };
  }
  if (attachState !== "idle") return { ok: false, code: "invalid-attach-request" };

  attachState = "attaching";
  const deadline = Date.now() + frame.timeoutMs;
  try {
    connection = new api.DaemonAgentConnection(client, activeSessionId, {
      ownedSession: true,
      supportsExtensionUi: false,
      closeClientOnDispose: false,
      snapshotTimeoutMs: frame.snapshotTimeoutMs,
    });
    unsubscribeConnection = connection.subscribe((event) => {
      try {
        if (event?.type === "closed") emitUnexpectedClose();
      } catch {}
    });
  } catch {
    attachState = "failed";
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return { ok: false, code: cleaned ? "session-attach-failed" : "cleanup-uncertain", terminal: !closing };
  }

  try {
    const timeout = remaining(deadline);
    if (timeout <= 0) throw new FixedTimeout("session-attach-timeout");
    await withTimeout(Promise.resolve().then(() => connection.attach()), timeout, "session-attach-timeout");
  } catch (error) {
    attachState = "failed";
    if (closing) return { ok: false, code: "host-closing" };
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return {
      ok: false,
      code: cleaned
        ? error instanceof FixedTimeout
          ? error.code
          : "session-attach-failed"
        : "cleanup-uncertain",
      terminal: true,
    };
  }

  let snapshot;
  try {
    const timeout = remaining(deadline);
    if (timeout <= 0) throw new FixedTimeout("initial-snapshot-timeout");
    snapshot = await withTimeout(
      Promise.resolve().then(() => connection.getInitialSnapshot()),
      timeout,
      "initial-snapshot-timeout",
    );
  } catch (error) {
    attachState = "failed";
    if (closing) return { ok: false, code: "host-closing" };
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return {
      ok: false,
      code: cleaned
        ? error instanceof FixedTimeout
          ? error.code
          : "initial-snapshot-failed"
        : "cleanup-uncertain",
      terminal: true,
    };
  }

  if (!snapshotReceipt(snapshot)) {
    attachState = "failed";
    if (closing) return { ok: false, code: "host-closing" };
    const cleaned = await cleanupOwned(frame.timeoutMs);
    return {
      ok: false,
      code: cleaned ? "invalid-initial-snapshot" : "cleanup-uncertain",
      terminal: true,
    };
  }
  if (closing) return { ok: false, code: "host-closing" };
  attachState = "ready";
  return { ok: true };
}

async function snapshotOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "timeoutMs"])) {
    return { ok: false, code: "invalid-snapshot-request", terminal: true };
  }
  if (!validTimeout(frame.timeoutMs)) {
    return { ok: false, code: "invalid-snapshot-request", terminal: true };
  }
  if (attachState !== "ready" || !connection) return { ok: false, code: "session-not-attached" };
  let snapshot;
  try {
    snapshot = await withTimeout(
      Promise.resolve().then(() => connection.getInitialSnapshot()),
      frame.timeoutMs,
      "session-snapshot-timeout",
    );
  } catch (error) {
    return {
      ok: false,
      code: error instanceof FixedTimeout ? error.code : closing ? "host-closing" : "session-snapshot-failed",
    };
  }
  if (closing) return { ok: false, code: "host-closing" };
  const receipt = snapshotReceipt(snapshot);
  if (!receipt) return { ok: false, code: "invalid-session-snapshot" };
  return { ok: true, data: receipt };
}

async function closeOperation(frame) {
  if (!hasExactKeys(frame, ["v", "id", "op", "timeoutMs"]) || !validTimeout(frame.timeoutMs)) {
    return { ok: false, code: "invalid-close-request", terminal: true };
  }
  const deadline = Date.now() + frame.timeoutMs;

  if (createState === "creating" && createOperationPromise) {
    const timeout = remaining(deadline);
    if (timeout <= 0) return { ok: false, code: "cleanup-uncertain", terminal: true };
    try {
      await withTimeout(createOperationPromise, timeout, "close-create-timeout");
    } catch {
      return { ok: false, code: "cleanup-uncertain", terminal: true };
    }
  }

  if (validNativeId(activeSessionId) && !cleanupProved) {
    const timeout = remaining(deadline);
    if (timeout <= 0 || !(await cleanupOwned(timeout))) {
      quietClose(client);
      return { ok: false, code: "cleanup-uncertain", terminal: true };
    }
  } else if (createMayBeCommitted && !cleanupProved) {
    quietClose(client);
    return { ok: false, code: "cleanup-uncertain", terminal: true };
  }

  try {
    unsubscribeClientClose?.();
  } catch {}
  unsubscribeClientClose = undefined;
  quietClose(client);
  client = undefined;
  openingClient = undefined;
  return { ok: true, terminal: true, exitCode: 0 };
}

const handlers = Object.freeze({
  load: loadOperation,
  connect: connectOperation,
  create: startCreateOperation,
  attach: attachOperation,
  snapshot: snapshotOperation,
  close: closeOperation,
});

async function runOperation(frame) {
  inFlight += 1;
  let result;
  try {
    result = await handlers[frame.op](frame);
  } catch {
    result = { ok: false, code: "host-operation-failed", terminal: true };
  }

  let written;
  if (result.ok) written = await enqueueOutbound(successFrame(frame, result.data), "response");
  else written = await enqueueOutbound(failureFrame(frame, result.code), "response");
  inFlight -= 1;

  if (!written) {
    await finishProcess(1);
    return;
  }
  if (result.terminal && (!closing || frame.op === "close")) {
    await finishProcess(result.exitCode ?? 1);
  }
}

function acceptFrame(frame) {
  if (inputTerminal) return;
  if (!isPlainRecord(frame) || frame.v !== CONTROL_VERSION || !validControlId(frame.id) || typeof frame.op !== "string") {
    terminalProtocolFailure(undefined, "invalid-request");
    return;
  }
  const id = BigInt(frame.id);
  if (id !== nextControlId) {
    terminalProtocolFailure(frame, id < nextControlId ? "stale-control-id" : "unknown-control-id");
    return;
  }
  nextControlId += 1n;
  if (!OPERATIONS.has(frame.op)) {
    terminalProtocolFailure(frame, "unknown-operation");
    return;
  }

  if (frame.op === "close") {
    closing = true;
    stopInput();
    void runOperation(frame);
    return;
  }
  if (closing) {
    terminalProtocolFailure(frame, "host-closing");
    return;
  }
  if (inFlight >= MAX_IN_FLIGHT) {
    void enqueueOutbound(failureFrame(frame, "too-many-in-flight"), "response").then((written) => {
      if (!written) void finishProcess(1);
    });
    return;
  }
  void runOperation(frame);
}

function parseLine(line) {
  let text;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(line);
  } catch {
    terminalProtocolFailure(undefined, "malformed-request");
    return;
  }
  let frame;
  try {
    frame = JSON.parse(text);
  } catch {
    terminalProtocolFailure(undefined, "malformed-request");
    return;
  }
  acceptFrame(frame);
}

process.stdin.on("data", (chunk) => {
  if (inputTerminal) return;
  if (!Buffer.isBuffer(chunk)) chunk = Buffer.from(chunk);
  input = Buffer.concat([input, chunk]);
  while (!inputTerminal) {
    const newline = input.indexOf(0x0a);
    if (newline < 0) {
      if (input.length > MAX_CONTROL_FRAME_BYTES) {
        terminalProtocolFailure(undefined, "request-exceeds-frame-bound");
      }
      return;
    }
    if (newline > MAX_CONTROL_FRAME_BYTES) {
      terminalProtocolFailure(undefined, "request-exceeds-frame-bound");
      return;
    }
    const line = input.subarray(0, newline);
    input = input.subarray(newline + 1);
    parseLine(line);
  }
});

process.stdin.on("end", () => {
  if (inputTerminal) return;
  if (input.length !== 0) {
    terminalProtocolFailure(undefined, "partial-request-at-eof");
    return;
  }
  stopInput();
  void finishProcess(1);
});

process.stdin.on("error", () => {
  if (!inputTerminal) {
    stopInput();
    void finishProcess(1);
  }
});

process.stdout.on("error", () => {
  stopInput();
  void finishProcess(1);
});

process.on("uncaughtException", () => {
  stopInput();
  void finishProcess(1);
});

process.on("unhandledRejection", () => {
  stopInput();
  void finishProcess(1);
});
