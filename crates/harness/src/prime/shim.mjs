// Comet-owned Prime Agent public-SDK bridge.
//
// Rust owns and supervises the native daemon. This process imports only the
// package's validated public root ESM entry and projects a small bounded JSONL
// control protocol. Raw hello payloads, paths, provider output, and SDK errors
// never cross stdout.

import { pathToFileURL } from "node:url";

const CONTROL_VERSION = 1;
const MAX_FRAME_BYTES = 64 * 1024;
const MAX_CAPABILITIES = 128;
const MAX_CAPABILITY_BYTES = 128;
const MAX_PRIVATE_ID_BYTES = 256;
const PROTOCOL_NAME = "prime-agent.daemon";
const MIN_PROTOCOL_VERSION = 7;
const RETRY_MS = 40;

let api;
let client;
let input = Buffer.alloc(0);
let queue = Promise.resolve();

const controlWrite = process.stdout.write.bind(process.stdout);
// Imported provider code must not be able to mix ordinary diagnostics into
// the normalized control channel. The bridge keeps the original writer only
// in this closure; daemon and bridge stderr are redirected to null by Rust.
process.stdout.write = (_chunk, encoding, callback) => {
  const done = typeof encoding === "function" ? encoding : callback;
  if (typeof done === "function") queueMicrotask(done);
  return true;
};
console.log = console.info = console.warn = console.error = () => {};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const respond = (frame) => controlWrite(`${JSON.stringify(frame)}\n`);
const fail = (id, code) => respond({ v: CONTROL_VERSION, id, kind: "error", code });

function validBase(frame) {
  return (
    frame &&
    typeof frame === "object" &&
    frame.v === CONTROL_VERSION &&
    Number.isSafeInteger(frame.id) &&
    frame.id >= 0 &&
    typeof frame.op === "string"
  );
}

function validatePublicApi(candidate, expectedVersion) {
  const prototype = candidate?.DaemonClient?.prototype;
  return (
    candidate &&
    typeof candidate === "object" &&
    typeof candidate.VERSION === "string" &&
    candidate.VERSION === expectedVersion &&
    candidate.DAEMON_PROTOCOL_NAME === PROTOCOL_NAME &&
    Number.isSafeInteger(candidate.DAEMON_PROTOCOL_VERSION) &&
    candidate.DAEMON_PROTOCOL_VERSION >= MIN_PROTOCOL_VERSION &&
    typeof candidate.DaemonClient === "function" &&
    typeof prototype?.connect === "function" &&
    typeof prototype?.waitForHello === "function" &&
    typeof prototype?.request === "function" &&
    typeof prototype?.close === "function" &&
    typeof candidate.DaemonAgentConnection === "function" &&
    typeof candidate.DaemonAgentConnection.attach === "function"
  );
}

async function load(frame) {
  if (api || typeof frame.entry !== "string" || typeof frame.manifestVersion !== "string") {
    fail(frame.id, "invalid-load-request");
    return;
  }
  let candidate;
  try {
    candidate = await import(pathToFileURL(frame.entry).href);
  } catch {
    fail(frame.id, "public-api-load-failed");
    return;
  }
  if (!validatePublicApi(candidate, frame.manifestVersion)) {
    fail(frame.id, "incompatible-public-api");
    return;
  }
  api = candidate;
  respond({
    v: CONTROL_VERSION,
    id: frame.id,
    kind: "loaded",
    protocolVersion: candidate.DAEMON_PROTOCOL_VERSION,
  });
}

async function connect(frame) {
  if (!api) {
    fail(frame.id, "public-api-not-loaded");
    return;
  }
  if (client || typeof frame.socket !== "string" || !Number.isSafeInteger(frame.timeoutMs) || frame.timeoutMs <= 0) {
    fail(frame.id, "invalid-connect-request");
    return;
  }

  const deadline = Date.now() + frame.timeoutMs;
  let connected;
  while (Date.now() < deadline) {
    const candidate = new api.DaemonClient(frame.socket);
    try {
      const remaining = Math.max(1, deadline - Date.now());
      await candidate.connect(Math.min(remaining, 500));
      connected = candidate;
      break;
    } catch {
      try {
        candidate.close();
      } catch {}
      await delay(Math.min(RETRY_MS, Math.max(1, deadline - Date.now())));
    }
  }
  if (!connected) {
    fail(frame.id, "daemon-connect-timeout");
    return;
  }

  let hello;
  try {
    hello = await connected.waitForHello(Math.max(1, deadline - Date.now()));
  } catch {
    try {
      connected.close();
    } catch {}
    fail(frame.id, "daemon-hello-failed");
    return;
  }

  const capabilities = hello?.serverCapabilities;
  if (
    hello?.type !== "daemon_hello" ||
    hello?.socketPath !== frame.socket ||
    hello?.protocol?.name !== PROTOCOL_NAME ||
    !Number.isSafeInteger(hello?.protocol?.version) ||
    hello.protocol.version < MIN_PROTOCOL_VERSION ||
    !Array.isArray(capabilities) ||
    capabilities.length > MAX_CAPABILITIES ||
    capabilities.some(
      (value) =>
        typeof value !== "string" ||
        value.length === 0 ||
        Buffer.byteLength(value) > MAX_CAPABILITY_BYTES ||
        !/^[a-z0-9_.-]+$/.test(value),
    )
  ) {
    try {
      connected.close();
    } catch {}
    fail(frame.id, "incompatible-daemon-hello");
    return;
  }

  client = connected;
  respond({
    v: CONTROL_VERSION,
    id: frame.id,
    kind: "ready",
    protocolVersion: hello.protocol.version,
    capabilities: [...new Set(capabilities)],
  });
}

function validPrivateId(value) {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    Buffer.byteLength(value) <= MAX_PRIVATE_ID_BYTES &&
    !/[\u0000-\u0020\u007f]/u.test(value)
  );
}

async function cleanupStatus(frame) {
  if (
    !client ||
    !validPrivateId(frame.activeSessionId) ||
    !Number.isSafeInteger(frame.timeoutMs) ||
    frame.timeoutMs <= 0
  ) {
    fail(frame.id, "invalid-cleanup-status-request");
    return;
  }

  let response;
  try {
    response = await client.request(
      { type: "get_owned_session_cleanup", activeSessionId: frame.activeSessionId },
      frame.timeoutMs,
    );
  } catch {
    fail(frame.id, "cleanup-status-failed");
    return;
  }
  const status = response?.data?.status;
  if (
    response?.type !== "response" ||
    response?.command !== "get_owned_session_cleanup" ||
    response?.success !== true ||
    !["active", "stopping", "settled"].includes(status)
  ) {
    fail(frame.id, "cleanup-status-failed");
    return;
  }
  respond({
    v: CONTROL_VERSION,
    id: frame.id,
    kind: "owned-session-cleanup",
    status,
  });
}

async function shutdown(frame) {
  if (!client || !Number.isSafeInteger(frame.timeoutMs) || frame.timeoutMs <= 0) {
    fail(frame.id, "invalid-shutdown-request");
    return;
  }
  let admitted = false;
  let acknowledged = false;
  try {
    const response = await client.request({ type: "shutdown" }, frame.timeoutMs);
    admitted = true;
    acknowledged =
      response?.type === "response" &&
      response?.command === "shutdown" &&
      response?.success === true;
  } catch {}
  try {
    client.close();
  } catch {}
  client = undefined;
  if (!acknowledged) {
    fail(
      frame.id,
      admitted ? "shutdown-response-rejected" : "shutdown-not-acknowledged",
    );
    return;
  }
  respond({ v: CONTROL_VERSION, id: frame.id, kind: "shutdown", acknowledged: true });
}

async function handle(line) {
  let frame;
  try {
    frame = JSON.parse(line);
  } catch {
    fail(0, "malformed-request");
    return;
  }
  if (!validBase(frame)) {
    fail(Number.isSafeInteger(frame?.id) ? frame.id : 0, "invalid-request");
    return;
  }
  switch (frame.op) {
    case "load":
      await load(frame);
      break;
    case "connect":
      await connect(frame);
      break;
    case "cleanup-status":
      await cleanupStatus(frame);
      break;
    case "shutdown":
      await shutdown(frame);
      break;
    default:
      fail(frame.id, "unknown-operation");
  }
}

process.stdin.on("data", (chunk) => {
  input = Buffer.concat([input, chunk]);
  if (input.length > MAX_FRAME_BYTES) {
    fail(0, "request-exceeds-frame-bound");
    process.exitCode = 1;
    process.stdin.destroy();
    return;
  }
  let newline;
  while ((newline = input.indexOf(0x0a)) >= 0) {
    const line = input.subarray(0, newline);
    input = input.subarray(newline + 1);
    queue = queue.then(() => handle(line.toString("utf8"))).catch(() => {
      fail(0, "bridge-operation-failed");
    });
  }
});

process.stdin.on("end", () => {
  queue.finally(() => {
    try {
      client?.close();
    } catch {}
  });
});
