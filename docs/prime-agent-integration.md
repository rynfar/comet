# Prime Agent integration coordination

## Purpose

Comet is adding Prime Agent through its native daemon API. The work overlaps the existing Pylon Prime provider and the Pylon Prime Agent fork. These implementations must collaborate on one reviewed public contract rather than build incompatible integrations in parallel.

This document is repository policy for agents and maintainers. It does not authorize changes in another repository.

## Repositories and authorities

| Area | Repository | Coordination authority |
| --- | --- | --- |
| Comet harness, engine mapping, sync boundary, desktop and mobile UI | [`rynfar/comet`](https://github.com/rynfar/comet) | [Comet Prime integration umbrella #1](https://github.com/rynfar/comet/issues/1) |
| Prime daemon/client contract, negotiated capabilities, recovery, and fork release | [`pylon-code/prime-agent`](https://github.com/pylon-code/prime-agent) | [Prime fork tracker #1](https://github.com/pylon-code/prime-agent/issues/1) and `PYLON.md` |
| Provider-neutral behavior, safety semantics, and the proven Pylon adapter | [`pylon-code/pylon`](https://github.com/pylon-code/pylon) | [Pylon provider umbrella #114](https://github.com/pylon-code/pylon/issues/114) and the daemon parity ledger |
| Prime upstream behavior | [`PrimeIntellect-ai/prime-agent`](https://github.com/PrimeIntellect-ai/prime-agent) | Upstream issues, releases, and public API |

Expected local checkouts are `~/repos/comet`, `~/repos/pylon`, and `~/repos/prime-agent`. A path is discovery information, not permission to edit that checkout.

## Product direction

The primary Comet integration is a dedicated native Prime harness using the public `DaemonClient` and `DaemonAgentConnection` surface. It must use a Comet-owned private socket and normalize Prime events at the host-engine boundary.

ACP omits many Prime features and is not the primary design. It may be added later as an explicitly degraded compatibility fallback. Any ACP issue must name every unavailable capability and must not present full-access execution as sandboxed or supervised.

The native integration should expose features in provider-neutral slices. The first slice covers durable session identity, model and thinking selection, correlated prompts, streaming, steering, interruption, recovery, and subagents. Resources, queues, compaction, refinement, goals, approvals, heartbeats, and schedules require their own Comet contracts and issues.

## Issue-first collaboration

Before editing code, create or claim a focused issue and cross-link it to the umbrella issues above. The issue must record:

1. User-visible outcome and explicit non-goals.
2. Owning repository, branch, worktree, and file area.
3. Public Prime API and negotiated capability names involved.
4. Stock-Prime behavior and the safe behavior when the capability is absent.
5. Data classification: what stays private, what becomes a normalized event, and what may synchronize.
6. Dependencies on the Pylon provider, Prime fork, or Comet work.
7. Required fake-protocol, compatibility, reconnect, and failure tests.
8. Cross-repository merge and release order.
9. Validation receipts and follow-up issues.

Use one concern per issue and pull request. Put durable decisions in repository documentation or contract tests. Use issue comments for changing status, blockers, handoffs, and evidence.

## Ownership and routing

### Prime fork

Change the Prime fork only when Comet and Pylon need a public behavior that stock Prime does not provide. Follow the fork's `PYLON.md`, `.pylon/features.yaml`, and `.pylon/upstream-review.md`. Preserve `main` as the upstream mirror and target product work to `pylon` through reviewed pull requests.

A fork change must be optional and capability-gated unless it is an intentionally reviewed incompatible protocol revision. Package versions shared with upstream are not sufficient detection. A Comet dependency needs a reproducible, uniquely identified artifact or pinned build with recorded provenance.

### Pylon

Use Pylon as evidence for behavior already hardened there, including private daemon ownership, exact resume identity, event bounds, reconnect proof, safe projections, and supervised execution. Do not copy its provider-specific payloads into Comet sync or tunnel its raw server methods.

If Comet needs different provider-neutral semantics, resolve that difference on the linked issues. Do not silently redefine a shared lifecycle, queue, approval, subagent, heartbeat, or checkpoint term.

### Comet

Comet owns its `Harness` implementation, normalized `AgentEvent` mapping, engine lifecycle, synchronized data model, and UI. The Prime adapter must terminate native paths, identifiers, credentials, raw errors, and private payloads before the sync boundary.

Only opaque host-resolvable resume tokens may enter durable Comet state. Absolute session paths, daemon sockets, package roots, and native correlation identifiers must remain host-private unless a reviewed contract explicitly replaces them with safe opaque identifiers.

## Safe concurrent workflow

1. Fetch without changing an active worktree.
2. Inspect status, active worktrees, remote bases, linked issues, and current ownership.
3. If the intended checkout is dirty or owned by another agent, do not touch it. Create a separate worktree and branch from the issue's approved base.
4. Announce the worktree, branch, files, and contract on the issue before implementation.
5. Keep edits inside the claimed area. Coordinate before touching shared protocol or generated files.
6. Never use reset, clean, stash, force-push, or branch switching to remove another agent's work.
7. Commit focused changes with the issue link. Push a branch; do not push directly to protected or upstream mirrors.
8. Post test receipts and compatibility results to the issue. Hand off through the issue, not through assumptions about another agent's local state.
9. Review both consumer sides together before merging a shared contract. Merge and release in the order recorded on the issue.

## Contract and safety requirements

- Negotiate `correlated_prompt_lifecycle_v1` and future features from the daemon hello/client capability surface. Method presence alone is not proof.
- Do not resubmit a prompt after an uncertain admission. Reconcile through authoritative lifecycle, replay, or snapshot evidence; otherwise fail closed.
- Keep local daemon transport private to the host user. Do not expose the raw local socket as a remote Comet protocol.
- Bound event queues, snapshot reads, subprocess startup, shutdown, and recovery. Surface typed failures instead of hanging a Comet turn.
- Preserve prompt provenance and deterministic terminal ownership across background work, compaction, subagent continuations, reconnect, and cancellation.
- Do not claim a sandbox or approval policy that Prime does not enforce before execution.
- Do not create heartbeat or scheduled-run UI until occurrence, turn, checkpoint, stop, deletion, and restart ownership are authoritative.
- Keep authentication in Prime Agent initially. Never copy `auth.json`, OAuth tokens, API keys, or provider diagnostics into synchronized state.
- Unknown future events and capabilities must degrade locally without preventing safe session attachment.

## Private native session owner

The daemon foundation and the native session owner are separate trust boundaries. The foundation can start and stop a compatible stock Prime 0.8.1 daemon. Creating a native session additionally requires the reviewed fork contract. Before any create request, the host verifies all of these conditions:

- the public package root exports a frozen `PRIME_AGENT_SDK_FEATURES` registry containing `bounded_daemon_ingress_v1`;
- `DaemonClient` is constructed with a 64 MiB inbound-frame limit inside a Node process with a 512 MiB V8 heap limit; and
- the daemon offers `client_owned_sessions`, `chunked_snapshot`, `immutable_snapshot_transfer_v1`, and `authoritative_owned_session_cleanup_v1`.

The session owner creates one fresh `draft` session and attaches it immediately. It uses the public `DaemonAgentConnection` constructor followed by instance `attach()`. It does not use the static attach helper because attach-failure cleanup evidence must remain visible to the owner. Attaching arbitrary saved sessions is deferred until Comet has an explicit cumulative snapshot budget and an opaque durable resume mapping.

The Rust-to-Node control channel is private and bounded. Raw frames are at most 16 KiB. Decimal request identifiers are 1–16 digits and are never reused. At most eight requests and 32 normalized events can be pending. Unknown, duplicate, mismatched, malformed, or oversized frames poison the channel. Only fixed error codes and bounded safe receipts can cross back to Rust.

The session-host process receives a strict non-secret environment allowlist. Provider credentials stay in the daemon environment. Native session identifiers, active-session selectors, session files, daemon paths, sockets, process details, payloads, diagnostics, and raw SDK errors remain host-private. The public receipt exposes only bounded snapshot counts and boolean state.

A normal close succeeds only after direct owning `complete_owned_session` proof. If the owning host crashes, a fresh non-owner bootstrap connection must report authoritative cleanup as `settled`. A timeout, malformed response, unprovable process identity, or incomplete durable cleanup returns `cleanup-uncertain` and consumes the private daemon. Cancellation and `Drop` transfer the daemon, host, and cleanup obligation to a bounded reaper; they never mean that cleanup was aborted. Bootstrap channels are replaced after an interrupted or failed cleanup query before shutdown is attempted.

This slice is host-only. It does not add a `Harness`, registry entry, synchronized document, RPC, settings, model selector, or UI. Prompt submission, streaming normalization, steering, interruption, correlated settlement, reconnect and resume, subagents, resources, MCP, compaction, refinement, goals, automation, approvals, and ACP fallback remain later reviewed slices.

## Merge readiness

A Prime integration change is ready only when:

- its focused issue and cross-repository dependencies are current;
- stock and fork-capable behavior are tested separately;
- fake-protocol tests cover duplicates, stale/out-of-order events, cancellation, reconnect, recovery, and unsupported capabilities as applicable;
- no private path, credential, raw prompt, or provider payload crosses the sync boundary;
- both Comet and Pylon reviewers agree on any shared public contract;
- the required Prime artifact or pinned build is reproducible; and
- documentation states the exact supported and unavailable behavior.
