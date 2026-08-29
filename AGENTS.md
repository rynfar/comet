# Repository Working Rules

## Prime Agent integration

These rules apply to work that adds or changes Prime Agent support.

- Read [`docs/prime-agent-integration.md`](docs/prime-agent-integration.md) before investigation, design, or implementation.
- Coordinate all implementation through the cross-linked GitHub issues. The Comet umbrella is [rynfar/comet#1](https://github.com/rynfar/comet/issues/1), the Prime fork tracker is [pylon-code/prime-agent#1](https://github.com/pylon-code/prime-agent/issues/1), and the Pylon provider umbrella is [pylon-code/pylon#114](https://github.com/pylon-code/pylon/issues/114).
- Claim an issue before editing. State the repository, branch, worktree, owned files, shared contract, dependencies, compatibility behavior, tests, and merge order.
- Treat the Comet harness and Pylon provider as peers over one Prime contract. Do not make a unilateral shared-contract change or merge one side before both consumers' compatibility tests are reviewed together.
- Never modify another agent's active checkout or worktree. Do not reset, stash, switch, stage, clean, or commit its changes. Create an isolated worktree from the correct remote base.
- Route Prime public daemon/API work to `pylon-code/prime-agent`, provider-neutral product semantics to `pylon-code/pylon`, and Comet adapter/UI work to this repository. Cross-link every dependency.
- Capability-negotiate optional fork behavior. Never infer support from the package version, branch name, method presence, or a private protocol field.
- Keep daemon sockets, session files, native identifiers, prompts, credentials, and diagnostics inside the host engine. Do not persist host-local paths or raw Prime payloads in Comet's synchronized documents.
- Prime Agent's native daemon is the primary integration target. ACP is a degraded compatibility path only when an issue explicitly defines its reduced behavior.
- If an issue, contract, ownership boundary, or safe merge order is unclear, stop and resolve it on the issue before changing code.
