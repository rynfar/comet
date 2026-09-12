# Upstream survey, merge and verification

Survey from the current checkout, then run merge/verification commands from the new isolated sync worktree. Select and record the intended local main SHA; do not silently discard unpushed local fork commits.

## 1. Survey — report before mutating anything

```sh
git fetch upstream
git log --oneline main..upstream/main            # incoming
git log --oneline --no-merges upstream/main..main # our divergence
comm -12 <(git diff --name-only main...upstream/main | sort) \
         <(git diff --name-only upstream/main...main | sort)  # conflict forecast
```

**Output:** a survey message — incoming commits grouped by area, our
divergence list, and the forecast files (both sides touched them) with which
local commits are at risk. If nothing is incoming, report "up to date" and
stop. Do not run `git merge` in this phase.

## 2. Merge on a sync branch

```sh
git worktree add -b sync/upstream-<date> <owned-worktree-path> <local-base-sha>
# Run remaining commands from that worktree.
git config rerere.enabled   # expect true; enable if unset
git merge upstream/main
```

The branch is the abort path: any mess is discarded with
`git merge --abort` / branch delete, and `main` never holds a half-resolved
state.

## 3. Resolve — the user picks non-trivial winners

For every conflicted file, gather intent for both sides before touching it:
the upstream commits behind their hunk (`git log --oneline main..upstream/main -- <file>`,
read the messages) and the local commits behind ours
(`git log --oneline --no-merges upstream/main..main -- <file>`).

- **Trivial** (formatting, adjacent-line collisions, or only one side changed
  behavior): resolve it, and list it in the decision table as auto-resolved.
- **Non-trivial** (both sides changed behavior): present it to the user using the current harness's user-question mechanism — ours / theirs / combined as options, each carrying the
  originating commit's intent, plus a recommendation. Honor an existing explicit choice covering the conflict; otherwise get the user's decision before the dependent resolution.

**Output:** a decision table — file, conflict, decision, who decided, why.

## 4. Verify — required even when the merge was clean

A clean textual merge still breaks semantically (upstream renames a symbol;
our fork-only code calls the old name — git flags nothing).

```sh
cargo build                                       # desktop workspace
# if apps/ios changed on either side:
cd apps/ios && xcodebuild test -project Zeron.xcodeproj -scheme Zeron \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath build -packageAuthorizationProvider netrc
```

The `-packageAuthorizationProvider netrc` flag is load-bearing: headless SPM
hangs forever on a Keychain lookup without it. Run long builds in the
background. On failure, trace to the merge decision that caused it, fix
forward on the sync branch, and include the failure verbatim in the report.

**Output:** pass/fail per suite, with failures quoted.

## 5. Land

Land only when every behavioral decision is covered by the user's answer or existing explicit approval, and affected verification passes. While a decision is pending, leave dependent conflict resolutions untouched and continue independent preparation. Do not commit or land a provisional behavioral choice.

Once decisions are resolved, finish the merge and verification on the sync branch. Merge it locally to `main` or open a PR according to the user's requested scope; preserve any existing checkout ownership. Do not push unless asked. Report the survey, decisions, verification and current ahead/behind counts against the reviewed upstream head.
