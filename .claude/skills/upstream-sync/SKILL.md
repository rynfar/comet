---
name: upstream-sync
description: Compare or merge zeronsh/comet upstream changes into this fork, preserving local product decisions.
---

# Upstream sync

Local fork commits represent intentional product decisions. A comparison request is read-only. A sync request permits preparation and verification in an isolated sync branch; it does not authorize pushing unless the user asks.

Use [sync operations](references/sync.md) for the survey commands, merge workflow and affected desktop/iOS checks. Refresh the relevant refs and report incoming work, local divergence and overlapping behavior before merging. If nothing is incoming, report that and finish.

Resolve formatting and adjacent-line collisions directly. Where both sides change behavior, explain each intent and get the user's choice unless an existing explicit decision covers it. Continue independent preparation while that decision is pending. Preserve that decision record through verification and landing.

Do not merge directly into the product branch or disturb another checkout. A clean textual merge still needs affected verification. For headless iOS SPM, preserve `-packageAuthorizationProvider netrc` to avoid Keychain hangs. Land only after required behavioral decisions and verification are complete, within the user's requested local-merge/PR/push scope. Report failed checks and remaining work accurately.
