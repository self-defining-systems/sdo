# Memory layout and ownership rules

Every rule names the validator rule IDs that enforce it. The detector and playbook rules are in the
`sdo-detector-authoring` and `sdo-playbook-authoring` skills.

## Layout

- Every file under `.sdo/` sits at a path the layout defines: goal, architecture, playbooks, detectors, traffic
  files, outcomes, and metadata. A stray file is rejected; move or delete it. Enforced by: `layout.unknown-path`.
- Memory holds regular files only: no symlinks, sockets, or directories posing as files. Enforced by:
  `layout.symlink`, `layout.not-regular`.
- File names must not look like secrets, and a file stays under 256 KiB. Never write credentials into memory.
  Enforced by: `layout.secret-path`, `layout.too-large`.
- YAML, front matter, and Go files parse. Enforced by: `yaml.parse`, `document.parse`, `go.syntax`.
- Logical IDs follow their patterns (playbooks `^[a-z][a-z0-9_-]*$`, detectors `^[a-z0-9][a-z0-9_.-]*$`) and are
  unique. Enforced by: `id.pattern`, `id.duplicate`.
- Memory is shared by every cluster that adopts it: it holds what to measure and thresholds, never one cluster's
  measurements (Service UIDs, allocated IPs, probe or capture times). Enforced by: `memory.cluster-local`.

## Ownership

- Which actor may change which kind of file depends on the lifecycle phase. Outcomes and lifecycle provenance are
  controller-owned: never edit them. Enforced by: `ownership.denied`.
`memory check` always includes ownership against `SDO_BASE_COMMIT`, using `SDO_ACTOR` and `SDO_PHASE`.
Missing inputs are errors; the caller supplies the accepted base and authoring role.

## Text

- Text you write into memory refers to artifacts by logical ID, never by physical path. Enforced by:
  `text.physical-path`.


## Complete acceptance

- Propose a nonempty diff for an update; otherwise return no_change with a concrete reason. Enforced by:
  `diff.empty`.
- Source bootstrap closes after detector acceptance. Deployed source must carry trusted verification for its
  exact tree. Enforced by: `lifecycle.bootstrap-closed`, `lifecycle.verified-source`.
- Reflection requires the broker's accepted outcome for this incident. Preserve the provisioned publication
  context. Enforced by: `lifecycle.accepted-outcome`.
- Before checking reflection edits, supply the typed decision to the memory check tool. The generated runtime
  contract defines its fields and the distinction between a decision file and the session result envelope.
  Return the same decision and IDs; no_change and sdk_gap require a reason and no diff. Enforced by:
  `reflection.decision`.
- SDO strips trailing whitespace from changed `.sdo/` text; never indent with a space before a tab, and keep logical
  snapshots within the aggregate budget.
  Supply valid detector sources so the snapshot can be projected. Enforced by: `diff.whitespace`,
  `memory.snapshot-budget`, `memory.snapshot`.
- Generic service-endpoint, service-reachability and traffic health detectors come from Go policy. Do not author or overwrite them;
  the shared check prepares and validates them. Enforced by: `bootstrap.generic-health`.

- A playbook status is omitted/`active` or `retired`; retirement requires a nonempty string `retired_reason`, which is invalid on an active playbook. Enforced by: `playbook.status`.

- A hand repair is any successful receipt that no accepted playbook's application performed, including generic menu actions and removals. Every recorded hand repair needs a changed executable program with positive binding metadata and its original `RepairReceipt`; README text and SDK gaps do not encode performed steps. Replay snapshots and verdicts remain informational. Enforced by: `reflection.program-required`.
- Typed SDK gaps name unavailable observations or effects, never work left to encode. Installed SDK actions are available; a named installed read needs an authoritative unavailable-read error. Enforced by: `reflection.sdk-gap-unavailable`.
- Update or retire each applied playbook that failed, left findings unresolved or needed later hand repair; edits to a peer playbook or an SDK gap do not excuse it. Enforced by: `reflection.stale-playbook`.
