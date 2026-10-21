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
- Before checking reflection edits, write the intended learning_decision, no_change_reason and logical
  playbook_ids to `.git/sdo-reflection-decision.json`, or pass an explicit JSON file with `--decision`.
  Return the same decision and IDs; no_change and sdk_gap require a reason and no diff. Enforced by:
  `reflection.decision`.
- Keep changed lines free of trailing whitespace and keep logical snapshots within the aggregate budget.
  Supply valid detector sources so the snapshot can be projected. Enforced by: `diff.whitespace`,
  `memory.snapshot-budget`, `memory.snapshot`.
- Generic service-endpoint and traffic health detectors come from Go policy. Do not author or overwrite them;
  the shared check prepares and validates them. Enforced by: `bootstrap.generic-health`.
