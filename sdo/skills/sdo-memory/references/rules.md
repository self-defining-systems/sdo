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
Run `memory check --base <accepted commit> --actor <actor> --phase <phase>` to include the ownership layer; the
broker always does.

## Text

- Text you write into memory refers to artifacts by logical ID, never by physical path. Enforced by:
  `text.physical-path`.
