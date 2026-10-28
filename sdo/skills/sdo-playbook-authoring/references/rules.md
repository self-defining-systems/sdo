# Playbook rules

Every rule names the validator rule IDs that enforce it. `sdo-operator memory check` prints the ID of each
diagnostic; look it up here.

## Location and identity

- A playbook is a directory named by its logical ID containing `README.md`, plus optional `scripts/*.sh`. The ID
  matches `^[a-z][a-z0-9_-]*$` and is unique. Create it with `memory new playbook`. Enforced by:
  `layout.unknown-path`, `id.pattern`, `playbook.missing-readme`.
- Memory contains at least one playbook. Enforced by: `playbook.none`.

## Front matter

- `README.md` starts with YAML front matter and a non-empty body. Enforced by: `document.parse`.
- Front matter: `schema_version: 1`, `owner: responder`, and a non-empty `originating_incident`; `fault_class` is
  derived from the playbook ID when SDO publishes. Enforced by: `playbook.owner`, `playbook.originating-incident`,
  `document.parse`.

## Body

- The body, the index, the architecture, scripts, and detector source never contain physical `.sdo/` paths. Refer
  to playbooks as the logical ID and to scripts as `<playbook-id>/<name>.sh`. Enforced by: `text.physical-path`.

## Index

- The playbook index links only to playbooks. SDO adds each changed playbook's link when it publishes.
  Enforced by: `playbook.index-missing`, `playbook.index-link`.

## Scripts

- Scripts are `.sh` files in the playbook's `scripts/` directory, executable (`chmod +x`), with valid bash
  syntax, and named in the playbook text. Enforced by: `script.not-executable`, `script.syntax`,
  `script.unreferenced`.
- Scripts contain no secrets and no symlinks, and each file stays under 256 KiB. Enforced by:
  `layout.secret-path`, `layout.symlink`, `layout.too-large`.

## References from detectors

- A detector surfaces a playbook by ID in `possiblePlaybooks`; the playbook must exist. Enforced by:
  `ref.unknown-playbook`.

## Ownership

- The responder writes playbooks, scripts, and the index during reflection. The health judge writes only the
  health-objective playbook during the initial health-judge stage. Enforced by: `ownership.denied`.

Replay results are information only; compilation, imports, changed-source formatting, ownership and
provenance are required.

## Corpus evidence

Every recorded hand repair needs an executable typed SDK or RunScript step (partial coverage is fine), with
relation-derived Env and digest-to-receipt linkage; README text holds only judgement and ordering.

- Build replay fixtures from recorded session observations when available. Missing or inconclusive replay
  is recorded with its reason and never rejects publication; retain RunScript receipt linkage. Enforced by: `program.mitigation-not-repaired`.
- Preserve every accepted case exactly as it appears at the accepted base commit, including snapshots, recordings,
  provenance, bindings and labels. Do not delete or relabel it; submit new evidence with a new case ID. Enforced by: `program.corpus-immutable`.
