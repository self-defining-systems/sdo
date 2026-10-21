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
- Front matter: `schema_version: 1`, `owner: responder`, `fault_class` equal to the playbook ID (the scaffold
  sets this), and a non-empty `originating_incident`. Enforced by: `playbook.owner`, `playbook.fault-class`,
  `playbook.originating-incident`, `document.parse`.

## Body

- The body names resources by role placeholder in angle brackets and capitals, such as `<TARGET_RESOURCE>`, not by
  concrete names. Enforced by: `playbook.placeholder`.
- The body, the index, the architecture, scripts, and detector source never contain physical `.sdo/` paths. Refer
  to playbooks as the logical ID and to scripts as `<playbook-id>/<name>.sh`. Enforced by: `text.physical-path`.

## Index

- The playbook index lists every playbook as a Markdown link to its README using its logical ID and links only to playbooks.
  `memory new playbook` maintains it. Enforced by: `playbook.index-missing`, `playbook.index-link`,
  `playbook.not-indexed`.

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

- A mitigator must clear its diagnosis after its patches are applied to the faulted fixture, including under consistent renaming. An already-faulty target value is not a repair. Enforced by: `program.mitigation-not-repaired`.
- A mitigation effect must be reproducible from recorded evidence; unavailable source reconciliation, revision lookup, or rollout effects fail explicitly. Use replayable SetField patches when the fixture proves the repair. Enforced by: `program.mitigation-unreplayable`.

## Corpus evidence

- Every added case requires captured provenance and matching snapshot digests, including cases for existing programs.
  A synthetic NoMatch case needs a reason, only negative labels, and no incident provenance. Enforced by: `program.corpus-provenance`.
- Captured cases require the provisioned capture directory and incident ID. If either is missing or the directory is
  unavailable, restore the controller-provided context before retrying. Enforced by: `program.capture-unavailable`.
- A case's incident ID must match the provisioned incident; its open, fixed and recorded primitive observations must
  match the trusted capture bytes. An empty incident label cannot bypass comparison. Enforced by: `program.capture-provenance`.
- Preserve every accepted case exactly as it appears at the accepted base commit, including snapshots, recordings,
  provenance, bindings and labels. Do not delete or relabel it; submit new evidence with a new case ID. Enforced by: `program.corpus-immutable`.
