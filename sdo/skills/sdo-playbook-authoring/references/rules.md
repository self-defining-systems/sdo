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

- Recorded faults must produce incident detector findings that bind required roles through production selection, reach Match, and replay mitigation on the same binding; the healthy baseline must stay quiet. Enforced by: `program.repeat-unreachable`.
- A mitigator must clear its diagnosis after its patches are applied to the faulted fixture, An already-faulty target value is not a repair. Enforced by: `program.mitigation-not-repaired`.
- Prefer narrow typed SetField repairs when the damaged field is known; whole-object ReconcileFromSource requires whole-object justification. Observed and parameter-derived fields require valid patches and available observations; accepted-source field selectors require accepted manifests in the fixture. Enforced by: `program.mitigation-unreplayable`, `program.mitigation-not-repaired`.

- CreateFromSource requires accepted-source presence; DeleteObject targets any named nonsecret object without a source-absence gate. Both require direct current UID/absence verification. Delete is disclosed as irreversible and never auto-recreated. They must execute successfully in replay to prove repair. Enforced by: `program.mitigation-unreplayable`.

## Value provenance and runtime effects

Mitigation returns to accepted or observed state; intended-state changes are source repairs. An emergency
literal is a declared scalar parameter the responder fills and justifies. Opaque `FromSource`, `FromObserved`,
or `FromParam` values enforce the Go API boundary; raw SetField, replica, or command literals fail compilation.

Declare each scalar parameter's kind and optional default. The SDK's `ValidateBindings` checks supplied values;
its value resolver rejects undeclared parameters, wrong types, zero Values, and Secret references before writes.

Runtime action reports and receipts retain commit/path, resourceVersion/path, or parameter-name provenance.
The runtime guards DeletePod by UID/version and directly verifies ScaleWorkload replicas. Exec uses existing
controller RBAC and returns an exit code with no stdout/stderr; commands can reach mounted Secret files.

- Replay external effects only with the required evidence. A skipped effect proves no repair; irreversible
  Pod deletion or Exec never grants restart mutation or recreation authority. Enforced by:
  `program.mitigation-unreplayable`.

## Corpus evidence

- Each verified cause needs diagnose and mitigation programs, a README, and detector coverage; one detector may
  surface several playbooks. Reflection can edit only responder-owned incident detectors, so new coverage goes
  there; health-owned and built-in detectors cannot newly link a program. Only a typed SDK gap naming the cause
  resources can excuse missing coverage.
  Enforced by: `reflection.program-required`, `reflection.detector-required`.
- Build replay fixtures from recorded session observations. Controller capture files and snapshot digests are
  optional. Mitigation must execute and clear diagnosis; skipped effects fail replay. Enforced by: `program.mitigation-not-repaired`.
- Preserve every accepted case exactly as it appears at the accepted base commit, including snapshots, recordings,
  provenance, bindings and labels. Do not delete or relabel it; submit new evidence with a new case ID. Enforced by: `program.corpus-immutable`.

- RemoveServicePort must identify exactly one port and explicitly allow removing the last entry.
  EnsureDatabaseUser uses only env or namespace-local ConfigMap key/assignment references, preserves other users and
  existing passwords, and discloses irreversible effects. Catalog diagnosis/replay requires a successful
  recorded open observation; fixed answers never substitute for applying the action. Enforced by:
  `program.mitigation-unreplayable`, `program.mitigation-not-repaired`.

General actions do not require per-fault value provenance or accepted-source presence. Use the
[general action vocabulary](general-actions.md) and retain exact recorded transition evidence
for commands, HTTP, raw API effects and one-off completion. Missing evidence remains Unknown;
Fixed replies never establish an effect. Existing compilation/replay diagnostics still apply.
