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

Replay reports diagnosis, detector observations and plan application as information only. Open NoMatch,
fixed/post-plan Match, unavailable reads, empty or unappliable plans and failing replay tests never block
saving. Compilation, imports, changed-source formatting, ownership and provenance remain required.
Source values and live mutation policies are checked when a plan is applied.

## Value provenance and runtime effects

Mitigation returns to accepted or observed state; intended-state changes are source repairs. An emergency
literal is a declared scalar parameter the responder fills and justifies. Opaque `FromSource`, `FromObserved`,
or `FromParam` values enforce the Go API boundary; raw SetField, replica, or command literals fail compilation.

Declare each scalar parameter's kind and optional default. The SDK's `ValidateBindings` checks supplied values;
its value resolver rejects undeclared parameters, wrong types, zero Values, and Secret references before writes.

Runtime action reports and receipts retain commit/path, resourceVersion/path, or parameter-name provenance.
The runtime guards DeletePod by UID/version and directly verifies ScaleWorkload replicas. Exec uses the controller's
pods/exec grant in the application namespace and returns an exit code with no stdout/stderr; commands can reach mounted Secret files.

Advice, not validator-checked: unavailable external-effect replay proves no repair; report its reason.
Irreversible Pod deletion or Exec never grants restart mutation or recreation authority.

## Corpus evidence

Partial mechanical coverage is useful: save each verified step as a program; the README explains orchestration
and judgement about when and in what order to run steps (what to observe, decide and why).
Any performed repair can be a `RunScript` bash step with relation-derived Env and digest-to-receipt linkage;
difficulty never justifies saving nothing. Verified, specific README-only playbooks are allowed
and never auto-applied. Return `updated` with IDs and scoped `sdk_gaps` for unencoded parts of the same repair.
Advice, not validator-checked: add detector coverage; one detector may surface several playbooks. Reflection
can edit only responder-owned incident detectors. Missing executable coverage is a gap, not a refusal.

- Build replay fixtures from recorded session observations when available. Missing or inconclusive replay
  is recorded with its reason and never rejects publication; retain RunScript receipt linkage. Enforced by: `program.mitigation-not-repaired`.
- Preserve every accepted case exactly as it appears at the accepted base commit, including snapshots, recordings,
  provenance, bindings and labels. Do not delete or relabel it; submit new evidence with a new case ID. Enforced by: `program.corpus-immutable`.

Advice, not validator-checked: live RemoveServicePort identifies exactly one port and explicitly allows removing the last entry.
  EnsureDatabaseUser uses only env or namespace-local ConfigMap key/assignment references, preserves other users and
  existing passwords, and discloses irreversible effects. Unavailable catalog observations remain Unknown. Replay findings are informational; fixed answers never
  establish a live repair.

General actions do not require per-fault value provenance or accepted-source presence. Use the
[general action vocabulary](general-actions.md) and retain exact recorded transition evidence
for commands, HTTP, raw API effects and one-off completion. Missing evidence remains Unknown;
Fixed replies never establish an effect. Compilation and provenance still apply; replay verdicts remain information.
