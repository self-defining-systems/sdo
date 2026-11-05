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

- SDO generates the playbook index: one line per active playbook, titled by its README's first heading.
  Edits to the index are replaced. Enforced by: `playbook.index-missing`, `playbook.index-generated`.

## Scripts

- Scripts are `.sh` files in the playbook's `scripts/` directory, executable (`chmod +x`), with valid bash
  syntax, and named in the playbook text. Enforced by: `script.not-executable`, `script.syntax`,
  `script.unreferenced`.
- Scripts contain no secrets and no symlinks, and each file stays under 256 KiB. Enforced by:
  `layout.secret-path`, `layout.symlink`, `layout.too-large`.

## One playbook per repair method

- A playbook is one repair method, named for the repair (`workload-restart`, `redis-acl-restore`), not for a
  symptom or a service; its roles bind the component it repairs, so the same repair on another component reuses
  it; list a reused playbook in every detector whose binding it needs. A new repair method is a new playbook, never an extra action in a symptom-named one; when an applied
  playbook did not fix this incident, narrow its Diagnose so it no longer matches this cause. Never add a
  required role that no detector binds; derive related objects (a workload's Pods) through ReadView. A new
  playbook may not copy an existing program (the same parameter kinds, Diagnose and Mitigate apart from names,
  prose and identifier spelling): refine that playbook instead. A new playbook that repeats only another's
  Mitigate is noted: refine the other unless this Diagnose needs evidence it lacks. A new playbook whose Diagnose
  differs from an existing one only in the components it names (scenario, workload, Service) is the same failure
  on another component: parameterize the existing playbook by its role instead. Enforced by:
  `playbook.duplicate-repair`, `playbook.component-copy`, `playbook.shared-repair`.
- A program with a role never adds a component name (an accepted-source object or traffic scenario ID) outside
  its Spec, prose and error text: derive it from the bound object ([localized components](programs.md#localized-components)).
  Names the accepted version already had are left alone. Enforced by: `playbook.component-literal`.
- Programs, libraries, scripts and incident detectors never name a generated Pod (`<owner>-<hash>-<suffix>`).
  Bind the owning workload, find its Pods through ReadView by owner, and exec with a typed `Command` or
  `EnsureExec`. Enforced by: `playbook.live-object-name`.

## References from detectors

- A detector surfaces a playbook by ID in `possiblePlaybooks`; the playbook must exist. Enforced by:
  `ref.unknown-playbook`.

## Ownership

- The responder writes playbooks and scripts during reflection. The detection designer writes only the
  health-objective playbook during the initial detection-designer stage. Enforced by: `ownership.denied`.

Compilation, imports, changed-source formatting, ownership and receipt provenance are required.
<!-- replay:on -->
Replay results are information only.
<!-- /replay:on -->

## Diagnosis

- Diagnose must observe its own repair. Read bookkeeping annotations (`kubectl.kubernetes.io/restartedAt`,
  revisions, last-applied) from the live object: source and baseline comparisons strip them, so a lookup there
  never changes and the program matches again after its own restart. Enforced by: `program.normalized-read`.
- Diagnose compares typed fields (`diff.ContainerChanges`, `diff.Field`), never a substring of `diff.Text`;
  such programs still load but are noted for rewrite. Enforced by: `program.diff-text-match`.

## Executable repair evidence

Every recorded hand repair needs an executable typed SDK or RunScript step (partial coverage is fine), with
relation-derived Env and digest-to-receipt linkage; README text holds only judgement and ordering.

<!-- replay:on -->
## Corpus evidence

- Build replay fixtures from recorded session observations when available. Missing or inconclusive replay
  is recorded with its reason and never rejects publication; retain RunScript receipt linkage. Enforced by: `program.mitigation-not-repaired`.
- Preserve every accepted case exactly as it appears at the accepted base commit, including snapshots, recordings,
  provenance, bindings and labels. Do not delete or relabel it; submit new evidence with a new case ID. Enforced by: `program.corpus-immutable`.
<!-- /replay:on -->
