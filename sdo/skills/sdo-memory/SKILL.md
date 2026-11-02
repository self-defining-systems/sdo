---
name: sdo-memory
description: Find and use operational memory by logical ID, or validate authorized memory edits. Covers artifact ownership, accepted provenance, and the worker's typed authoring interface.
---

# Operational memory

Memory exists so later incidents are fixed faster and cheaper. Read and reason across all accepted memory; incident-linked entries come first and full files remain readable in the checkout. Edit only the artifact classes and proposal checkout your task authorizes.

1. Read the supplied playbook index (ID and one-line description), then open only relevant READMEs; read a playbook before using it. Check its diagnosis against current resources first, or after applying when the match is ready to mitigate.
2. For reflection, follow [`sdo-playbook-authoring`](../sdo-playbook-authoring/SKILL.md): it is the reflection entry point. For detector changes, read [`sdo-detector-authoring`](../sdo-detector-authoring/SKILL.md).
3. The [generated layout and ownership map](references/layout.md) lists every `.sdo/` path and its owner; preserve accepted provenance, and the task's actor, phase, accepted base and incident identity.
4. Validate final edits with [check_memory](references/runtime.md#check_memory), supply typed decision fields directly, and return the same decision and logical IDs; correct named diagnostics before checking again. The broker owns acceptance; it reuses a trusted check only for identical final bytes, accepted base, policy and toolchain.

<<<<<<< HEAD
| Artifact | Meaning | Written by |
|---|---|---|
| goal | optional human-owned free text | the human |
| architecture | deterministic manifest resources, selectors, images and declared dependencies | Go manifest inventory |
| playbooks | how to diagnose and repair a class of fault, with optional scripts or deterministic Go programs, shared helpers and captured corpus | the responder (reflection); the health judge only for the health-objective playbook |
| diagnostics | deterministic Go detectors (health and incident) and synthetic-traffic files | health detectors: the health judge; incident detectors: the responder (reflection) |
| outcomes | what happened in each incident | the controller only; never edit |
=======
## Memory principles
>>>>>>> origin/vic-lsh/dev/sdo-improve

- Treat memory as advice and accepted source as intent; check both against live state before acting.
- Keep one failure mechanism per playbook; refine it when only a resource or parameter differs.
- Avoid hard-coded service or app names: prefer runtime bindings from findings, Diagnose or responder values; isolate unavoidable names in one logical helper and document app constants in the playbook README.
- Use detectors to find problems proactively and playbooks to diagnose and mitigate; their relationship need not be one to one.
- Refine a playbook whose mitigation failed or was undone, or set `status: retired` and a nonempty `retired_reason` in its front matter; retired playbooks remain in history but are not compiled or offered.
- Keep memory small and current: give each playbook README a one-line `description:` in its YAML front matter; merge duplicates. SDO generates the playbook index from each README's first heading; never edit it.
- A script fix is a first-class learnable mitigation with relation-derived Values, within incident budgets.
- Preserve [credential references](../sdo-incident-response/references/database-credentials.md), never resolved values: typed database repairs accept env or namespace-local ConfigMap keys and optional literal assignment names; only the executor resolves them.
- Exclude benchmark/grader details, secrets and physical paths from memory text; physical layout is only for checkout edits and tooling, while prose and results use logical IDs.

<<<<<<< HEAD
Refer to artifacts by logical ID everywhere you write or report:

- a playbook is `restart-loop`, not a file path;
- a playbook script is `restart-loop/restart-reason.sh`;
- a detector is its ID (`restart-loop`).

Physical paths are an implementation detail of the storage layer. Memory text, detector source, and your results
must not contain them. Use `sdo-operator memory ls` to turn an ID into a file you can read.

## Find and apply a playbook

1. Your task lists surfaced playbook IDs, or a finding lists the IDs in `Playbooks`. Otherwise run
   `sdo-operator memory ls --kind playbook`.
2. Read the playbook file that `memory ls` prints for that ID. Scripts live next to it; list them with
   `sdo-operator memory ls --kind script`.
3. Playbooks name resources by role placeholders such as `<TARGET_RESOURCE>`. Bind each placeholder to the live
   resource from the finding's parameter bindings before acting, and verify the diagnosis on the live object first.
4. Report a playbook you applied by its logical ID, and a script by its name, in the structured result field your
   task defines.

## Automatic stages before you

When an incident opens, the controller may already have applied a saved playbook program. Your task then carries
`prior_stages`: per attempt, what it did, what it cleared and why it skipped, and `findings` lists only what
remains. An applied attempt already wrote to the cluster, so check live state first and do not repeat it. A skipped
attempt wrote nothing. Treat "cleared" as a claim until `sdo verify` confirms it.

## Commands

```
sdo-operator memory ls [--kind goal|architecture|playbook|script|detector] [--json]
sdo-operator memory check [--json] [--decision FILE]
```

- `ls` lists artifacts as `kind<TAB>id<TAB>path`.
- `check` runs the exact validator the commit broker runs. Exit 0 means clean, 1 means diagnostics, 2 means a
  usage or infrastructure error. Each diagnostic reads `path:line: rule: message (hint)`. Fix by rule ID: the
  `references/rules.md` file of this skill and of the authoring skills explains each rule.
- Edit the files in your proposal checkout, then run `sdo-operator memory check`. It checks structure, semantics,
  ownership against the accepted base, and builds, vets, and tests detectors and programs, including generated corpus replay. Actor, phase, and base come from
  `SDO_ACTOR`, `SDO_PHASE`, and `SDO_BASE_COMMIT`; missing inputs are errors. Fix diagnostics and repeat until it passes.
- Reflection writes its intended decision JSON to `.git/sdo-reflection-decision.json` or passes `--decision FILE` before checking; the returned decision and playbook IDs must match. A no_change or sdk_gap decision requires a reason and no edits.
- Finish an authoring task only when `memory check` exits 0. Diagnostics you cannot fix mean your
  change is wrong or out of scope: revert it instead of working around the validator.

Read [validator rules](references/rules.md) when a diagnostic mentions a layout, ownership, or schema rule.

## Incident verification

Run `sdo verify` before returning a completed repair. It sends an incident tool
request to the controller, which runs the accepted bounded traffic workload in
the active memory host and independently evaluates current health. Optional
`--workload NAME` and repeated `--scenario ID` select the bounded workload.
Exit codes are 0 healthy, 1 unhealthy, and 3 unavailable. The host is read-only;
only the controller applies plans. Agents must reach accepted memory only through
`sdo` incident tools, never a host socket or a prober URL.
=======
The [generated runtime contract](references/runtime.md) defines decision fields, envelope shape, tool names and capture phases. Read [validator rules](references/rules.md) when a diagnostic names an ownership, layout or schema rule.
>>>>>>> origin/vic-lsh/dev/sdo-improve
