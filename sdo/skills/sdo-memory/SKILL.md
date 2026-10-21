---
name: sdo-memory
description: Use when a task touches an application's SDO operational memory (the .sdo directory) - to find or apply a playbook by logical ID, list what memory holds, or validate memory with sdo-operator memory ls/check. Explains what each artifact is and which flows may write it.
---

# SDO operational memory

Operational memory is the `.sdo/` directory of the application repository. Git history is its history. You may
read it freely. You may write it only through an authoring flow your task names (see the
`sdo-detector-authoring` and `sdo-playbook-authoring` skills). Never write it otherwise.

## What `.sdo/` holds

| Artifact | Meaning | Written by |
|---|---|---|
| goal | the human health objective | the human |
| architecture | how the application is built and deployed | the deployer |
| playbooks | how to diagnose and repair a class of fault, with optional scripts or deterministic Go programs, shared helpers and captured corpus | the responder (reflection); the health judge only for the health-objective playbook |
| diagnostics | deterministic Go detectors (health and incident) and synthetic-traffic files | health detectors: the health judge; incident detectors: the responder (reflection) |
| outcomes | what happened in each incident | the controller only; never edit |

## Logical IDs, never paths

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
