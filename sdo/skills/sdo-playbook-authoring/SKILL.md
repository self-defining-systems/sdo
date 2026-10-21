---
name: sdo-playbook-authoring
description: Use when writing or refining an SDO playbook - judgement prose, deterministic Go programs, shared helpers and captured replay cases for a fault class. Gives the memory new workflow, validator rules, and a complete example.
---

# Authoring a playbook

A playbook generalizes one verified repair into instructions for the same failure mechanism on different
resources. Read the `sdo-memory` skill first for artifact IDs and the `memory` commands.

## Workflow

1. Look before you add: `sdo-operator memory ls --kind playbook`. Refine an existing playbook when the mechanism
   and repair are the same; create a new one only for a different mechanism or repair.
2. Scaffold: `sdo-operator memory new playbook <id> --incident <incident-id>`. The ID is lowercase letters,
   digits, `-` and `_`, starting with a letter. The command writes the playbook and registers it in the index; do
   not omit the index entry. For a program, use `program scaffold --fault-class <id> --param name=Kind
   [--mitigate] --out .sdo/playbooks/<id>` to create final files directly, then add its logical index entry.
3. Fill in the playbook ([restart-loop example](assets/restart-loop/README.md) is a complete example): front matter unchanged except
   what the scaffold set, then symptoms, diagnosis, repair, and verification.
4. For deterministic programs, read [program authoring](references/programs.md) and keep the README to judgement
   prose with no shell commands or persisted scripts. For prose-only playbooks, add scripts only when they make diagnosis or verification repeatable. Put them in the playbook's `scripts/`
   directory as executable `.sh` files, name each one in the playbook text, and keep them read-only unless the
   playbook explicitly repairs with them.
5. If a detector should surface the playbook, add its ID to that detector's `possiblePlaybooks` and `Playbooks`
   (see `sdo-detector-authoring`).
6. Run `sdo-operator memory check` until it exits 0.

## Principles

- Write only what was confirmed by evidence in the incident, never a guess.
- Describe resources by role with placeholders such as `<TARGET_RESOURCE>`; no concrete names from the incident.
- Diagnosis first: say how to confirm the fault on the live object before repairing.
- Repairs change the source of truth (source manifests, then redeploy), not only live objects.
- Refer to other playbooks and scripts by logical ID, never by file path.
- Read [validator rules](references/rules.md) when a diagnostic names a `playbook.*`, `script.*`, or `text.*` rule.
