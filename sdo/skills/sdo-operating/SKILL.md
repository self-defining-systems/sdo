---
name: sdo-operating
description: Reading and changing an application through SDO's `sdo` command.
---

# Operating with SDO

`sdo detect` summarizes the incident: what opened it, suspects on the failing path (changes before onset, new log
errors, clues), other clues, and learned playbooks as advice. Each `more:` line is the command that expands it.
The app map is `.sdo/arch.md`: components, edges, user operations, what holds data.

- `sdo detect [--finding ID]`
- `sdo changes [--since 30m] [--limit N]`: changes as CSV from before onset to now (default 10m before); `sdo timeline [--since 30m]`: failing tests and workload readiness over time as CSV
- `sdo source Kind/ns/name`: intended state, e.g. `sdo source Deployment/shop/api`
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one
- `sdo mitigate ReconcileFromSource --bind ref=Kind/ns/name`: restore an object from accepted source, e.g.
  `sdo mitigate ReconcileFromSource --bind ref=Deployment/shop/api`
- `sdo verify --wait SECONDS`: has it recovered?

`sdo COMMAND --help` gives the syntax. Use `kubectl` for reads and changes; SDO records every API
call you make. A `kubectl exec` counts as a change; check recovery with `sdo verify`. Keep a source fix with `git commit` (patching skill).

SDO decides recovery: the alerts that opened the incident cleared and stayed clear after your last change settled.
SDO then ends your incident work and moves the session to its closing turn. Returning before that escalates to a
human.

Never read Secret values. Label helper Pods/Jobs `sdo.dev/responder-helper=true` and delete them when done.
