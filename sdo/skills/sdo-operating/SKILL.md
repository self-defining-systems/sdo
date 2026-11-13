---
name: sdo-operating
description: Reading and changing an application through SDO's `sdo` command.
---

# Operating with SDO

`sdo detect` summarizes the incident: what opened it, suspects on the failing path (changes before onset, new log
errors, clues), other clues, learned playbooks. Each `more:` line is the command that expands it.
`.sdo/arch.md` maps components, edges, user operations, data holders.

- `sdo detect [--finding ID]`
- `sdo changes [--since 30m] [--limit N]`, `sdo timeline [--since 30m]`: changes and signals as CSV
- `sdo source Kind/ns/name`: intended state
- `sdo try [TEST]`: user tests; one test's request, response, status (read-only)
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one
- `sdo mitigate ReconcileFromSource --bind ref=Deployment/shop/api`: restore an object from accepted source
- `sdo verify --wait SECONDS`: has it recovered?

`sdo COMMAND --help` gives the syntax. Use `kubectl` for reads and changes; reads, including `kubectl exec` to read, are fine. `sdo verify` judges recovery. Keep a source fix with `git commit`.

SDO decides recovery: the alerts that opened the incident cleared and stayed clear after your last change settled.
SDO then ends your incident work for its closing turn. Returning before that escalates to a human.

Never read Secret values. Label helper Pods/Jobs `sdo.dev/responder-helper=true`; delete them when done.
