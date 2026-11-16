---
name: sdo-operating
description: Reading and changing an application through SDO's `sdo` command.
---

# Operating with SDO

`sdo detect` summarizes the incident by candidate node (failing paths or upstream): changes near onset,
state vs last healthy, log errors, clues. `more:` expands one. `.sdo/arch.md` is the map.

- `sdo detect [NAME] [--wait SECONDS]`: NAME is a user test (sends its request now), a detector or an item;
  `--wait` returns on change
- `sdo map [path OP | near COMPONENT]`: the map, a path, neighbours
- `sdo changes`, `sdo timeline`: CSV; `--since 30m`, `--component NODE`, `--limit N`; changes name an actor
  (agent is you, sdo, kubernetes, other), nearest onset first
- `sdo metrics QUERY`, `sdo logs COMPONENT [--failing]`, `sdo traces`: PromQL, pod logs, traces; compare
  failing with working requests
- `sdo source Kind/ns/name`: intended state
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one

A listed non-matching playbook may still describe the fix: check its cause, reuse its mitigation if it holds. SDO never applies it.

`sdo COMMAND --help` gives the syntax. Use `kubectl` for reads and changes. Keep a source fix with `git commit`.

SDO decides recovery: the opening alerts cleared and stayed clear after your last change settled; then
it ends your work. Return only to give up; that escalates.

Never print, copy, log or write a credential value, including in findings and memory. A credential already in a
pod's environment or mounted files may be used in place (exec into that pod and let its client read it) to check the
data the failing operation needs. Label helper Pods/Jobs `sdo.dev/responder-helper=true`; delete when done.
