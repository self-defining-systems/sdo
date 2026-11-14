---
name: sdo-operating
description: Reading and changing an application through SDO's `sdo` command.
---

# Operating with SDO

`sdo detect` reads now and summarizes the incident by candidate node (on the failing paths or upstream, not
cleared by passing operations): changes near onset, state against last healthy, log errors, clues. `more:`
lines expand one. `.sdo/arch.md` is the app map.

- `sdo detect [NAME] [--wait SECONDS]`: NAME is a user test (sends its request now), a detector or an item;
  `--wait` returns when a failing test or alert changes
- `sdo map [path OP | near COMPONENT]`: the map, a path, neighbours
- `sdo changes`, `sdo timeline`: CSV; `--since 30m`, `--component NODE`, `--limit N`; changes name an actor
  (agent is you, sdo, kubernetes, other)
- `sdo source Kind/ns/name`: intended state
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one
- `sdo mitigate ReconcileFromSource --bind ref=Deployment/shop/api`: restore from source

`sdo COMMAND --help` gives the syntax. Use `kubectl` for reads (`exec` to read is fine) and changes. Keep a
source fix with `git commit`.

SDO decides recovery: the opening alerts cleared and stayed clear after your last change settled. SDO then
ends your incident work for its closing turn. Return only to give up; that escalates to a human.

Never read Secret values. Label helper Pods/Jobs `sdo.dev/responder-helper=true`; delete them when done.
