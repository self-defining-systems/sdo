---
name: sdo-operating
description: Live diagnosis and repair with SDO.
---

# Operating with SDO

`sdo detect` groups onset changes, healthy-state diffs, log errors and clues by candidate. `.sdo/arch.md` is the map.

- `sdo detect [NAME] [--wait SECONDS]`: NAME is a user test (sends its request now), a detector or an item;
  `--wait` ends early only on an alert-set change or Recovered; use it, not `sleep`
- `sdo map [path OP | near COMPONENT]`: the map, a path, neighbours
- `sdo changes`, `sdo timeline`: CSV; `--since 30m`, `--component NODE`, `--limit N`; changes name an actor
  (agent is you, sdo, kubernetes, other), nearest onset first
- `sdo metrics QUERY`, `sdo logs COMPONENT [--failing]`, `sdo traces`: PromQL, pod logs, traces; compare
  failing with working requests
- `sdo source Kind/ns/name`: intended state
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one
- `sdo mitigate ReconcileFromSource --bind ref=Deployment/shop/api`: restore source

Read captures before repeating slow reads. Opening verdicts are historical. Diagnose live or mitigate with bindings to recheck and apply a match.

Syntax: `sdo COMMAND --help`. Read and write via `kubectl`. Keep source fixes with `git commit`.

At handoff, run `sdo detect` for evidence and refusals.
SDO ends detector-opened work on recovery.

Recovery covers all users. Probes sample user health. Never change state for probe/test identities. If the app serves others,
verify with real traffic or a non-SDO request.

Never print, copy, log or write a credential value anywhere, including findings and memory.
Credentials in a pod's environment or mounts may be used in place: let that pod's client read them to check the failing operation.
Label helper Pods/Jobs `sdo.dev/responder-helper=true`; delete when done.
