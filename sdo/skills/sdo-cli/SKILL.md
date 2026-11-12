---
name: sdo-cli
description: Reading and changing an application through SDO.
---

# Operating with SDO

Start with `sdo detect`: failing and passing user tests, suspects on failing paths (findings, changes before
onset, log errors, matching playbooks), then background. Each `more:` is the exact command that expands its line.

The app map is `.sdo/arch.md`: components, edges, operations and their paths, which hold data, and procedures.

| Command | Example |
|---|---|
| `sdo detect [--finding ID]` | `sdo detect --finding 3fa1c2` |
| `sdo source Kind/ns/name` | `sdo source Deployment/app/api` |
| `sdo diagnose NAME --bind ROLE=Kind/ns/name` | `sdo diagnose stale-client --bind client=Deployment/app/api` |
| `sdo menu` | lists fixes as commands |
| `sdo mitigate NAME --bind ROLE=Kind/ns/name` | `sdo mitigate DeletePod --bind ref=Pod/app/api-0` |
| `sdo act --resource Kind/ns/name -- COMMAND` | `sdo act --resource Deployment/app/api -- kubectl -n app scale deploy/api --replicas=2` |

Change things only with `mitigate` or `act`, so SDO records them; plain `kubectl` is for reads.

SDO confirms when users recover (your session then ends), records what you did, and learns.

Playbooks in `sdo detect` are advice: use one only if its `sdo diagnose` and your evidence agree.

Never read Secret values. Label helper Pods/Jobs `sdo.dev/responder-helper=true`; delete them when done.

Returning before SDO confirms recovery escalates; put what you found in your note.
