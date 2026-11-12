---
name: sdo-operating
description: Reading and changing an application through SDO's `sdo` command.
---

# Operating with SDO

`sdo detect` summarizes the incident: what opened it, suspects on the failing path (changes before onset, new log
errors, clues), other clues, and learned playbooks as advice. Each `more:` line is the command that expands it.
The app map is `.sdo/arch.md`: components, edges, user operations, what holds data.

- `sdo detect [--finding ID]`, e.g. `sdo detect --finding checkout`
- `sdo source Kind/ns/name`: intended state, e.g. `sdo source Deployment/shop/api`
- `sdo diagnose NAME --bind ROLE=Kind/ns/name`: test a learned playbook, e.g.
  `sdo diagnose targetport --bind service=Service/shop/api`
- `sdo menu [NAME]`: list fixes. `sdo mitigate NAME --bind ROLE=Kind/ns/name`: apply one
- `sdo mitigate ReconcileFromSource --bind ref=Kind/ns/name`: restore an object from accepted source, e.g.
  `sdo mitigate ReconcileFromSource --bind ref=Deployment/shop/api`
- `sdo act --resource Kind/ns/name -- COMMAND`: any other change, e.g.
  `sdo act --resource Deployment/shop/api -- kubectl -n shop set image deployment/api api=img:1.2`
- `sdo verify --wait SECONDS`: has it recovered? e.g. `sdo verify --wait 60`

`sdo COMMAND --help` gives the syntax. Make changes through `mitigate` or `act` so SDO records them; use `kubectl`
for reads. A source fix is a patch (see the patching skill).

Never read Secret values. Label helper Pods/Jobs `sdo.dev/responder-helper=true` and delete them when done.
