---
name: sdo-shell-authoring
description: Author or refine executable shell detectors and operational notes when shell guidance is selected.
---

Keep useful operational memory as Markdown notes and shell scripts. Use whatever
organization helps future investigations within the owned artifact directories.

Health detectors belong under `.sdo/diagnostics/detectors/health/<id>/` and are
owned by the health judge. Incident detectors belong under
`.sdo/diagnostics/detectors/incidents/<id>/` and are owned by responders during
reflection. Detectors are executable `.sh` files. Playbook notes and scripts
belong under `.sdo/playbooks/<id>/`. Preserve other roles' artifacts.

A detector exits zero when clear. Two consecutive nonzero runs fire an incident
finding. Timeouts and process failures are detector errors and cannot establish
health. Runs are bounded; keep observations focused and output useful evidence.
The detector runner has read-only application access and cannot read Secrets.

Respond using the fired scripts' evidence, live observations, and accepted notes.
Use normal repair tools and independent controller verification. Reflection may
freely improve notes and scripts after an outcome, including lessons from failed
repairs. Explain uncertainty instead of inventing observations.

Edit only the proposal checkout. The broker validates hygiene and ownership and
alone publishes accepted memory. Never store credentials or edit outcome records.
