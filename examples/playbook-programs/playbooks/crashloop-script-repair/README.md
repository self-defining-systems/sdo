---
schema_version: 1
owner: responder
fault_class: crashloop-script-repair
originating_incident: worked-example
description: Replace Pods still crash looping after their dependency recovered.
---

Use only after the dependency the Pods failed on is healthy again; otherwise the new Pods
crash too. Fix the dependency first, then run this step and check the Pods become ready.
