---
schema_version: 1
owner: responder
fault_class: deployment-source-drift
originating_incident: worked-example
description: Restore a Deployment whose live spec drifted from accepted source.
---

Use when a detector finding binds a Deployment whose live spec differs from accepted source,
for example an edited resource request, image or replica count. Restore it, then check the
rollout finishes; a rollout that stays pending needs its Pods' scheduling evidence.
