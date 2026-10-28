---
schema_version: 1
owner: responder
fault_class: webhook-ca-sync
originating_incident: worked-example
description: Copy the serving Secret's CA into a webhook whose caBundle no longer matches.
---

Use when Pod creation fails with an unknown-authority TLS error from an admission webhook.
Sync the caBundle, then check that blocked rollouts create their Pods. A webhook whose
backend has no ready endpoints is a different fault: restore the backend first.
