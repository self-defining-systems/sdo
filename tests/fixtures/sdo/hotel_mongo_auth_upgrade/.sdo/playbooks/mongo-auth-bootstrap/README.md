---
schema_version: 1
owner: responder
fault_class: mongo-auth-bootstrap
originating_incident: mongo-auth-upgrade
---

Diagnose `<POD>` using its recorded MongoDB catalog. Restore the application's missing
user and direct readWrite role, with credentials referenced by mounted ConfigMap key and assignment name.
Keep unrelated users and existing passwords. An unavailable catalog remains Unknown.
This synthetic hotel-shaped fixture models a MongoDB Pod with a mounted init script,
no container env, and a separate dependent geo Deployment.

The database container has no credential env. Resolve the mounted init-script key by reference only; restart the bound application after restoring its account.
