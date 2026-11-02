---
schema_version: 1
owner: responder
fault_class: mongo-auth-bootstrap
originating_incident: mongo-auth-upgrade
---

Diagnose `<POD>` using its recorded MongoDB catalog. Restore the application's missing
user and direct readWrite role, with credentials referenced by container environment name.
Keep unrelated users and existing passwords. An unavailable catalog remains Unknown.
This synthetic fixture models a MongoDB sidecar and an application container in one Pod.
