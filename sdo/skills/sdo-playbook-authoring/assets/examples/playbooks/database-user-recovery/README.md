---
schema_version: 1
owner: responder
fault_class: database-user-recovery
originating_incident: worked-example
description: Recreate a missing application database user, or restore its role, from the Pod's configuration.
---

Use when an application cannot authenticate to its database and the catalog lacks its user.
The user, database and credential env names come from the bound database Pod's environment;
passwords stay references the executor resolves. Restart dependent workloads only if they
do not reconnect on their own.
