---
schema_version: 1
owner: responder
fault_class: health-objective-failure
originating_incident: lifecycle-bootstrap
---
# Health objective

The notes Service (port 8080) is the only component; users sign in with a seeded account and use a bearer token.
Probes sign in as seeded users, read their seeded notes, and create a marked note that cleanup deletes and reads back gone.
The app keeps state in memory: a restart loses tokens and created notes, so probes sign in on every iteration.
