---
schema_version: 1
owner: responder
fault_class: health-objective-failure
originating_incident: toy-bootstrap
---

# Toy health objective

Inspect <AFFECTED_RESOURCE> and its backing Deployment and pods. Compare the live
workload to accepted source before proposing a repair. Independently verify
ready endpoints and rollout completion after restoring the broken field.
