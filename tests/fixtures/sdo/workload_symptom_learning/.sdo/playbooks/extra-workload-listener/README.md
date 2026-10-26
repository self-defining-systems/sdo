---
schema_version: 1
owner: responder
fault_class: extra-workload-listener
originating_incident: workload-symptom-learning
---

Diagnose the unexpected listener configuration on `<DEPLOYMENT>` and remove it.
The associated Service recovers endpoints without a Service write. Observe both
resources; bind the workload repair to its opening finding and replay to NoMatch.
