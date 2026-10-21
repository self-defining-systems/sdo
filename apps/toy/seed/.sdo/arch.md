---
schema_version: 1
generated_at_commit: SOURCE_COMMIT
generated_at: 2026-10-21T00:00:00Z
application: toy
topology_fingerprint: TOPOLOGY_FINGERPRINT
---
# Toy application architecture

Three independent Deployments (frontend, backend, catalog) each expose HTTP 8080
through a same-named Service. All declare containerPort 8080 and local /ready
probes. frontend /journey calls backend:8080. catalog is a healthy peer for
invariant generalization. Source manifests live in kubernetes/. All images are
preloaded, use imagePullPolicy Never, and run without Kubernetes credentials.
