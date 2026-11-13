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

# Manifest inventory

Manifest facts only; read application semantics from source where it is shipped.

- Deployment/default/backend (source: kubernetes/backend.yaml)

- Deployment/default/catalog (source: kubernetes/catalog.yaml)

- Deployment/default/frontend (source: kubernetes/frontend.yaml)

- Service/default/backend (source: kubernetes/backend.yaml)

- Service/default/catalog (source: kubernetes/catalog.yaml)

- Service/default/frontend (source: kubernetes/frontend.yaml)

<!-- sdo:application-map:begin -->
```yaml
version: 1
components:
  - {name: frontend, workload: Deployment/frontend, services: [frontend], holds_data: false}
  - {name: backend, workload: Deployment/backend, services: [backend], holds_data: false}
  - {name: catalog, workload: Deployment/catalog, services: [catalog], holds_data: false}
edges:
  - {from: frontend, to: backend, evidence: [config], source: kubernetes/frontend.yaml}
operations:
  - id: frontend-home
    writes: false
    entry: {service: frontend, port: 8080, method: GET, path: /}
    path: [frontend]
  - id: frontend-journey
    writes: false
    entry: {service: frontend, port: 8080, method: GET, path: /journey}
    path: [frontend, backend]
  - id: catalog-home
    writes: false
    entry: {service: catalog, port: 8080, method: GET, path: /}
    path: [catalog]
probes:
  - {scenario: frontend-home, kind: call, operations: [frontend-home]}
  - {scenario: frontend-journey, kind: call, operations: [frontend-journey]}
  - {scenario: catalog-home, kind: call, operations: [catalog-home]}
no_user_records: the toy application keeps no records
```
<!-- sdo:application-map:end -->
