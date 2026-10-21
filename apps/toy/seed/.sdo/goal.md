---
schema_version: 1
owner: human
application: toy
---
# Toy application health objective

The frontend, backend and catalog Deployments remain available and their Services
have ready endpoints. Local readiness probes must pass. The frontend /journey
endpoint calls backend and returns HTTP 200 when the dependency is reachable.
Continuous health detection intentionally checks readiness and endpoints only;
latent routing faults require source-grounded drift investigation.
