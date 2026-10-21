# Toy Kubernetes application

Three tiny Go HTTP servers run as frontend, backend and catalog. Each has a
Service on 8080, a declared containerPort 8080, and local `/ready` readiness.
`frontend:8080/journey` calls `backend:8080/`; each `/` identifies its service.
No registry access, credentials, persistent data, or external harness is needed.

`faults.py` owns the reusable `toy_<kind>_<service>` specs, strategic patch
injectors and cluster-state mitigation oracles. IDs include `toy_target_port_frontend`, `toy_image_catalog`,
`toy_readiness_backend` and `toy_network_policy_backend`; service names are frontend, backend and catalog. The oracle
requires the field to be restored, and for workload faults also requires the
new Deployment generation and exactly one updated, ready replica. Fault labels
and oracle results stay in the experiment driver, outside agent inputs.

`toy_target_port_unowned_catalog` first clears the healthy Service's field
ownership, waits for informer observation, then introduces the real bad port.
It models an imported object and lets the unchanged periodic invariant sweep
find the fault even when the ownership-based drift feed has no event. Use
`toy_sweep.json` after the repeat stream, with `--timeout 900 --time-scale 0.1`
so the full verification deadline exceeds the 60-second sweep interval.

The hand-authored seed uses the normal SDK endpoint health detector and the
schema v3 operational-memory layout, per-detector manifests and logical playbook IDs. It is a deterministic test fixture, not
an attestation of an agent-authored lifecycle. The endpoint health objective
leaves numeric targetPort mismatch latent: Kubernetes still publishes ready
endpoint addresses even though no process listens at that port. The separate
journey endpoint makes the broken dependency observable on demand.

Pinned Calico enforces NetworkPolicy, as required by the production broker's
isolated validator canaries. `toy_network_policy_backend` isolates backend
ingress; its oracle requires the injected policy to be removed. This fault is
provided for network validation; the default scripted latent stream exercises
targetPort drift and an image control. See [the design decision](../../docs/toy-cluster-design.md)
and [fastloop usage](../../benchmarks/sregym/experiments/fastloop/README.md).
