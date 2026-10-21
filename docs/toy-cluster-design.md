# Toy cluster validation tier

Decision (review requested; revised after live validator evidence): use a
three-service toy application on a single-node kind cluster with pinned Calico. This is tier 2a, between the hermetic controller
gate and hotel-reservation. Timing targets are <60 s from a prepared host to a
ready controller and <10 s warm reset; measurements will distinguish cluster
creation, image preparation, application deployment, and controller startup.

Scores: 5 is best. Bring-up scores assume locally cached images and build tools;
these are estimates until the live validation report is recorded.

| Alternative | Bring-up | Latent path fidelity | Maintenance | Trade-off |
| --- | ---: | ---: | ---: | --- |
| Toy on kind/default CNI | 5 | 2 | 5 | Real pods/endpoints/watches, but the broker refuses reflection without enforced isolation |
| Toy on kind/enforcing CNI | 3 | 5 | 3 | Adds policy coverage, CNI images, installation and readiness overhead |
| Reduced hotel-reservation | 2 | 5 | 2 | Application build/dependency graph and benchmark source coupling remain |
| envtest (apiserver/etcd) | 5 | 2 | 3 | Real API/watch semantics, but no kubelet, endpoints, readiness or networking |

The toy source, manifests, health objective, seed diagnostics, and cluster-state
fault injectors/oracles belong in `apps/toy/`: they are reusable outside SREGym.
Fastloop orchestration and experiment streams remain under
`benchmarks/sregym/`; they may call the toy helpers and production APIs. Production
`sdo/` code must not import either. Hidden fault identities and oracle answers
never enter production tasks or operational memory. The seed contains only the
normal five durable artifact classes and lifecycle validation metadata.

Three independently ready HTTP services expose container port 8080. The frontend
also calls a backend on a separate journey endpoint. Readiness is local so changing
a Service targetPort leaves pods ready and the endpoint-count health detector
clear: the responder must establish the source contradiction. A repeat
on a different service must be detected by the accepted invariant Go program.
A bad image and a broken readiness probe provide health-visible controls.

NetworkPolicy deny is included. The initial kindnet decision was overturned by
the real broker: its isolated validator requires a positive connectivity canary
and an enforced default-deny canary before accepting any program. The unchanged
production check is in `sdo/operator/broker/kubernetes_validator.go`. Calico is
therefore required for this tier, even when the application fault itself does
not involve network policy. No drift feed
or production operational decision behavior is changed by this tier. Scripted
reflection validates learning plumbing; it does not establish model learning.

## Implementation notes

The selected boundary is implemented in `apps/toy/`, with a worker-protocol adapter
in `benchmarks/sregym/fastloop/toy.py`. A prepared kind node image caches runtime
image content; this is a separate, amortized preparation step. Complete manifest
identities must be compared rather than Docker's manifest ID against CRI's config
ID. A cold node without this cache pays the archive transfer cost during startup.

Readiness means the current ready controller Pod has produced a fresh clear
health evaluation and adopted the accepted source. Historical firing records do
not block a currently clear baseline. Toy reset retires the stopped controller's
lease before recreating its trusted Job template, avoiding a 60-second leadership
wait. These are benchmark setup changes; production decisions and the drift feed
are unchanged.
