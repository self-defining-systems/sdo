# Toy cluster validation method

Use three tiny HTTP services on kind with pinned Calico. The production broker's
default-deny validator canary requires enforced network policy; a default CNI
without enforcement cannot validate reflection isolation. See the
[design note](../../toy-cluster-design.md) and
[testing guide](../../testing-guide.md#tier-2a-toy-real-cluster).

`apps/toy/` owns reusable source, manifests, seed, injectors and oracles.
Fastloop orchestration and streams stay under `benchmarks/sregym/`.
Production code has no toy or SREGym dependency.

Validate the first latent incident through investigation, then require an
accepted invariant through the real broker and independent outcome verification.
Repeat faults on different services to check program matching and hint binding.
Test an unowned Service separately to establish periodic sweep detection, and
include a health-visible image control. Correlate rows, outcomes and native
broker ledgers; scripted workers establish integration and reuse, not model learning.

Preserve raw rows, outcomes, ledgers, oracle results and timing measurements in
an external experiment directory. Measure setup, image delivery, reflection and
incident phases separately; missing timestamps mean unknown latency.
