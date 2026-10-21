# Latent stream catalog fixture

`latent_fault_catalog.json` contains metadata, not fault implementations. It
was exported from the real SREGym registry at the recorded `sregym_commit`
using `benchmarks.sregym.fastloop.fault_spec_worker.describe`, then each
registered problem's namespace, injection parameters and mitigation-oracle
class were recorded. Constructors used the same Kubernetes-client patches as
`describe`; injection, recovery and grading were never called.

The target ports, Service selectors and pod-template labels come from
`SREGym-applications/hotelReservation/kubernetes/**/*.yaml` at the recorded
`application_commit`. Refresh both metadata and targets together when changing
the stream to a different SREGym revision. The unit test uses this snapshot so
an empty submodule, unavailable cluster or missing harness dependencies cannot
turn stream validation into a skip.
