# Lane resource admission

`assert_host_headroom()` rejects low host memory before lane creation.
`assert_lane_resources(kubeconfig, app_namespaces={...})` is the read-only
pre-injection guard shared by ordinary runs and the eval dry run. Import both
from the `benchmarks.sregym.adapter` facade. A failure raises `ValueError` with
an `eval:` label and the offending host, component or namespace. The successful
result is JSON evidence suitable for `admission/lane-resources.json`.

The defaults leave at least 16 GiB of Linux host `MemAvailable`, allow at most
8 GiB working set per container and at most 2 GiB for Jaeger all-in-one.
`LaneResourcePolicy` accepts explicit byte budgets for named
`namespace/container` or `namespace/pod/container` components. Defaults are
conservative lane policy, not estimates of each workload's requests. Memory
budgets are checked independently of Kubernetes requests and limits, so an
unbounded or incorrectly limited container still blocks admission.

Namespace admission permits exactly the configured application namespaces,
their matching `-sdo` control namespaces, and the policy's enumerated benchmark
and Kubernetes infrastructure namespaces. Every other namespace fails with its
name and age, including terminating foreign apps. A configured app namespace
must exist and must not be terminating. Configure additional infrastructure
explicitly; a namespace prefix or label cannot exempt an unknown app.

Collection uses only `kubectl get` for namespaces, Pods, nodes and kubelet
`/stats/summary`. Every running regular, init and ephemeral container must have
an exact Pod-UID-bound working-set sample no more than 60 seconds old. Static
mirror Pods bind the kubelet hash through matching file/hash/Node ownership
metadata, since the API mirror has a distinct UID. Missing
or malformed evidence, errors and timeouts block admission. All queries together
have a 15-second deadline, each subprocess at most 5 seconds. No metrics-server,
Docker operation, exec, Secret read or model call is needed. The host floor
measures the local kind host; remote execution must supply the remote host's
measurement to the pure `evaluate_lane_resources` function.

The deterministic fixtures live in
`tests/unit/benchmarks/sregym/adapter/test_lane_resources.py`. Run them before
an expensive lane dry run. The check applies before injection, so injected
resource faults remain benchmark scenarios rather than admission exemptions.

## SREGym pin update

The companion [SREGym PR 12](https://github.com/vic-lsh/SREGym/pull/12) targets
`sdo3`; this branch pins its ready commit `b3214b9aca3e1d47613ca92131f3e1cd9286445d`.
Merge that PR before adopting the SDO pin. The observer now stores at most
10,000 traces with a 256 MiB request and 2 GiB container limit. SREGym applies
the same policy to rendered app-local all-in-one manifests (hotel plain YAML
and source overlays; social-network Helm dependencies). The pinned astronomy
chart already uses Jaeger v2 with 25,000 traces and a 600 MiB limit.

The PVC recovery oracle calls Jaeger's `/api/services`, not historical traces;
span-metric alerts come from the OTel collector before trace storage. Bounded
retention does not change oracle criteria. A trace cap is not a byte cap, so the
container limit provides the hard bound for unusually large traces. Updating
the pin alone does not change existing warm-lane Pods: lane operators own the
rollout, and resource admission rejects unhealthy lanes in the meantime.
