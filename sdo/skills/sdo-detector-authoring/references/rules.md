# Detector rules

Every rule names the validator rule IDs that enforce it. `sdo-operator memory check` prints the ID of each
diagnostic; look it up here.

## Location and registration

- A detector is a directory of Go files plus one `manifest.yaml`: incident detectors under the incidents directory
  of diagnostics, health detectors under the health directory. The directory name is the detector ID. Create it
  with `memory new detector`. Enforced by: `layout.unknown-path`, `manifest.location`, `manifest.missing`,
  `detector.no-go-source`.
- The ID matches `^[a-z0-9][a-z0-9_.-]*$` and is unique across both classes. Enforced by: `id.pattern`,
  `id.duplicate`.
- Memory registers at least one detector; the initial health-judge proposal's resulting tree (base plus diff) holds a `traffic-<probe>` detector for each probe workload the application map declares; every detector Go installs (`memory.PreparedHealthDetectorIDs`: the generic health detectors and `traffic-<probe>` for each health-probe or link-probe workload file) counts as present before Go has written it. Enforced
  by: `detector.none`, `ownership.initial-needs-detectors`.
- `manifest.yaml` is strict YAML: no unknown fields, no duplicate keys. It carries `apiVersion: sdo.dev/v1alpha1`,
  `kind: DetectorManifest`, `sdkVersion: v0.1`, `id`, `constructor` (default `New`), `class`, `owner`, `watches`,
  `interval`, `persistence`, `batching`, `possiblePlaybooks`, and provenance. Enforced by: `yaml.parse`,
  `manifest.parse`, `manifest.invalid`.
- `class: incident` pairs with `owner: responder` and needs `originatingIncident`; `class: health` pairs with
  `owner: health_judge`. Both need `originatingCommit`. Enforced by: `manifest.invalid`.
- `Spec()` in `detector.go` equals the manifest field for field (ID, class, owner, interval, persistence,
  batching, watches, playbooks, provenance). Enforced by: `detector.test`.
- The Go source parses, builds, and passes `go vet`. Every `func` is package-level. Enforced by: `go.syntax`,
  `detector.compile`, `detector.vet`.

## Watchable kinds

- A watch is `{apiVersion, kind}` and must name a kind the controller can load: `apps/v1 Deployment`,
  `apps/v1 ReplicaSet`, `apps/v1 StatefulSet`, `apps/v1 DaemonSet`, `batch/v1 Job`, `batch/v1 CronJob`,
  `v1 PersistentVolumeClaim`, `v1 ServiceAccount`, `autoscaling/v2 HorizontalPodAutoscaler`,
  `policy/v1 PodDisruptionBudget`, `networking.k8s.io/v1 Ingress`, `v1 Node` (cluster-scoped, read-only),
  `v1 Secret` (name, namespace, type, labels and sorted data keys only),
  `discovery.k8s.io/v1 EndpointSlice`, `networking.k8s.io/v1 NetworkPolicy`,
  `v1 ConfigMap`, `v1 Endpoints`, `v1 Event`, `v1 Pod`, `v1 Service`, the change-only kinds
  `v1 PersistentVolumeClaim`, `v1 ServiceAccount`, `apps/v1 StatefulSet`, `apps/v1 DaemonSet`, `batch/v1 Job`,
  `batch/v1 CronJob`, and the `rbac.authorization.k8s.io/v1` kinds `Role`, `RoleBinding`, `ClusterRole`,
  `ClusterRoleBinding` (they trigger evaluation; the snapshot has no accessor for them), the synthetic-traffic watch
  `sdo.dev/v1alpha1 SyntheticTraffic` (health detectors over traffic workloads only), and the controller-emitted
  `sdo.dev/v1alpha1 ServiceReachability` (detectors over Service reachability dials). Watches are unique and
  non-empty. The synthetic `sdo.dev/v1alpha1 ResourceUsage` watch exposes kubelet usage windows. Memory check runs the controller's own startup validation, so a detector it rejects can never be
  accepted. Enforced by: `manifest.invalid`, `detector.controller-start`.
- The interval-driven `sdo.dev/v1alpha1 Logs` watch exposes bounded container-log counts and excerpts through
  `logs.Consumer` and `logs.Require`; declarations validate RE2 patterns and query limits at registration.
  Source errors must propagate as detector errors. Enforced by: `manifest.invalid`, `detector.compile`, `detector.test`.
- `podread.Consumer` exposes bounded in-container command output through `podread.Require`. Declarations need a `v1 Pod`
  watch and pass `podread.Query.Validate` at registration: a role target, a bounded command, at least the minimum
  interval. Source errors must propagate as detector errors. Enforced by: `detector.controller-start`, `detector.test`.
Advice, not validator-checked: watch every kind whose snapshot accessor the detector calls; the controller
evaluates a detector when a watched kind changes.

## Firing and Clearing persistence

- `persistence.firing` is the number of consecutive evaluations a finding must be active before an incident
  opens; `persistence.clearing` is how many consecutive quiet evaluations close it. Both are integers of at least
  1. `batching.severity` is `info`, `warn`, or `critical`; `batching.debounce` is `"0"` or a positive duration;
  `interval` is a positive duration such as `30s`. Enforced by: `manifest.invalid`.
- A responder's new or changed incident detector must use `persistence.firing: 1`: it was taught by an incident
  that already happened, so it fires on first match. Use `clearing: 2` or more to avoid flapping. Enforced by:
  `ownership.responder-firing`.
Advice, not validator-checked: health detectors judge steady state, so use `firing: 2` and `clearing: 2` unless the
health objective needs more.

## Tests

- Compile and vet detector source and tests. Compilation and static registration failures reject memory.
  Enforced by: `detector.compile`, `detector.vet`, `detector.test`.
Advice, not validator-checked: tests and recorded-state replay run as information only. Failing assertions, unavailable observations,
  findings on healthy captures and diagnosis mismatches never reject saving. Inspect their reasons when
  refining a detector; preserve controller-captured evidence and never substitute invented observations.

## Playbooks by ID

- `Spec().Playbooks`, `Finding.Playbooks`, and manifest `possiblePlaybooks` hold logical playbook IDs matching
  `^[a-z][a-z0-9_-]*$`, never paths. Every listed ID names an existing playbook. Enforced by: `manifest.invalid`,
  `ref.unknown-playbook`, `detector.compile`.
- Detector source must not contain physical `.sdo/` paths. Enforced by: `text.physical-path`.
- A finding's named `ParameterBindings` bind only the playbooks it lists, so an incident detector that binds roles
  (`ParameterBindings`, `localize.BindSuspects`) declares the playbooks those bindings are for; a reused playbook
  is listed by every detector whose binding it needs. Enforced by: `detector.unscoped-binding`.
- One incident detector covers every component that fails the same way: a new incident detector that copies an
  existing one apart from IDs, prose and the scenario or workload it names is rejected; widen the existing one to
  bind the component from the failure (`localize.BindSuspects`) while it stays keyed on the cause's own signal. Enforced by: `detector.component-copy`.
- An incident detector that binds roles never adds a component name (an accepted-source object or traffic scenario
  ID) outside its Spec and prose: accept scenario failures (`traffic.RuleIDPrefix`) that match the cause's failure text (`traffic.MatchFailures`) and bind with
  `localize.BindSuspects` ([example](../../sdo-playbook-authoring/references/programs.md#localized-components)).
  Names the accepted version already had are left alone. Enforced by:
  `detector.component-literal`.

## Ownership

- The detection designer owns health detectors and the responder owns incident detectors; neither may add, change, or
  remove the other's. Enforced by: `ownership.protected-detector`, `ownership.denied`.
- The initial detection-designer proposal adds health detectors only. Enforced by: `ownership.initial-health-only`.
- A responder keeps the provenance (`originatingIncident`, `originatingCommit`) of an existing detector. Enforced
  by: `ownership.provenance-rewrite`.
- Detector files are changed only in the phase and by the actor the task names. Enforced by: `ownership.denied`.

## Go-installed generic health detectors

Do not author `service-endpoints`, `workload-health`, `service-reachability`, `cluster-wiring`,
`workload-controllers`, `warning-events`, `node-health`, or `resource-saturation`.
The shared memory check installs these constructors from the SDK. Wiring checks selectors,
protocol-specific declared targets, required template references/keys, PVC binding, Ingress backends,
total NetworkPolicy isolation and local ExternalName aliases. Other controller kinds, persistent
warning events and application-node health also require no application-source expectations.
Saturation covers memory and PVC bytes/inodes (90% or short forecasts); CPU throttling is opt-in
because collecting cAdvisor counters adds a kubelet request and substantial parsing allocation.
Runtime-crash patterns cover Go panic/fatal, Python traceback, Java OOM/thread exceptions and
segmentation faults; `logs.Appeared` supplies transition persistence (firing 1, clearing 2).
Cause detectors batch at warn so cause findings open incidents. Workload unavailability is critical.
Report degradation (slower but still serving) at warn and loss of service at critical: a warn finding
holds an incident's closure only when that incident opened on it.
Use source-specific detectors for config parsing, named peers, journeys, domain logs and metrics.

Detection designers author ordinary health detectors; no detector ID, package, watch set or Spec is reserved
for the human goal. No goal digest or authoring-round metadata is required. Use the health-objective
playbook as the default repair guide and run the shared memory check before returning logical IDs.
