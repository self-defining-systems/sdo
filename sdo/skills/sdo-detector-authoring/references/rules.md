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
- Memory registers at least one detector; the initial health-judge proposal installs detector manifests. Enforced
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
  `apps/v1 ReplicaSet`, `discovery.k8s.io/v1 EndpointSlice`, `networking.k8s.io/v1 NetworkPolicy`,
  `v1 ConfigMap`, `v1 Endpoints`, `v1 Event`, `v1 Pod`, `v1 Service`, the change-only kinds
  `v1 PersistentVolumeClaim`, `v1 ServiceAccount`, `apps/v1 StatefulSet`, `apps/v1 DaemonSet`, `batch/v1 Job`,
  `batch/v1 CronJob`, and the `rbac.authorization.k8s.io/v1` kinds `Role`, `RoleBinding`, `ClusterRole`,
  `ClusterRoleBinding` (they trigger evaluation; the snapshot has no accessor for them), the synthetic-traffic watch
  `sdo.dev/v1alpha1 SyntheticTraffic` (health detectors over traffic workloads only), and the controller-emitted
  `sdo.dev/v1alpha1 ServiceReachability` (detectors over Service reachability dials). Watches are unique and
  non-empty. Memory check runs the controller's own startup validation, so a detector it rejects can never be
  accepted. Enforced by: `manifest.invalid`, `detector.controller-start`.
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

- A detector ships `detector_test.go` with a matching case and near-miss cases, using `sdktest.Snapshot`. The
  tests run during `memory check` and fail it when they fail. Enforced by: `detector.test`.
Advice, not validator-checked: make tests generalize by varying resource names, namespaces, and counts from the
incident that taught the pattern.
- Incident detectors must stay quiet on the controller's healthy reference (pre-incident, or the closure state it
  verified healthy), including required recorded observations; an agent-authored healthy state cannot replace it. Enforced by:
  `detector.healthy-baseline`.
- Health detectors must not report ExternalName Services for missing endpoints; the compile layer runs this
  generated check. Enforced by: `detector.test`.

## Playbooks by ID

- `Spec().Playbooks`, `Finding.Playbooks`, and manifest `possiblePlaybooks` hold logical playbook IDs matching
  `^[a-z][a-z0-9_-]*$`, never paths. Every listed ID names an existing playbook. Enforced by: `manifest.invalid`,
  `ref.unknown-playbook`, `detector.compile`.
- Detector source must not contain physical `.sdo/` paths. Enforced by: `text.physical-path`.

## Ownership

- The health judge owns health detectors and the responder owns incident detectors; neither may add, change, or
  remove the other's. Enforced by: `ownership.protected-detector`, `ownership.denied`.
- The initial health-judge proposal adds health detectors only. Enforced by: `ownership.initial-health-only`.
- A responder keeps the provenance (`originatingIncident`, `originatingCommit`) of an existing detector. Enforced
  by: `ownership.provenance-rewrite`.
- Detector files are changed only in the phase and by the actor the task names. Enforced by: `ownership.denied`.
