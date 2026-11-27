# The detector for your playbook

A detector makes the next occurrence find your playbook: its finding names the playbook, so SDO tries that one first. It is a learned detector: it never opens an incident or gates an install. Write one only when something specific to this cause exists. A generic symptom (a timeout, "not ready") is not one: add none and say so in your reply.

Write `.sdo/diagnostics/detectors/learned/<id>/` with `manifest.yaml`, `detector.go` and `detector_test.go`. Copy `/opt/sdo/sdo/skills/sdo-playbook-authoring/assets/examples/detectors/crashloop-script-repair/` and change:

- `id`, package name, and `possiblePlaybooks`, `Spec().Playbooks` and each finding's `Playbooks`: your playbook's ID (all three agree; it must exist, written in this same change). `originatingIncident` is this incident.
- The signal: the state your playbook's `Diagnose` tests, read from the snapshot (`snapshot.Deployments()`, `Pods()`, ...), the probe's failure text (`traffic.MatchFailures`) or, for a store's own state (an account, role, setting or table gone), the store-state readings (below). Never a log substring of `Finding.Evidence`. Keep `interval: 30s`, `persistence.firing: 1`: when an incident opens, SDO evaluates the detector at once before trying playbooks, so the interval never delays the match, but an answer slower than the diagnosis deadline (10 s by default) is missed.
- Components: no literal names or ports. A role-less playbook (the usual one) needs no bindings. If it declares a role, bind it from the object that shows the signal (`ObjectRefFrom(...)` of that object, or `localize.BindSuspects` then keep what has the playbook's workload properties: image, ports), as `ParameterBindings` under the role's name.
- `Diagnose` of the playbook still decides; the detector only recognises the cause.

Store state: Go already reads each supported store (`storestate.AdapterFor`: redis, mongodb, postgres, mysql, memcached, consul) and freezes it into the snapshot against the last healthy reading. Detect with `storestate.Changes(view)`: per store Pod, `Changes` lists what differs from healthy (`Change.Key().Kind` is `storestate.KindPrincipal`, `KindNamespace`, `KindSetting`, ...; `Before` set and `After` nil is a removal). Fire on the kind and name your `Diagnose` tests, and skip an entry whose `Error` is set (`storestate.NoBaseline` or a refused read is unknown, not unchanged). Do not write a second reader. Test with `sdktest.Snapshot.PodReads` holding a store-state reading; see `/opt/sdo/sdo/skills/sdo-detectors/references/pod-reads.md`. If your `Diagnose` needs a store read the snapshot lacks, say so in your reply instead of adding none silently.

Test with `sdktest` snapshots, in `detector_test.go` (shape: `/opt/sdo/sdo/skills/sdo-detectors/assets/restart-loop/detector_test.go`):

1. a snapshot with the fault: one active finding whose `Playbooks` lists your playbook;
2. a near miss: the look-alike symptom (same failing workload, different cause or healthy state) yields no finding.

Check: `sdo-operator memory check`; it rejects a detector that names no playbook or one that does not exist.
