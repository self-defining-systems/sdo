# Pod reads

A detector can judge state only a running process knows, such as a store's runtime config, role or access list, so a
cause-specific detector can fire on the bad state before users are affected. `Detect` never execs: implement
`podread.Consumer`, and the controller runs the command on its interval and freezes the output into the snapshot.

```go
func (d policyDetector) PodReads() []podread.Query {
	return []podread.Query{{ID: "store-policy", Target: podread.Target{Image: `/synthetic-store:`},
		Args: []string{"store-cli", "get", "policy"}, Interval: time.Minute}}
}

func (d policyDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	result, err := podread.Require(view, "store-policy")
	if err != nil {
		return nil, err
	}
	// Judge each reading's Stdout and ExitCode; name reading.Ref() in a finding.
}
```

- Watch `v1 Pod`. Choose targets by role: an image pattern and/or labels the observed Pods carry, never Pod names. An
  image pattern also selects the container; labels alone read the Pod's first container.
- Commands must only read. Print just the field you judge, never a credential value: output is recorded in captures.
  Take credentials from the container's environment through `sh -c`.
- Limits are in the package's constants: a 30 second minimum interval, 3 second timeout, 4 KiB of stdout, 16 Pods per
  query. Stderr is not collected. `podread.Require` is a detector error for a read that is missing, failed, truncated,
  matched no Pod or could not start its command; a nonzero exit is the command's answer. Never turn these into a clear.
- To treat a hung store as the signal, use `podread.ResultFrom`, which also returns failed readings, and judge them.
- A repair shows once the read refreshes, so the closure wait includes `Interval`. Keep it short for detectors that
  verify repairs.

- A target may also name the owning workload as the application map does, `podread.Target{Workload: "StatefulSet/db"}`.
- Each result's `Baseline` holds the same query's complete readings at the controller's last promoted healthy baseline
  (empty until the application has been healthy once). Compare with it to report what changed since healthy.

Go installs a `store-state-<component>` health detector for every map component whose `data.engine`
`storestate.AdapterFor` reads (redis, mongodb, postgres, mysql, memcached and consul), every one of them; it diffs
against the promoted healthy readings. Together they use half of the pod-read load: stores a probed user operation
depends on (on its path, a knownBroken one included, or a registry or config service such a path's components are
mapped to use) read every 30 s while the budget allows, the others less often, but never more than four times less
often (the first group slows down instead). A store it has never read healthy (no CLI
in the image, credentials the container does not expose, a refused read) is a coverage note on the clear verdict, not
a finding. Do not author a second reader for those stores; a store a health detector of yours already reads
through `storestate` (a literal `Component`) gets no Go reader, and two readers that declare one pod read differently
fail `memory check`. For another store, use `storestate.NewDetector` instead of
writing the command: it reads write blocks, role, caps, principals and inventory counts in one bounded read, diffs them
against the controller's promoted healthy readings, and names the changed item in each finding.

`podread.MaxQueries` (16) is a load budget, not a count: a query counts 30 s / its interval, so 32 queries every
60 s fit, and the controller starts at most 16 queries a cycle, rotating the rest (each keeps its last answer until
its turn). At most `podread.MaxDeclaredQueries` are declared. A read starts together with its lookups, so it can have at
most 15.

Test with `sdktest.Snapshot.PodReads`; a capture records the readings, so replay sees the same values.
