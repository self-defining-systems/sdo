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

Test with `sdktest.Snapshot.PodReads`; a capture records the readings, so replay sees the same values.
