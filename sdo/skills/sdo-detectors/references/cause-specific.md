# Cause-specific detectors

A detector is the link that lets SDO apply a learned program without the model: it names the cause, supplies the playbook
and binds the target. Write one for every new or materially changed playbook, unless nothing beyond the generic symptom
distinguishes the cause (then say so in the outcome reason).

- Prefer the most specific signal: the distinctive failure text of the probe (`traffic.MatchFailures` over failing samples,
  never a substring search of `Finding.Evidence`), then a log pattern (`logs.Query`), then a state read (resource state,
  metrics, runtime reads). A timeout or "not ready" alone is the generic symptom, not a cause.
- Do not route a broad restart-type playbook from a generic symptom: it would offer the restart for every suspect.
- Take the component from the failure, never a literal name: bind suspects with `localize.BindSuspects`, then keep only
  workloads the playbook can act on (by image, ports or another property of the workload, not by name). Pass the
  application's probe workload name (a file under the traffic workloads directory), not an assumed one.
- Make the failure text and patterns specific to the cause and free of component names; the validator rejects component literals.
- Test a match, and a near miss: the same symptom with a different failure text, or a workload of the wrong kind, stays quiet.
- Each playbook Diagnose still confirms its own fault class on the bound target and returns NoMatch on the wrong kind; the
  detector narrows, it does not replace that check. List the playbook in `possiblePlaybooks`, Spec `Playbooks` and the finding.
