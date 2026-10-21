# Recorded worker result replay

Each JSON file is the `data["agent-result.json"]` value from one parseable
responder or reflection result ConfigMap in
`results/recordings/evidence_smoke*/`.

The fixture names preserve the smoke directory and role. Session and physical
memory paths, along with any secret-like assignment in free text, are redacted.
The typed envelope, IDs, timestamps, evidence shape, and usage counters remain
as recorded. `TestRecordedWorkerResultsReplayThroughContracts` decodes every
file strictly, validates its registration, and sends it through the incident
or reflection result validation path. Keep new fixtures in this format and
redact free text before adding them.
