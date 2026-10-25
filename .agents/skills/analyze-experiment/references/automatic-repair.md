# Agent-selected repair evidence

Every recurrence dispatches a responder. Go diagnoses at incident open and sends `IncidentRequest.playbook_coverage`; outcome field 35 copies that exact snapshot. Removed automatic execution flags and `fast_path_*` fields are not emitted. Native receipts require a correlated successful responder Job.

`PlaybookCoverage.matches` contains every Match (never NoMatch, Unknown or NeedsJudgement), deduplicated by logical playbook ID, program revision and canonical object/value bindings. Match fields are `playbook_id` (1), `program_revision` (2), repeated existing `Binding` messages (3), sorted `explained_finding_fingerprints` (4), `can_mitigate` (5), deterministic menu option ID `mitigation_selection` (6), `verified_successes` (7) and `verified_failures` (8). The option ID selects the same binding from a fresh menu; it grants no write authority. Retired playbooks are excluded by the loader. Counts are information and do not filter matches.

Findings join on primary, related and parameter-bound resources, or direct workload relations such as selected Services and owned Pods/ReplicaSets. `unexplained_finding_fingerprints` lists the sorted remaining active findings. A finding may be explained by multiple programs; all matches remain available.

Coverage is diagnosis, not proof that a program fixed a finding. Join its bindings and explained fingerprints to controller-recorded `plan_applications`, `program_tool_calls`, `repair_actions`, and independently verified final findings. Program options and raw agent actions have separate program IDs (`action:<type>` for raw menu actions). Use executed receipts to attribute repairs; do not count a Match as a repair.

Failed verification retains landed writes. Only explicit responder rollback can undo a completed apply; compensation within a partially failed write batch remains. Defaults are 10 loops, 5 validation retries, 5-minute verification and the enabled mitigation menu. Historical automatic-repair evidence must be interpreted against its original code revision.

Menu applications also appear as typed `plan_applications` with their selected program ID, or `action:<type>` for raw actions. Per-application `program-plan` receipts and per-action `mitigation-menu` receipts can describe the same write at different granularities; count applied actions once, correlated by application/attempt and resource, rather than summing receipt list lengths.
