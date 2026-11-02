## apply

apply a previously planned repair

```text
sdo apply [-h] plan_ids [plan_ids ...]
```

Apply plans returned by sdo plan. An attempt recorded as applied in prior_stages already wrote: re-check live state before applying it again.

Complete report fields (`details_file`): `actions[].{application_id, targeted_healthy, mitigation_health, program_runs, program_revisions, report}`.
