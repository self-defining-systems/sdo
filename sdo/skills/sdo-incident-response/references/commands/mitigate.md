## mitigate

apply a fix by name, with the same --bind as diagnose

```text
sdo mitigate [-h] [--bind NAME=VALUE] [--finding FINGERPRINT] [--rounds ROUNDS] NAME
```

Apply a playbook's fix, or a built-in action such as DeletePod, by name. Take the name and --bind values from sdo diagnose, or omit --bind when sdo diagnose or coverage already matched the playbook once. To apply independent fixes together, separate the commands with --: sdo mitigate A --bind x=Kind/ns/a -- B --bind y=Kind/ns/b. Overlapping writes are refused. Programs use the current checkout; edits compile on demand.

- `target`: playbook or built-in action
- `--bind`: a parameter: an object is NAME=Kind/NAMESPACE/NAME; structured values are JSON text; repeat a list parameter's NAME once per element, or give it a JSON array
- `--finding`: verify against this finding only (default: the findings the playbook explains)
- `--rounds`: maximum rounds within the remaining incident allowance

```text
Example: sdo mitigate targetport --bind service=Service/app/api
More: sdo-incident-response/references/fixes.md
```

Complete report fields (`details_file`): `actions[].{application_id, targeted_healthy, mitigation_health, rollout_pending, program_revisions}, option_reports[]`.
