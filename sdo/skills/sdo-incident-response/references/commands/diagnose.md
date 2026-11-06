## diagnose

evaluate a playbook; a match names its fix

```text
sdo diagnose [-h] [--bind NAME=VALUE] [NAME]
```

Evaluate a playbook against live state. A match with a fix prints the command to run. A wrong or missing role is an error that says what the playbook takes.

- `--bind`: a playbook parameter: an object is NAME=Kind/NAMESPACE/NAME; repeat for each

Key JSON fields: `program_runs[].{program_id, program_revision, verdict, can_mitigate, mitigate_command}, program_revisions`.
