## diagnose

evaluate a bound program

```text
sdo diagnose [-h] [--bind NAME=Kind/namespace/name] [program_id]
```

Program commands use the current checkout, including shared libraries and new playbook IDs. Edits compile on demand; fix compile errors in place and retry. Revision receipts use working-tree:<source digest> for unpublished code. Reusable API reads use program.Kubernetes(view) with upstream client-go options; reads are recorded for replay.

- `--bind`: named program parameter; repeat for each binding

```text
Example: sdo diagnose targetport --bind service=Service/app/api
```

Key JSON fields: `program_runs[].{program_id, program_revision, verdict, can_mitigate}, program_revisions`.
