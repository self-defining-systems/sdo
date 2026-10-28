## act

record one command; outer shell chains run separately; shows progress and a complete JSON report

```text
sdo act [-h] --resource Kind/namespace/name [--summary SUMMARY] [--irreversible] ...
```

Run one live mutation and record its repair receipt (target, resources, timing, outcome and command line). The command's exit code is returned. Do not hand-write receipts. Report one cause per independently fixable fault and record a separate act for each, declaring only that cause's resources, so each receipt is attributed to its cause; combine only faults that can't be fixed separately. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. SDO options go before --; the command after it runs verbatim, except that a kubectl command may also carry SDO's --json, --summary and --irreversible, which kubectl never defines. A rejected command line returns a one-line hint with the correct form.

- `--resource`: object the command mutates (Kind/name when cluster-scoped); repeat for each one
- `--summary`: one line describing the repair
- `--irreversible`: the mutation cannot be undone
- `act_command`: the mutating command

```text
Example: sdo act --resource Deployment/app/api -- kubectl -n app set image deployment/api api=img:1.2
```

Complete report fields (`details_file`): `actions[].{success, exit_code, receipt, output, failure_reason, hint}`.
