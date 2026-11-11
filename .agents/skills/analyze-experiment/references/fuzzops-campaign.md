# fuzzops campaign results

A fuzzops campaign runs SDO as fuzzops' attached `exec:sdo-<round>` operator and grades each round against noop and
the `best-fix` ceiling operator (the fault's first reference fix, applied once its evidence shows).

## Layout (in sdo-exp, never in the repository)

```text
<campaign>/<app>/baseline/runs.jsonl            fuzzops suite: screens, noop (>= 3 per instance), best-fix
<campaign>/<app>/lanes/<lane>/lane.json         sdo_version, plan, namespaces, window
<campaign>/<app>/lanes/<lane>/runs/NNN-<template>-<round>/
    record.json                                 fuzzops record (operator exec:sdo-first|repeat|variant)
    run.json                                    round, instance, idle wait before the run, exit code
    exec/<ns>-<time>/state-*.json               controller state snapshots taken by the observer
<campaign>/<app>/lanes/<lane>/memory/.sdo/{outcomes,playbooks}   accepted memory copied after each round
<campaign>/<app>/graded-<version>/{runs.jsonl,report.md,grades.json}
<campaign>/table-<version>.{md,csv}, ratios-<version>.json
```

## Rounds

- `first`: an instance this lane's SDO has not seen (its memory may hold other faults of the same app).
- `repeat`: the same instance on the memory the first round left; `variant`: another instance of the template.
- A lane waits until the controller has no open incident, no pending closure and no queued reflection before
  every run (`campaign.idle_reason`), so the repeat sees reflection's accepted playbook.

## Reading the table

- Verdict, noop and best-fix bad seconds, lost writes and lifecycle times come from `fuzzops report --plain`
  (`grades.json`); SDO runs on another cluster than its baselines, so the interference rule is off.
- `noop->best`: 0 = no better than noop, 1 = as good as the reference fix; blank when the references are within 5 s.
- Tokens, cost, cause class, reach and `auto-applied` come from the outcome records whose detection time falls in
  the run. `auto-applied: yes` means `resolution_path: deterministic` (the learned playbook resolved it without the
  responder); `partial` means a learned plan applied and the responder still ran.
- Repeat/first ratios use users' recovery time, else the escalation time, else SDO's closure time.
- Regenerate with `uv run python -m benchmarks.fuzzops.table <campaign> --fuzzops <checkout>`.
