# Plans

A plan is a declarative `plan.yaml` that the runner executes and recovers: arms x items x reps, edges that carry a
verified artifact, lane-free steps and pins. `lanes plan add FILE` validates it and stores it under
`<root>/plans/<name>/`; the manager (`lanes serve`) ticks every plan on each pass. The engine (`internal/plan`) sits
above the experiments: each node of a plan is one experiment (spec) that it adds to the manager, in plan order, once
its inputs are verified. All state is on disk, so a restart continues where the last pass stopped.

```yaml
plan: tp-gitlab-1                      # at most 24 characters; never reused
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}    # written into every spec (fuzzops, operator.revision)
arms:                                  # declaration order is the order inside an item
  - {name: p1,   spec: specs/p1.json}  # spec template: ${plan} ${arm} ${item} ${item.<var>} ${rep} ${name} ${artifact.<kind>}
  - {name: cold, spec: specs/cold.json}
  - {name: p2,   spec: specs/p2.json}  # operator.seed: ${artifact.memory}
items: [gmem, gfeat]                   # or [{id: gmem, vars: {fault: "..."}}], or {from: items.json, select: key}
reps: 2
order: rep-major                       # rep 1 of every item before rep 2 (item-major: every rep of an item first)
parallel: 6                            # experiments released and not yet ended
isolation: two-way                     # optional; written into every spec
artifacts: {memory: {path: "${run}/operator/learned-baseline", verify: subject}}
edges: [{from: p1, to: p2, artifact: memory}]       # p2 of an item starts after p1 of the same item and rep verified
steps: [{name: merge, after: [p1], per: rep, run: "merge-memories", lane: none, out: "${step_dir}/merged"}]
```

- **Order.** Nodes are `(rep, item, arm)`, arms in dependency-then-declaration order, so every comparison's partner is
  released right after its pair, never behind a whole arm and never by name order. A node blocked on an input is passed
  over; the next ready node takes its slot, and the blocked one goes first as soon as it is ready.
- **Edges.** `from` is an arm (the same item and rep) or a step; `to` is an arm. With `artifact`, the source's artifact is
  exported and verified when the source ends and verified again just before the dependent is released. A failed verify
  fails the source (`artifact:<kind>`); its dependents are skipped. Verify modes: `exists`, `subject` (the subject's
  `verify-artifact` verb, falling back to `exists` when unsupported), `cmd:<shell>` (exit 0; `ARTIFACT_PATH`,
  `ARTIFACT_KIND`, `BENCH_EXPERIMENT` in the environment). Path templates: `${run}` (the source's newest done run),
  `${exp_dir}`, `${step_dir}`.
- **Steps.** A lane-free step runs detached, off any lane, once every node of its `after` arms (that rep's, with
  `per: rep`) has ended; it runs over the done ones (`LANES_INPUTS` JSON, `STEP_DIR`, `PLAN_DIR`). Its exit file, not a
  process table, says how it ended. Exit 0 is success.
- **Engine-owned spec fields.** `name`, `fuzzops`, `operator.revision`, `retries: 0` (only the runner retries) and
  `isolation`. Relative `faults.instances`, `operator.baseline` and `operator.seed` are made absolute against the plan
  file's directory.
- **Lock.** `lock.json` records the pins, the items and the hashes of the stored plan and templates; a changed copy
  refuses to run.
- **Commands.** `lanes plan add FILE`, `lanes plan status [NAME] [--json]` (per arm: done, in-flight, queued, blocked,
  failed, skipped; the next runs; why the first blocked ones wait). `lanes status` prints one line per plan.

Replaces the hand-rolled twopass drivers: `driver.py` (chaining pass 2 after a verified pass-1 archive) and `mkspec.py`.
