# CI tooling map

- `prepare_go_ci.sh`: compiles the normal and race graphs, downloads modules,
  builds the source-verified CLI, and records the compiled race test inventory.
- `go_cost_plan.py`: deterministic first-fit decreasing allocation of that inventory
  using `operator_test_costs.json` and `runtime_test_costs.json`. Each bin gets 210
  seconds of measured work inside the unchanged five-minute job, leaving 90
  seconds for setup. Unknown tests reserve the full work budget and run in isolation. Capacity grows through
  the generated Actions matrix; no test placement lists are maintained. A single
  test above the work budget is included in the nightly overflow suite.
- `test_operator_shard.py`, `test_runtime_shard.py`: run a numbered computed bin
  (or `nightly`), validating its budget before executing the real race suite.
  Without `--plan`, they compile the inventory and compute a local plan.
- `go_test_timing.py`: streams Go JSON and reports actual active costs, including
  parallel children and excluding pauses. Actual assertion failures still fail.
- `update_go_test_costs.py`: refreshes costs from hosted JSON logs, with run/head
  provenance, retaining fast completions and skipped race-only/process helpers. Use `--log <log> --run <run-id> --head <sha> --write`. Fresh hosted
  costs change capacity automatically on the next run. Removed names are harmless.

`tests/unit/scripts/test_go_cost_plan.py` guards assignment, conservative defaults,
coverage, deterministic output and the over-budget negative control. The workflow
contract tests live in `tests/unit/sdo/operator/test_runtime_shards.py`.

The race preparation publishes the plan as a workflow artifact. Consumers download
that artifact from the same run; the independent normal cache carries only the CLI
and normal build graph. Discovery and execution both use `-race`, preserving tests
with build tags and excluding normal-only latency probes from the race inventory.

Go replay tests use the same computed runtime bins. Go-backed Python contracts
(including recorded provider reflection) use `python_cost_plan.py` and the same
`pack`/budget guard. Normal preparation collects exact pytest node IDs through
`pytest_ci.py` and publishes a separate same-run artifact. Consumers run numeric
bins through `test_go_integration.sh`; `all` remains the exhaustive local mode.
Every report records all pytest phases in a JSON timing artifact. Refresh the
Python cost profile with `update_go_test_costs.py --pytest-costs <reports...>
--run <id> --head <sha> --write`. Unknown and oversized nodes follow the same
isolation/nightly rules as Go tests. No file-name placement list is maintained.

Measured local Go tests can bootstrap missing hosted costs with `--local-log`
(instead of `--log`/`--run`) and `--head <sha>`. Raw local values are retained
separately; until a hosted observation exists, each reserves max(10 seconds,
10 times the local duration). This avoids one setup-heavy job per already
measured tiny test while preserving isolation for genuinely unmeasured tests.
