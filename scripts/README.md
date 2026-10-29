# CI tooling map

PR CI has **four fixed test jobs**: two exhaustive non-Go Python parts and two
native contract jobs. The two compile/cache preparation jobs finish before tests;
only `Required PR CI` is required. Preparation is capped at five minutes, test jobs
at four minutes, and the aggregate gate at one minute. The maximum
executed critical path is ten minutes; runner queue time is outside that bound.

- `check_ci_budget.py` structurally checks the workflow: no computed PR matrices,
  at most four test jobs, dependency critical path <=600 seconds, and contract
  work plus setup margin within the job cap. It runs before test fanout and in
  `check_errors.sh`. Its negative controls add a fifth job or extend the path.
- `ci_policy.json` owns the two contract buckets' fixed capacity (180 seconds of
  measured work each, 60 seconds reserved for setup). Inventory growth never adds
  a PR job. `pr_test_plan.py` keeps the broadest cheap regression set within that
  capacity; every other test is assigned to nightly. Both sets are exhaustive
  and disjoint, and a generated Markdown artifact lists every deferred test.
- `prepare_go_ci.sh` compiles normal production archives and race test archives.
  `go_test_inventory.go` reads Go's generated test tables after `go list -test
  -export -race` and retains vet checks, avoiding discarded test-executable links.
  A real Go fixture checks parity with `go test -race -list`, including build tags,
  external tests, examples and fuzz targets. The portable graph warms only
  `runtime/prober`, the generated traffic binary's dependency, rather than the
  whole Kubernetes runtime. The CLI and bootstrap fixture are cached once.
  `download_go_modules.sh` warms each full module graph through temporary module
  metadata; cache preparation never rewrites the source-bound go.mod/go.sum.
- `go_ci_artifacts.sh` packs prepared compiler/module/tool roots into
  same-run artifacts and restores executable permissions on consumers. Only pushes
  to main or vic-lsh/dev/sdo-improve save shared Go caches; throwaway PR/queue refs
  restore the base and transfer artifacts without competing for its cache quota.
  Consumer jobs work even when the base entry is missing or evicted. Compiler-free
  Python jobs fetch only module sources and tools, excluding the compiler archive.
  `go_ci_race_artifact.py` derives the same fixed plan on the race producer,
  then asks Go for each bucket's selected test dependency exports and publishes
  a separate artifact per bucket. Contract jobs fetch their normal and selected
  race inputs concurrently to avoid adding independent network stalls. It omits stale archives and compiled static
  tools. The consumer verifies that the accompanying plan matches its own.
  Only matching Go action records accompany the exports; consumer Go commands
  retain their own validity checks. A 320 MiB compressed size gate rejects growth
  during preparation; the durable cache is unchanged. A real producer/empty-consumer fixture rejects dependency rebuilds.
- `check_errors.sh --go-only` owns Go static contracts (schema/deadline/fixture
  ratchets, generated Go references, parity and purity) and runs on the prepared
  race graph. `--python-only` owns Python checks and runs behind the compiler
  guard in Python CI. The default local command retains both sets.
- `unit-tools/go` rejects compiler commands in unmarked Python tests, including
  Go invoked by child CLIs. The shared reflection-memory fixture routes all its
  transitive consumers to `go_integration` before marker selection; new compiler
  tests must use that marker. Python unit fixtures reuse the verified prepared CLI.
- `python_go_tools.sh` prepares the real operator, receipt checker and submission
  guard once. Both Python jobs download its tar artifact from their own workflow
  run, verify the operator source stamp, and select the adapters' existing binary
  environment variables. Missing tools or stale source fail before pytest; the
  consumer never rebuilds an adapter as a handoff fallback.
- `go_cost_plan.py` collects the real race inventory, including test/fuzz/example
  names and bounded chaos/explorer cases. `python_cost_plan.py` collects exact
  pytest node IDs. Same-run artifacts bind consumers to those inventories.
- `run_ci_contracts.py` executes a fixed PR bucket or a nightly bucket with the
  real Go and pytest commands. `go_test_timing.py` streams timing diagnostics;
  assertion failures always fail. `pytest_ci.py` records every pytest phase.
- `update_go_test_costs.py` refreshes observed costs from `--log <hosted-log>` or
  structured `--pytest-costs <reports...>`, with `--run <id> --head <sha> --write`.
  It retains subsecond/skip observations. `--local-log` stores raw Go bootstrap
  timings separately; max(10s, 10x local) is reserved until hosted data exists.
  Genuinely unmeasured names reserve a provisional 15 seconds and run in the PR
  (they are admitted before measured tests) until a run records them; unmeasured
  work is never deferred. `ci_policy.json` `protected_patterns` name the memory,
  broker, compile and publication guards, which are admitted before cheap filler.
  No test-name or file-name placement list is maintained.
- `link_cache.sh` + `linkcache/main.go`: a `go -toolexec` wrapper that serves a
  repeated link from `SDO_LINK_CACHE` (key: linker arguments, importcfg and every
  archive's Go build ID). Validating one detector workspace links the controller
  and test binaries, the shared cost of memory/compile tests, and Go caches
  compilation but never links. CI appends `bash scripts/link_cache.sh` to
  `$GITHUB_ENV`; locally `eval "$(bash scripts/link_cache.sh --shell)"`. Without the
  directory, or on any cache error, the real linker runs.
- `test_go_integration.sh all` retains standalone prerequisite/compile warming;
  prepared CI buckets reuse builds instead. Legacy per-module shard scripts are
  local conveniences, not PR matrix inputs.

Nightly owns deferred real-compiler/provider/simulation tests, image builds/smokes,
proto toolchain/lint/codegen drift, full SDK/core compatibility at Go 1.24.5 and
1.26.8, full race protocol contracts, and the existing coverage-guided fuzz campaigns.
The extra short schedule/fuzz replay invocation was deleted: its seeds already run
in the one ordinary test inventory. Nightly does not rerun the same cheap Go/native
nodes selected for PR CI.

`check_model_cli_versions.py` runs in the image's `model-deps` stage after npm
installation. `sdo/operator/model_cli_requirements.json` keeps the image's default
model compatibility list beside its required CLI minimums. The check measures
`claude --version` and `codex --version`, rejects versions below the declared
minimum or different from the Dockerfile pins, and writes both measured versions
and the policy to `/opt/sdo/model-cli-versions.json`. The inherited
`dev.sdo.claude-code.version` and `dev.sdo.codex.version` image labels expose the
verified pins without starting a container. Scripted images omit the model CLIs
and this gate. Focused tests: `uv run --extra test pytest -q -p no:tach
tests/unit/scripts/test_model_cli_versions.py`.

`check_claude_worker.py` qualifies a model worker through one real Opus turn
with the same `danger-full-access` mode used by health authoring. It reads a
small synthetic source fixture and writes a health diagnostic marker through
Bash, then checks the structured result and records provider usage. It proves
CLI/provider startup and shell execution, not detector quality or learning.
Release builds run it inside the built image with `SDO_CLAUDE_SMOKE=1`,
`SDO_CLAUDE_CREDENTIALS_FILE` naming the existing Claude credential file, and
`SDO_OPUS_POOL_DIR` naming the existing host pool (plus its usual capacity/mode
settings). The opt-in is explicit because it spends provider quota; ordinary CI
builds do not read account credentials. The scripted rehearsal adds the same
live lane gate with `--claude-worker-image <production-worker-ref>` and
`--claude-credentials-secret <existing-secret-with-.credentials.json>`. Its Job
inherits a real health worker's pod confinement and records `claude-worker.json`.
