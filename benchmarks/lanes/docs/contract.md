# Adapter contract

`lanes` runs two kinds of adapter, each an executable in any language. A spec picks them; the platform never imports
a benchmark.

- A **workload** owns the app and its grading: bring it up, snapshot and reset it, gate on its health, inject and
  run, grade, report its footprint.
- A **subject** is the operator under test: prepare it on a lane, swap it, tell whether it is idle, collect its
  artifacts, reset it.

## Calling convention

Both roles: **environment in, last stdout line JSON out.**

- The call is `sh -c "<command> <verb>"` in the pinned checkout of the adapter, on the lane's host.
- Everything else is read from the environment. Every call gets the lane variables (`BENCH_LANE`, `BENCH_HOST`,
  `BENCH_EXPERIMENT`, `KUBECONFIG`, `FUZZOPS_NAMESPACE`, `BENCH_PACK`, `BENCH_LANE_DIR`, `BENCH_OUT`, the evidence
  variables and the host's own `env`; the `FUZZOPS_*` names are kept for compatibility). A workload call adds
  `WORKLOAD_VERB`, `WORKLOAD_ACTION` and `WORKLOAD_ARGS` (a JSON array of the operation's options).
- The result is the **last non-empty line of stdout**, a JSON value. Earlier lines are progress and go to the log.
  stderr is the log; its last bytes are the reason of a failure.
- Exit 0 is success. Any other exit is a failure of that verb. Retrying is the platform's job: an adapter never
  retries internally.
- Each verb has a time limit (`hookTimeouts` in `internal/bench/hooks.go`); the platform kills the process group at
  the limit.

### Unsupported verbs

A verb an adapter does not implement answers **exit 64** and/or a last stdout line `{"unsupported": true}`. The
platform then treats the verb as absent (skips it, or runs its own default); it is never a failure. For
compatibility, a subject hook that exits 2 with no stdout (a usage error from an adapter that predates the verb)
counts as unsupported too.

## Subject verbs

Run by `<hook_command> <verb>` of `hosts.yaml` for the spec's `operator.hooks`. The first ten are the fuzzops hook
contract, unchanged (semantics in fuzzops `docs/bench.md`, "Hook contract").

| verb | status |
|---|---|
| `check`, `prepare`, `swap`, `idle`, `watch`, `collect`, `snapshot`, `reset`, `teardown`, `transcripts` | implemented by the platform's callers (plus the optional `reset-incidents`) |
| `preflight` | called by `lanes preflight` once per hooks checkout per host. Answer: `{"checks":[{"check":"name","ok":true,"detail":"..."}]}`. **Unsupported falls back to the SDO operator's built-in checks** (credential call, sink, proto binding, baseline memory, `sdo-*` images), which is what preflight has always run |
| `export-artifact`, `verify-artifact`, `reseed`, `verify-start` | defined, **not called yet**; every adapter answers unsupported |

The build check waits `max(ready_timeout, startup_bound + 10m)` for `idle`: a subject reports busy through its own start-up period, and a lane is never failed for a wait shorter than it. `startup_bound` is `operator.settings.startup_bound` (default 30m, SDO response.startup_bound default; `0s` means no start-up period); the hooks must render the same value into the subject config.

## Workload verbs

| verb | operations (`WORKLOAD_ACTION`) | built-in `fuzzops` workload runs |
|---|---|---|
| `identity` | none | `bin/fuzzops version` (JSON: revision, modified); the step fails if it is not the pinned revision |
| `app-up` | `check-pack`, `up`, `down`, `images` | `pack load`, `app up`, `app down`, `app images` |
| `app-snapshot` | `write`, `verify` | `app fingerprint --out F`, `app fingerprint --against F` |
| `app-reset` | `recover` | `recover` (undo a fault a dead run left; exit 2 busy, 3 invalid input) |
| `gate` | `check` | `check` (journeys clean for a span) |
| `run` | `run`, `active`, `undo` | `run` (record JSON on stdout), `active-instance`, `undo-instance` |
| `grade` | `report` | `report DIR` (`DIR/runs.jsonl` in, `report.md`, `grades.json` out) |
| `footprint` | none | **unsupported**: lane size stays the spec's `lane` limit or the default |

The built-in workload is the default (`workload` unset or `fuzzops` in the spec): it runs `bin/fuzzops` with the
same command lines, environment, process-group and SIGTERM handling as before the contract existed, so existing
specs behave identically (`TestFuzzopsWorkloadKeepsEveryCommandLine` and the fake-fuzzops end-to-end tests). Any
other `workload` value is a command run as an adapter executable; `grade` receives `WORKLOAD_RESULTS=DIR`. A
workload's `run` result is its last stdout line.

The workload name is not part of an experiment's identity yet (a pool claim does not compare it); plan the
identity change with the plan engine.

## Status

Implemented here: the calling convention, unsupported handling, the whole workload seam (every former
`bin/fuzzops` call is a verb call), subject `preflight` with fallback, time limits for all subject verbs.
Stubbed (unsupported everywhere, not called): `footprint`, `export-artifact`, `verify-artifact`, `reseed`,
`verify-start`.
