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
- `BENCH_OPERATOR` names the operator file (the spec's operator block: `hooks`, `revision`, `baseline`, `seed`, `settings`)
  on **every call of a build, run or release step** (workload verbs, hook verbs and the identity) whenever the spec has an
  operator block, `BENCH_OPERATOR_BLOCK=1` with it. Lanes writes the file before the step's first workload call, so
  `identity`, `app-up` (`check-pack`, `up`, `down`, `images`), `app-snapshot`, `app-reset`, `gate` and `run` see the same
  readable file, including the verbs a build runs before it prepares the operator. A workload that chooses images,
  credentials or seeds from it reads it in any verb; one that finds `BENCH_OPERATOR_BLOCK=1` without `BENCH_OPERATOR`
  must fail by name, never fall back to defaults. A step with no operator block (a spec without one) gets neither.
- A run step also carries the spec's bounds, `BENCH_CAP_SECONDS` (inject to idle) and `BENCH_READY_TIMEOUT_SECONDS`
  (the gate). The workload's `run` verb is bounded by them from inside: a run that outlives ready_timeout + cap + its
  own close-out margin is the adapter's to end, killing its whole process tree and answering invalid with a named
  reason; lanes' own limit on the verb (`hookTimeouts`) is only the backstop for an adapter that stopped answering.
- A cancelled or killed step ends its adapter too: the manager signals the step's whole session (TERM, then KILL
  after a grace), and a workload verb cancelled by its caller gets TERM to its process group and KILL to what is left
  after `adapterTermGrace`.
- The result is the **last non-empty line of stdout**, a JSON value. Earlier lines are progress and go to the log.
  stderr is the log; its last bytes are the reason of a failure.
- Exit 0 is success. Any other exit is a failure of that verb. Retrying is the platform's job: an adapter never
  retries internally.
- Each verb has a time limit (`hookTimeouts` in `internal/bench/hooks.go`; `run active` and `run undo` are bounded at
  20 minutes, not the run's six hours); the platform kills the process group at the limit.

### Workload settings belong to the workload, not the host

A host's `env` is seen by every step on it, whatever workload runs. A workload's own settings (pack maps, helper
paths, tuning knobs) go in the host's `workload_env` instead:

```yaml
hosts:
  spokane1:
    env: {FUZZOPS_REGISTRY_PORT: "5002"}          # every step on the host
    workload_env:
      "benchmarks/sregym/lanes/run.sh workload": {SREGYM_LANES_PACKS: "sgl1=apps/a sgl2=apps/b"}   # only steps of this workload
```

The key is the workload's id: the spec's `workload` string exactly as written (an adapter command is the whole command line; `default` when the spec names none). A key named after the adapter instead matches nothing, and `preflight` fails the host (`workload-env`). A step of that workload gets
its entries on top of `env` (an entry of the same name wins); a step of another workload does not see them. Host-level
work that belongs to no workload (ledger, preflight, reservations) never carries them. A host with no `workload_env`
behaves as before.

"A step of that workload" is every process the platform starts for the workload: the verbs of build, run, release,
recover and unhold steps, and the footprint, grade and identity calls made outside a step. A step takes its workload
from its experiment, or, for a release, recover or unhold whose plan may be gone, from what the lane was prepared with.

### Unsupported verbs

A verb an adapter does not implement answers **exit 64** and/or a last stdout line `{"unsupported": true}`. The
platform then treats the verb as absent (skips it, or runs its own default); it is never a failure. For
compatibility, a subject hook that exits 2 with no stdout (a usage error from an adapter that predates the verb)
counts as unsupported too. A build skips an unsupported `app-up images`, `app-snapshot write` (no fingerprint: a
later reuse of the app copy rebuilds it) and `app-reset recover` (nothing to undo).

## Subject verbs

Run by `<hook_command> <verb>` of `hosts.yaml` for the spec's `operator.hooks`. The first ten are the fuzzops hook
contract, unchanged (semantics in fuzzops `docs/bench.md`, "Hook contract").

| verb | status |
|---|---|
| `check`, `prepare`, `swap`, `idle`, `watch`, `collect`, `snapshot`, `reset`, `teardown`, `transcripts` | implemented by the platform's callers (plus the optional `reset-incidents`) |
| `describe` | called at `plan add` and `add` once per hooks checkout and operator settings, on the manager's host, with `BENCH_OPERATOR` (hooks, revision, settings). Answer: `{"max_startup":"30m","needs_credentials":true,"inputs":{"seed":{"requires":["baseline"]}}}`. `max_startup` is how long a fresh install may report busy (`0s` or absent: none); `needs_credentials` says whether preflight should check a model credential; `idle_confirm` (optional duration) is the gap between the two `idle` answers a settle or a teardown's idle wait needs in a row: a subject whose `idle` cannot change in that gap says `0s` and is not asked again after a wait (absent: the step's poll, 10 s; a busy answer is always polled at the poll); `inputs` are rules on the spec's named inputs (`requires` lists inputs that must also be set). Lanes stores the answer in the experiment's lock (`described`) and reads it instead of the operator settings. **Unsupported (a pin that predates the verb) falls back to the built-in SDO answer, and lanes logs `pre-contract pin` once per checkout** |
| `check-input` | called at `add` / `plan add` and in `lanes preflight` for each of a spec's inputs (`operator.baseline`, `operator.seed`) that is set and no placeholder, on the manager's host. Environment: `BENCH_INPUT_KIND` (`baseline` or `seed`), `BENCH_INPUT_PATH`, `BENCH_INPUT_PHASE` (`add`: no cluster is known yet; `preflight`), `BENCH_INPUT_ALLOW` (comma-separated diagnostics a human asked to allow) and `BENCH_OPERATOR`. Answer: `{"ok":true,"detail":"...","note":"..."}`; `ok:false` refuses the spec with `detail`; `note` says why nothing was checked (not a failure). Lanes runs no check of its own on an input. **Unsupported falls back to the built-in SDO memory check (`memory migrate`) with a `pre-contract pin` log once** |
| `preflight` | called by `lanes preflight` once per hooks checkout per host. Answer: `{"checks":[{"check":"name","ok":true,"detail":"..."}]}`. **Unsupported falls back to the SDO operator's built-in checks** (credential call, sink, proto binding, baseline memory, `sdo-*` images), which is what preflight has always run. A subject that answers `preflight` owns all the checks of its specs: the platform runs none of its own for them, inputs included. In that fallback a spec whose operator settings say `model: stub` (a no-model stand-in agent) gets no credential call (a passing note says it was skipped), and the baseline memory check on a host with no hooks checkout of the pin reports "not checkable here" as a passing note, not a failure |
| `export-artifact`, `verify-artifact`, `reseed` | defined, **not called yet**; every adapter answers unsupported |
| `verify-start` | defined, **not called by lanes yet**. The SDO hooks implement it as the proof that a lane (built or reset in place) equals a fresh install: accepted memory at its prepared commit, no open incident or pending closure, every workload on the base release's images. Answer `{"ok":bool,"problems":[...]}`, exit 1 with problems. The commit-series adapter calls it at position 1 and adds the branch's checks |

The build check waits `max(ready_timeout, max_startup + 10m)` for `idle`: a subject reports busy through its own start-up period, and a lane is never failed for a wait shorter than it. `max_startup` is the subject's `describe` answer (for a pin without `describe`: SDO's `operator.settings.startup_bound`, default 30m).

The `idle` before a teardown (a release, a rebuild that removes the operator) is called with `BENCH_IDLE_FOR=teardown`: the operator is about to be deleted, so the subject answers only whether its agent has stopped writing and not what an inject would need (for example a settled baseline); unset, `idle` is the gate before an inject or the build check. Settle passes `BENCH_OWN_DETECTED_AT`.

## Workload verbs

| verb | operations (`WORKLOAD_ACTION`) | built-in `fuzzops` workload runs |
|---|---|---|
| `identity` | none | `bin/fuzzops version` (JSON: `workload` id when it says one, `revision`, `modified`); the pin check at `add` / `plan add` (on each usable host, through the workload's own identity command) and every step fail if it is not the pinned revision or was built from a modified checkout. The revision is stored in lane and step files as `workload_rev` (files written before read `fuzzops_rev`, and the old key is still written for a rollback) |
| `app-up` | `check-pack`, `up`, `down`, `images` | `pack load`, `app up`, `app down`, `app images` |
| `app-snapshot` | `write`, `verify` | `app fingerprint --out F`, `app fingerprint --against F` |
| `app-reset` | `recover`, `owned` | `recover` (undo a fault a dead run left; exit 2 busy, 3 invalid input); `owned` is unsupported. `owned` answers `{"objects":["kind/name",...]}` (names as `kubectl get -o name` prints them): the cluster-scoped objects the workload's own row start removes and its row recreates, for example an operator it installs at every row. A run step's residue check (cluster-scoped objects beyond the build's baseline: a fault's webhook or ClusterRole makes the cluster be made anew) excuses exactly those and nothing else; it asks only when it found something, so a lane with no residue pays no call. Unsupported: nothing is excused. An answer that cannot be read leaves the residue `unchecked`, which counts as residue |
| `gate` | `check` | `check` (journeys clean for a span) |
| `run` | `run`, `active`, `undo` (answer `{"active": bool}`: whether the fault is still active) | `run` (record JSON on stdout), `active-instance`, `undo-instance` (their `active=<bool>` text is turned into the answer) |
| `grade` | `report` | `report DIR` (`DIR/runs.jsonl` in, `report.md`, `grades.json` out) |
| `footprint` | none | the lane size the workload wants for its app (`BENCH_PACK` is the app), answered `{"cpus":8,"memory":"30Gi"}`. Lanes asks at `add` / `plan add` for a spec that sets no `lane`, on the manager's host, and records the answer in the experiment's lock (`lane`). Unsupported: the platform default (5 cores, 12Gi). The built-in fuzzops workload carries its own sizes (a GitLab pack gets 8 cores and 30Gi); lanes reads no app name. A lock written before keeps its size: a spec with no recorded `lane` still resolves to the same value |

The built-in workload is the default (`workload` unset or `fuzzops` in the spec): it runs `bin/fuzzops` with the
same command lines, environment, process-group and SIGTERM handling as before the contract existed, so existing
specs behave identically (`TestFuzzopsWorkloadKeepsEveryCommandLine` and the fake-fuzzops end-to-end tests). Any
other `workload` value is a command run as an adapter executable; `grade` receives `WORKLOAD_RESULTS=DIR`. A
workload's `run` result is its last stdout line.

A host without a checkout of the workload at a spec's pin cannot take that spec's items. That is a fact about the pin, not
about the host: the host is reachable and its probe answers, so it raises no host alarm. `add` and `plan add` say it
per host, as `<host>: not usable for <workload>@<rev>: no checkout`.

An adapter that wraps the built-in workload (handles the verbs it adds, delegates the rest) finds the command line the
built-in one would run for the call in `WORKLOAD_BUILTIN` (a JSON array: the program, then its arguments); it is absent for a call
the built-in workload does not implement. The adapter then needs no copy of the verb-to-command table.

An experiment whose `sequences` list several seeds runs them in order on one lane, with no reset between them, and each
run is graded under its own operator label (`exec:<hooks>-<name>-r<position>`). `plan results` has one row per run for
such an experiment (`position`, 1-based; the TSV gets a `position` column only when a row has one); a one-run experiment
is one row, as ever.

The workload name is not part of an experiment's identity yet (a pool claim does not compare it); plan the
identity change with the plan engine.

## Status

Implemented here: the calling convention, unsupported handling, the whole workload seam (every former
`bin/fuzzops` call is a verb call), subject `preflight` with fallback, time limits for all subject verbs.
Stubbed (unsupported everywhere, not called): `footprint`, `export-artifact`, `verify-artifact`, `reseed`,
`verify-start`.

## Platform code is not the workload's

The platform's own code, the `deploy/` scripts (lane limits, registry, kind and vcluster lanes, docker shim) and the step
binary, is embedded in the `lanes` binary and installed on each host on first use at `<staging>/lanes-<rev>/` (`deploy/`
and `bin/lanes`; `<rev>` is a hash of the lanes executable; temp directory, then rename; staging gc keeps it). Every
platform script and step-binary path resolves there. Host-configured commands (`cluster_up`, `cluster_down`) are workload/operator
commands: they run in the pinned workload checkout with `LANES_INSTALL` (the install) and `BENCH_WORKLOAD_DIR` (the checkout)
set, and the platform defaults name `$LANES_INSTALL/deploy/...`. The pinned workload checkout is used only for workload verbs and the
workload's own files; `TestPlatformCodeNeverComesFromTheWorkloadCheckout` fails on a platform path joined to a checkout.

## Lane labels and selectors

A host may label its lanes, and a spec may say which lanes it takes:

```yaml
hosts:
  spokane1:
    clusters: [cs1, sgl3]
    lane_labels:
      sgl3: {workload: sregym}       # any key and value; lanes does not interpret them
```

and in a spec (or a plan's spec template) `selector: {workload: sregym}`. A lane takes a build only when every pair of the
spec's selector is among the lane's labels. A labeled lane is not taken by a spec with no selector unless one of its
labels is `shared: "true"`; unlabeled lanes take any spec with no selector, as before, and a spec with a selector never
takes an unlabeled lane. This holds for fresh builds, pool claims and resets alike. It replaces listing order as the way to keep a
spec off a lane prepared for another workload. `lane_labels` may only name lanes in the host's `clusters`.

## Hosts that adopt clusters

A kind host with no `cluster_up` never makes a cluster: its lanes must already exist, with their kubeconfig in the host's
`kubeconfigs` directory. `add`, `plan add` and `host status` say so up front when a lane has none: `host <h> has no
cluster_up and lane <l> has no cluster: set cluster_up (e.g. $LANES_INSTALL/deploy/kind-lane.sh up)`, instead of the step
failing later at recover with "context kind-<lane> does not exist". Hosts that have `cluster_up`, vcluster hosts and hosts whose
clusters exist behave as before.

## What lanes is, and is not

`lanes` is an experimental resource-management and dispatch layer and nothing more.

- It owns resources: hosts, reservations, kernel minimums, health gates, lanes (clusters with limits), pools, the core
  ledger, isolation and starvation.
- It owns dispatch: plans to steps, placement, retries and parking, artifact edges as opaque data, alarms and
  result-row collection.
- It does not know what a run does or how it is graded (the workload adapter: fuzzops, SREGym), or what the subject is
  (the subject adapter: SDO). Whatever it needs to know about either, it asks through a verb; a spec's
  `operator.settings` is opaque to it.

`internal/layerguard` enforces this: the non-test Go of `cmd/`, `internal/` and `deploy/` must not name SDO, an app,
or an SDO configuration field, except what `internal/layerguard/allowlist.json` lists with a reason. The allowlist is a
ratchet: a count that goes down must be lowered in the same change, and no count goes up. Adapter registrations
(files named `adapter_*.go`: a built-in adapter for a workload or subject that predates the contract) are outside the
count; they are listed in the same file and the list only shrinks.
