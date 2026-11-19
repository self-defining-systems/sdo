# Warm-lane experiment tools

Use `check-plan` to admit every selected fault before lane work; the
[fault observation tools](../faults/README.md) describe its independent probes.
`preflight` checks the host and frozen images. `run` owns incident injection,
native learning, independent grading and fenced recovery. `sequence` adds a
restartable journal for adjacent first/exact-repeat pairs with private lane memory.

## Pinned-tools lanes

Measurement lanes set `pinned_tools: true` in `fastloop.json` with a digest
reference (`name@sha256:...`) for the controller image. Loading the environment
fails otherwise. The kind nodes keep their dev and shared-memory mounts, but the
controller and the workers it launches run image code.

## Change the application without recreating kind

`up --no-dev-mount --reuse-cluster-only` attaches using the existing lane's
`KUBECONFIG`. Missing cluster, kubeconfig or incompatible node topology stops the
operation; this mode never falls back to cluster creation.

`retire-app` combines a bounded old-app retirement with attach-only deployment:

```bash
uv run python -m benchmarks.sregym.fastloop retire-app \
  --run-dir OLD_ENVIRONMENT --next-run-dir NEW_ENVIRONMENT \
  --next-namespace social-network --next-problem SOCIAL_PROBLEM_ID \
  --seed FRESH_SOCIAL_SOURCE --lease LANE_LEASE --coord COORD_COMMAND \
  --controller-image CONTROLLER --responder-image RESPONDER
```

It checks the current lane lease before writes, refuses unfinished native closure
or workers, pauses an idle native controller, deletes only the old application
namespace and its SDO namespace, and waits for both to disappear. The new app is
deployed on the same cluster and kubeconfig through attach-only `up`. Completion
requires the ordinary `lane_resources` check for the new app. The next environment
retains intent and completion receipts; a partial retirement can be retried with
the same fixed target set. The command never removes infrastructure or unrelated
namespaces. New-app model credentials must be provisioned through the experiment's
existing credential helper; retirement does not read or copy credential data.

## Lane admission and transient baseline observations

`check-plan --plan PLAN --lane-environment RUN_DIR` filters the plan against the
saved lane's topology and emits `runnable_problem_ids` plus a `lane_plan` decision
for every problem. Decisions with `status: not_runnable_on_lane` include the
unsatisfied injector prerequisites. `run` applies the same admission before any
wipe, worker startup or onboarding, and writes `results/RUN_ID/lane-plan.json`.
Worker count comes from the recorded `SREGYM_KIND_WORKER_NODES`; old lanes without
that fact cannot admit faults requiring a worker count. Optional `lane_topology`
in `fastloop.json`, captured by `up` using node metadata, APIService availability
and Calico configuration, records worker/control-plane counts and confirmed cluster
features (`network-policy`, `metrics-server`, `calico-bgp`). Missing feature
proof fails closed. SecretRotation injectors remain excluded until the lane guard
allows their Secret read. This admission never grants production repair authority.

Baseline probe construction retries the complete observation for 90 seconds on
`ObservationUnavailable`, discarding partial references between attempts. If
exhausted before any injection, the result has `infra_error: never_injected_retry`.
A sequence writes `never-injected-N.json` and `retry.json`; restarting it uses a
fresh `-retry-N` result directory and preserves all earlier evidence. Missing,
uncertain or attempted-injection evidence still holds for native recovery.

## Shared image delivery

All lanes publish frozen host image IDs to the harness-owned
`sdo-fastloop-registry` container on the `kind` Docker network. The existing
host-wide image-delivery lock covers registry creation, publication, node pulls
and retention. A content-derived tag locates a manifest; verified cache hits skip
`docker push`. Nodes pull its immutable manifest digest and retain the original
source image names. Manifest bytes, the frozen config ID and each node's CRI
image ID are checked before reporting success. Identical images under different
source names share the same publication. The frozen ID is the config digest on
Docker's classic image store and the manifest digest on its containerd image
store; both are accepted, and nodes are always checked against the config digest.

Lane teardown removes only retired lane-owned registries. It leaves the host
registry and its data intact for other lanes. The host registry is bound to a
loopback port for uploads; nodes reach it through the kind network. An existing
container with that name must prove harness ownership. Registry failures retain
the existing loud, recorded archive fallback.

Each `image_delivery.jsonl` entry includes `registry` metadata: container name,
`uploads`, `cache_hits`, `publication_seconds`, `pull_seconds`, `frozen_ids` and
`manifest_digests`. Total `seconds` includes setup and transfer; retention evidence is recorded
separately. Use the phase fields to distinguish a first publication from subsequent lane delivery.

## Shared source-built app images

SREGym tags each source-built app image `sregym-src-<identity>`, where the
identity digests the build context, Dockerfile, build arguments and platform,
and labels it `sregym.source-identity`. No lane name is part of the ref, so
every lane deploying the same sources resolves the same image. All hosts share
one app-image registry, `FASTLOOP_APP_IMAGE_REGISTRY` (`host:port`; default:
this host's `sdo-fastloop-registry`), as `sdo/app-src:<tag>`. Docker pulls
plain HTTP only from loopback, so another host names a loopback port that
relays to the registry host over the LAN. Before a build, the worker bridge
holds a host lock for that identity and uses the local image carrying it, else
pulls it from the shared registry, else builds and publishes it there. A pulled
image must carry its identity label. An unreachable registry is recorded as
`registry_error` and the lane builds without publishing. Each lookup is
appended to `app_images.jsonl` next to `image_delivery.jsonl`, with its
`source` (`local`, `registry` or `build`), `published`, `pull_seconds` and
`seconds`. Node delivery is unchanged: kind nodes still receive app images
through the host's image-delivery path above.

### Opt-in persistent row preparation

`run --warm-lane` tries to retain the application, controller and standby responder
on a private non-toy SDO lane. It requires a recorded live controller, a live
standby, known injection history without data effects, terminal prior reflection,
and the installed cluster inventory. It pauses intake, synchronizes the accepted
repository back to the host, reconciles changed application resources, and proves
fresh baseline health against that exact revision. The usual injection admission
still runs. Default runs retain the existing full-reset policy. Both run modes
take the same host/lane lock, so a concurrent full-reset caller cannot invalidate
a warm transaction. A competing caller stops before any lane mutation.

An uncertain warm transaction falls back to the ordinary row-start reset; an open
prior incident stops preparation without deleting its owner. Each attempt writes
`results/<run-id>/warm-transaction.json` with phase timings and a typed fallback
reason. `reused: false` describes reset selection, not successful admission. The
warm option currently leaves evidence export synchronous and does not claim the
30-second target; use measured row prelude and gate timings to assess it.

Private warm reuse also requires a sealed passing no-fault setup receipt. The
cluster configuration (including existing RBAC, webhooks, API registrations,
storage classes, volumes and namespaces), plus retained nonsecret infrastructure
objects in other namespaces, must match that receipt. Application and accepted
memory changes are reconciled separately. Missing evidence takes full reset;
preparation never seals a post-fault cluster as a new clean baseline.

Shared-lane preflight verifies the accepted-repository bind on the PV's owning
node before native identity inspection or revision adoption. A node restart can
lose this in-node mount. `repair-repository-bind --run-dir <lane-run-dir>` invokes
the same idempotent bind implementation as initial attachment and verifies its
device/inode readback. It binds only the owning node, never stacks an unexpected
mount, and never resets the accepted checkout. If an already-running controller
does not see the repaired mount, native shared-identity inspection still refuses
admission; controller recovery remains separate.

## Generic fault stream on a steady lane

`gitlab_faults` runs N application-agnostic faults one at a time against a steady lane (`generic_faults.py` chooses
targets from the lane's workload inventory; nothing is named by hand). Each fault: healthy baseline and no open
incident, inject, wait for SDO's incident, wait for its verified closure, re-verify the baseline, record. The harness
reverts a fault only when SDO did not recover it, and says so in the row (`recovery_error`, `harness_reverted`). Rows
are `incidents.jsonl`/CSV `IncidentRecord`s; totals (resolution rate, MTTR, human edits) are `fault-stream.json`.
`--dry-run --inventory LISTING.json` prints the selected faults and their `kubectl` actions with no lane.

Stream labels: `resolved_by_sdo` (closure `success`, or `cleared_without_action` with a learned program applied, since a source residual in the receipt does not undo the repair; or a `cleared_without_action` closure whose faulted object `sdo-deployer` last changed after the injection, i.e. the controller's accepted-source apply restored it, recorded as `restored_by`), `self_healed` (a fault declared `self_heals` whose incident closed `unconfirmed` or `cleared_without_action` with no repair and the baseline back), `cleared_without_sdo_action`, `unresolved`. Credit and `self_healed` both require the healthy baseline to return. Each fault snapshots known incidents after the settle wait, only attributes an incident that opened after its injection (earlier ones are listed as `preexisting_incidents`), and is not injected while an incident stays open past the baseline bound (row `incident_open_before_injection`).

```bash
uv run python -m benchmarks.sregym.fastloop gitlab faults --run-dir LANE --count 10 --seed 1 --dry-run
```

## Vanilla Claude Code arm (`--agent claudecode`)

`run --agent claudecode` runs SREGym's own stock Claude Code agent (no SDO in the lane): the instruction is
SREGym's `clients/claudecode` `build_instruction`, the model defaults to `claude-sonnet-5-5` (`--model`
overrides; Opus 5.5 is `claude-opus-5-5`), and auth comes from the environment or the `env` block of
`$CLAUDE_CONFIG_DIR/settings.json` (the ClaudeGCP Vertex profile). Each row's directory
(`results/<run>/<index>_<problem>/`) holds `session-dir/` (the cell's own persistent CLI config home with every
session and sub-agent JSONL), `transcript.jsonl` (raw stream-json), `claude-code.stderr`, `prompt.txt`,
`clock.json` (notice = page, start, end, timeout, `claude --version`) and `accounting.json` (tokens split into
uncached input / cache read / cache write / output, unique-request count, USD from the shared `ClaudeLedger`
and the agentshim price table; the CLI's own cost is a cross-check). A timeout kills the process group and
keeps the partial transcripts; a row whose session or sub-agent files are missing carries an error and is not
gradable. `claudecode_agent.run_claudecode_cell` is the same executor for a per-cell runner.

## External agent arm (`--agent external`)

`run --agent external --agent-command 'CMD ARGS...'` runs any agent as an arm: SDO knows only the command, its
arguments and environment. The instruction is SREGym's own stock instruction (the same one the vanilla arm gets),
so the comparison is about the agent, not the prompt. Flags: `--agent-label` (on every row), `--agent-env KEY=VALUE`
(repeatable), `--agent-closing-timeout` (turns the closing phase on), `--agent-closing-prompt-file`,
`--agent-state-map` (JSON `{problem_id: state dir}`), `--agent-evidence-grace`, and `--model` (recorded as declared).
An eval runner arm needs no code: map its `vanilla` arm to `external` in `RowSettings.agent_names` and put these flags
in `extra_args`.

One invocation is one phase of one cell (`incident`, then optionally `closing` after the oracle grade, so it never
affects the grade):

```text
CMD ARGS... --phase incident|closing --cell-dir D --prompt-file F --kubeconfig K --deadline-epoch T [--state-in S]
```

- `--prompt-file`: the SREGym notice (incident) or a fixed neutral text (closing; no outcome or grade reaches the command);
- `--kubeconfig`: the filtered proxy's for the incident phase, an *empty* file for the closing phase;
- `--deadline-epoch`: the command stops its agent itself by then and still exports evidence; the arm kills the
  command's process group only `--agent-evidence-grace` (default 90 s) after it, as a backstop;
- `--state-in`: the state directory a previous pass kept for this problem (`--agent-state-map`); copy it, never move it;
- environment: the arm's plus `SDO_EXTERNAL_PHASE`, `SDO_EXTERNAL_SUBMIT_URL` (incident), `SDO_EXTERNAL_STATE_OUT`
  (`D/agent`, where the state to keep belongs) and `KUBECONFIG`. `D` is named after the problem: keep the agent's
  working directory and environment free of it.

The command writes `<D>/<phase>/agent-result.json` (schema `sdo.external-agent-result/v1`):

```json
{"schema": "sdo.external-agent-result/v1", "phase": "incident", "label": "...", "agent_version": "...",
 "started_at": "...", "ended_at": "...", "timed_out": false,
 "requests": "incident/requests.jsonl", "independent_requests": "incident/requests-independent.jsonl",
 "required_evidence": ["incident/events.json"], "problems": [], "state_dir": "agent"}
```

Paths are relative to `D` (the phase directory is searched too) and stay inside it. The request logs are JSONL in
the shape `ClaudeLedger` reads (`{"type": "assistant", "phase": ..., "message": {"id", "model", "usage": <raw
provider usage>}}`, plus `{"type": "request_error"}` lines); a log shared by phases is split by `phase`. The
optional independent log (a recorder outside the agent; its records also carry `http_status` and `complete`) is
cross-checked: the same message ids and token fields, the only allowed difference being responses cut by a timeout
(`complete: false`, priced from the independent record and flagged `partial`).

A cell is not gradable (its row carries an error) when the result file is missing or invalid, a `required_evidence`
file is missing or empty, `problems` is non-empty, the logs or `state_dir` are missing, no request was recorded or the
cross-check fails. Per cell: `<phase>/prompt.txt`, `command.stdout`, `command.stderr`, `clock.json` (notice, start,
end, return code, timeout, backstop kill, label), and `accounting.json` with one row per phase (incident online,
closing offline; uncached / cache read / cache write / output tokens, unique requests, USD from the same price table
as the vanilla arm, error counts, `partial`, cross-check). Rows record `agent_label` and `agent_state_dir`; the
closing phase's tokens are the row's `reflection_tokens`.
