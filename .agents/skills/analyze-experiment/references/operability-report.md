# Operability report evidence

Acceptance of a deployment, a health commit or an upstream advance is a Go report (`sdo/operator/operability`).
Read it before judging why onboarding or an advance stopped. The report has no verdict field: `Decide` is recomputed
by each reader, so a stored claim never accepts anything.

| Item | Means | Typical checks |
|---|---|---|
| O1 | patches apply, render deterministically, deploy, ready | `O1.patches`, `O1.render`, `O1.deterministic`, `O1.deploy`, `O1.ready` |
| O2 | every live object attributable to accepted source | `O2.images`, `O2.stamps`, `O2.pins`, `O2.inputs`, `O2.resources` |
| O3, O5 | detection coverage and bound sources; memory consistent | coverage and memory-validation findings |
| O4 | health detectors clear, probes pass under synthetic traffic | `O4.detectors` |
| O6 | scope, allowlist, ownership, budget | `O6.scope`, `O6.allowlist`, `O6.budget` |

Stages: `deployment` (O1, O2, O6; gates publication of verified source), `complete` (O1-O6; gates the health commit
and install), `advance` (O1, O2, O4, O6; gates `PublishAdvance`). Check statuses: `pass`, `fail`, `note`, `pending`
(both stop acceptance), `skipped` (a check not run because an earlier one failed; never a pass).

## Where it lives

- Evidence store (outside `.sdo/`): `source-evidence/<sha256(key)>.operability-<stage>.json`; the evidence record
  carries `operability_digest`; source and advance commits carry `SDO-Operability: <stage> sha256:<digest>` (list
  with `git log --format='%H %(trailers:key=SDO-Operability,valueonly)'`). The health commit has no trailer; its
  report names it in `subject.accepted_commit`.
- ConfigMap `sdo-operability` in the control namespace, current state only: `summary.json`
  `{accepted_commit, stage, digest, observed_at, items:[{id, status, first_failed_check, cause, owner}]}` and
  `report.json`, plus `obligations.json`: the open upstream obligations as a JSON list (`kind`, `accepted`, `cause`,
  `owner`, `attempts`, `reason`, `since`, and `held`/`held_owner` once the obligation is a typed hold that is no
  longer retried; `[]` when none; absent until the follower first saves). A new report keeps it.
  Read `kubectl -n <control> get cm sdo-operability -o json`.
- Operate Job termination message `{summary, decision, owner}` when onboarding stops on a report.

## Causes and owners

`patch`, `render`, `render_nondeterministic`, `build` are the deployer's; `rollout` is the deployer's in onboarding
and the responder's in an advance; `scope`, `allowlist`, `budget`, `environment` and `internal` end with a human
(`scope`/`allowlist`/`budget` are the deployer's only in fresh onboarding); `coverage`, `binding`, `memory_invalid`,
`detector_fault`, `probe_unavailable` are the designer's; `source_fault` is the deployer's in onboarding and the
responder's in an advance; `infrastructure` is retried without costing an attempt; `attribution` is an SDO defect.
Two terminal causes end a lifecycle agent task without another round: `agent_not_converging` (the judge or deployer
was rejected twice in a row with the same normalized diagnostics (rule, path, message), or spent its retry budget; the decision owner is the
agent's role, `designer` or `deployer`, the check is `O5.agent_not_converging` / `O1.agent_not_converging`, and the
detail names the repeated rules) and `base_invalid` (the accepted base fails the shared validator before any agent
edit, owner `operator`, check `O5.base_invalid`, zero agent turns spent). Both reach the `SDO-OPERABILITY` stderr line
and the termination message like any terminal operability stop.
`policy` (owner `operator`, check `O6.policy`) is the typed hold of an accepted deployment that violates the
install-time deployment policy in force: the operate Job ends on it, and a controller start (checked on every start,
refresh due or not) publishes it to the `sdo-operability` ConfigMap, keeps detecting, and deploys no upstream
revision until the policy is relaxed or the deployment advanced.
A correction to a designer is typed diagnostics `operability.<check id>`; the count of designer corrections is in
the durable rejection log.

## Typed hold causes (`sdo-upstream-holds`)

Every held upstream revision persists its reason as `cause=<c> owner=<o> items=<O1,O3|-> <hold>: <detail> (<paths>)`
(`items=-` when no item failed). Parse the prefix with `^cause=(\w+) owner=(\w+) items=([A-Za-z0-9,]+|-) `; the text
after it is the hold kind (`rollout`, `health`, `render`, `build`, `merge_conflict`, `patch_conflict`, `shape_change`,
`memory_touched`, `scope`, `ref_rewritten`, `materialize`, `designer_refresh`, `operability`). Advance-mode owners: a
failed rollout or firing Go floor detector (`cause=source_fault`) is the responder's (the cluster is left for ordinary
detection; no agent task is dispatched by a hold), a firing designer-only detector or pending probe is the designer's
(`detector_fault`, `probe_unavailable`), patch/render/build are the deployer's and are held before any cluster effect,
scope/allowlist/environment/unknown are the human's. An offline hold has zero cluster effect; an infrastructure cause is
retried, never held.

An accepted shape-change advance whose designer refresh failed also leaves an open obligation in the `sdo-upstream-obligations`
ConfigMap (one JSON record per source and kind: `kind=designer_refresh`, `accepted` commit, `cause`, `owner`, `attempts`,
`reason`, `since`). Unlike a hold it survives the next poll (upstream equals accepted) and is retried with 30 s..10 min
backoff and at controller start; the record disappears when a refresh succeeds (`upstream_obligation_settled` log).
Retries are bounded (each is a full judge run): after 5 attempts the record gains `held=attempts_exhausted`, `held_owner=designer`,
and at once `held=<cause>` (`agent_not_converging`, `base_invalid`, `policy`) when the refresh ended on that terminal stop
(`upstream_obligation_held` log); a held record is not retried until a newer accepted commit owes it or the controller restarts.
A record that cannot be decoded is logged as `upstream_obligation_quarantined` (cause `record_invalid`, owner `operator`),
kept in the ConfigMap, and does not stop following.

## Toy harness assertion

`benchmarks/sregym/fastloop/toy_scenarios.py` (via `toy_operability.py`) requires, before each scripted push, the
ConfigMap present, stage `complete`, O1-O6 each present with status not `fail`/`pending` and with checks in
`report.json`, and `accepted_commit` equal to an accepted first-parent commit. Its violations appear in
`scenario-<commit>.json` (`violations`, and `operability` holds the summary the harness read). An absent ConfigMap on
a lane installed before the report existed is expected: reinstall the lane.

The scripted broken commit must also be held with a routed prefix: cause `rollout` or `source_fault`, owner
`responder`; `toy_operability.HOLD_ROUTES` is the harness's own per-kind table (checked, never derived from SDO), and a
reason without the prefix is "not typed".

## GitLab release replay records

`fastloop gitlab replay --run-dir <lane>` (`benchmarks/sregym/fastloop/gitlab_replay.py`, releases in
`apps/gitlab/releases.json`) pushes each later chart release to the lane's followed ref (`<namespace>-upstream`) and
writes, in the lane run directory, `gitlab-replay.json` (rewritten after every release) and one
`replay-gitlab-<version>.json` per attempted release. Summary: `baseline` (chart the accepted source held), `planned`,
`preflight` (violations of the steady-lane check, which stops before any push), `stopped` (empty when every planned
release ran), `human_edits`, `agent_tasks`, `wall_seconds`, `passed`, `results`.

Per release: `outcome` is `advanced` (adopted, no agent commit), `repaired` (adopted after agent commits; `repair_commits`
lists actor and changed paths, all under `.sdo/`), `held` (a typed hold: `hold` = kind, cause, owner, items, reason),
`incident` (an incident opened and the revision was not adopted by an agent-free path) or `timeout`. Also
`accepted_commit`, `operability_digest` (the `SDO-Operability` trailer of the advance commit), `agent_tasks` (new agent
request ConfigMaps, the replay's agent-turn count), `wall_seconds` (push to resolution), `human_edits` (new accepted
commits with `SDO-Actor: human`; must be 0), `changed_workloads` (workloads whose running image ID changed),
`pod_failures`, `controller_relaunches`, `expected` (what `releases.json` says the release changes), `operability` (the
ConfigMap summary read after the verdict; it is not refreshed by advances) and `violations`. A hold whose owner is not
deployer, designer or responder, a hold with no routing prefix, an agent owner that never acts (`owner_action` window), an
unfinished owner (`resolution` window), a verdict timeout and an incident in the quiet window set `stop_reason` and end the
replay; the replayer never edits anything for SDO.
