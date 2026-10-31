# Scripted worker retirement inventory

The retirement is **in progress**. `adapter/scripted_worker.py` and its
registrations still run. The first replacement is the side-effect-free
`fastloop.reflection_script.reflection_script` generator: it produces version 1
agent scripts for captured-pair learning. The only script-format extension is a bounded tool timeout: the existing
60-second default cannot preserve the old 180-second memory-check deadline. The real Go memory-check contract test now executes its steps
through the agent-script runner.

## Behavior inventory and replacement mapping

| Existing behavior | Required script behavior | Status |
| --- | --- | --- |
| `_command`, `_tool` | Execute commands with exit assertions; parse the first response line and reject tool errors | Existing command/tool steps provide execution; responder translation pending |
| `_now` | Capture turn/action/verification times | Existing `now`, `turn.started_at`, step timestamps |
| `_manifest` | Prefer `kubernetes/`, scan YAML/YML deterministically, skip Helm templates and unreadable/unparseable files; fail when the named accepted object is absent | Responder translation pending |
| `_patch_from_source`: Service | Read `sdo source Kind/namespace/name`; restore accepted Service ports with strategic patch | Responder translation pending |
| `_patch_from_source`: Deployment | Restore containers by name, immutable source image digests, healthy-baseline images for mutable source tags, and declared readiness/liveness/startup probes; await rollout with bounded timeout; reject unsupported kinds | Responder translation pending |
| `_binding_args`, `_matched_plans` | Select only Match verdicts, reject missing IDs/bad binding maps, deduplicate program ID plus canonical bindings, sort binding arguments, collect plans that have IDs, apply matched plans and attribute program IDs | Responder translation pending |
| `_assess_drift`: default | Copy the event, clear prior investigation evidence, default to unsure | Responder translation pending |
| `_assess_drift`: Service | Require one indexed targetPort field, accepted source contradiction, live field matching event, selected pods and integer ports. Probe every selected Pod IP: accepted port must listen and changed port must explicitly refuse. Timeouts, source-port refusal, no pods, named ports, unsupported shapes and read failures remain unsure. Scratch pods need no exec utilities | Responder translation pending |
| `_assess_policy_drift` | Require source absence, one complete spec field matching the live policy, Ingress isolation, no ingress rules, no matchExpressions, an unchanged accepted Service selector/port tuple and selected pod, and no selecting ingress allow policy. Otherwise remain unsure | Responder translation pending |
| `scripted_responder`: scope | Investigate only drift matching a current drift finding's primary resource; ignore prior-incident drift. Preserve unsure labels and suppress source repair for those resources | Responder translation pending |
| `scripted_responder`: repairs | Delete proven source-absent policies; patch proven Service drift. After verification failure, repair modified Services/Deployments before finding resources, deduplicating and excluding unsure/already repaired resources | Responder translation pending |
| `scripted_responder`: verification | Verify once before fallback repairs, then at most 24 attempts with five-second intervals; do not poll when any investigated drift is unsure | Responder translation pending |
| `scripted_responder`: result | Report completed/failed from health, attributed root causes from detector findings and baseline changes only when repairs/plans ran; labeled drift, applied playbooks, typed repair actions with times/resources/reversibility, changes, verification evidence, timing, zero model usage and session ID | Responder translation pending; usage/session should ultimately come from the real worker |
| `_captured_pair` | Compare open/fixed Service ports; require a distinct fixed peer with a nonempty selector, selected pods, ports and corresponding integer container ports or named ports | Implemented in reflection generator |
| `_captured_policy_pair` | Find a removed open policy and deterministically choose a retained same-kind peer; tolerate absent/null policy collections | Implemented in reflection generator |
| `ScriptedReflector`: capture selection | Use detect captures for proven drift, pre-apply otherwise; prefer policy class over targetPort class | Implemented in reflection generator |
| `ScriptedReflector`: no change | Existing diagnose.go in selected class or no supported captured pair gives explicit no_change with reason and no IDs; one accepted class does not suppress another | Implemented in reflection generator |
| `ScriptedReflector`: program | Scaffold correct parameter; include mitigation only for targetPort; install reference sources, replace metadata with responder ownership and originating incident, delete scaffold test | Implemented as tool/write/delete steps |
| `ScriptedReflector`: corpus | Case ID includes first eight incident-SHA256 digits; copy original open/fixed/healthy and recordings; preserve incident/open_at and byte hashes, root-cause IDs, fault and healthy-peer bindings | Implemented as write steps; rejects preexisting case |
| `ScriptedReflector`: publication | Index all playbook directories, run shared memory check with updated decision and logical ID, return updated result | Implemented as write/tool/delete steps; decision written to writable tool exchange outside checkout and removed after validation |
| `main` | Validate role and required task/publication environment; invoke real production workers through agent-script launcher; publish normal typed result and meaningful failures | Worker/caller rewiring pending |

## Direct callers and packaging

- `fastloop/cli.py`: default toy registrations, time-scale guard, responder/reflection replay selection.
- `fastloop/toy.py`: default registration publication when installing the toy controller.
- `fastloop/replay_worker.py`: direct responder executor, reflected program authoring, fake captured Kubernetes API and monkeypatches of worker transport/template paths.
- `adapter/scripted_registrations/{responder,reflector}.json`: replace custom worker entry points with launcher-wrapped production workers.
- `benchmarks/sregym/Dockerfile.worker`: copies worker and both reference programs into scripted image.
- `scripts/build_sdo_images.sh`: scripted-image import smoke.
- `sdo/operator/devmount/devmount.go` and its test: file mount for removed worker.
- `tests/fixtures/sdo/abort_smoke/run.py`: scripted registration publication.
- `experiments/fastloop/README.md`, `docs/testing-guide.md`: commands, replay behavior, registrations and test documentation.

Retain production/benchmark boundaries. Assurance and its tests are outside this
migration's ownership; do not edit them. Generated scripts and run captures must
remain outside the repository.

## Dependent tests

- `adapter/test_scripted_drift.py`: source/listener evidence, scratch-pod probes and ambiguous failures, current-incident scope, policy-isolation evidence and near misses, deletion/result attribution.
- `adapter/test_native_driver_host.py`: lifecycle gate and registration publication.
- `fastloop/test_scripted_worker.py`: real binding shape, source repairs through typed entry points on both seed copies and protobuf casing variants, null/absent policy captures, corpus and decision attribution, selected healthy peers, immutable images, state changes behind Deployment findings.
- `fastloop/test_scripted_worker_contract.py`: real Go memory checker for ordinary and detect captures, real Go responder-tool response shapes. Reflection now exercises generated steps; tool-response test still uses legacy helpers.
- `fastloop/test_scripted_networkpolicy.py`: policy learning, same-kind healthy peer, no mitigator, independent class learning.
- `fastloop/test_replay.py`: captured Service/Deployment repair and attributed result, reflected captured corpus, broker acceptance/rejection.
- `fastloop/test_fastloop_hermetic_run.py`: two-incident controller/adapter flow, source-digest preservation, reflection provenance, accepted publication, program reuse and bindings, receipts.
- `fastloop/test_toy.py`: readiness-probe source restoration and default/custom registrations.
- `fastloop/test_fastloop_cli.py`: registration overrides and acceleration guard.
- `fastloop/test_recording_registration.py`: capability parity of recording and deterministic worker registrations.
- `fastloop/test_scripted_image_build.py`, `sdo/operator/test_dockerfile_benchmark_scope.py`, `sdo/agents/lifecycle/test_smoke_script.py`, Go devmount tests: image contents/build graph/import smoke/mount path.
- `fastloop/test_reflection_script.py`: generator purity, provenance, pair selection, indexing, cleanup and explicit no-change behavior.

## Remaining integration work

1. Express responder decisions as small scripts/generators, preserving conservative drift decisions and live verification; avoid moving the old worker wholesale into another module.
2. Route fastloop, toy installation, replay and abort smoke through the existing launcher and generated registrations. Each script must drive the real worker and provider parser.
3. Migrate remaining assertions to script execution and real-worker evidence, then remove duplicate captured-pair helpers, ScriptedReflector, scripted_responder, old registrations and obsolete image/mount helpers.
4. Rerun all required validation after integration. No Tier 2 incident has been attempted in this preparatory change.
