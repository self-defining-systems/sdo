# Accepted-memory ablation variants

Generate one of `full`, `detectors-only`, or `playbooks-only` in an isolated
checkout outside this repository. The command audits the whole accepted tree
with replay off before pushing. Existing remote branches are never force pushed.
The output branch defaults to `ablate/20261103-<arm>`; `--branch` names another (for example a new date).

```bash
SDO_OPERATOR_ROOT="$PWD/sdo/operator" uv run python -m benchmarks.sregym.ablation \
  --repository git@github.com:vibesys-playground/hotel-reservation-sdo.git \
  --source-branch "$ACCEPTED_MEMORY_BRANCH" \
  --arm detectors-only \
  --directory /tmp/ablate-hotel-detectors \
  --report /tmp/ablate-hotel-detectors.json --push
```

`full` preserves every byte. `detectors-only` removes playbooks (including
scripts and programs), shared helpers and corpus, and preserves detector code, tests and manifests unchanged.
Detector playbook hints are advisory when the playbook catalog is absent. `playbooks-only` removes every health and incident detector;
playbook Markdown, program code, helpers and corpus are preserved byte for byte.
Reports record source and output commits, artifact counts, metadata edits and
whether the verified branch was pushed. No Markdown is rewritten.

These commands prepare memory; incident triggering and frozen-run behavior are
separate fastloop concerns.

## Repeat runs with fixed memory

`fastloop run --agent sdo --ablation-arm {full,detectors-only,playbooks-only}` fetches
`ablate/20261103-<arm>` (or the branch named by `--ablation-branch`) from the problem application's private memory repository,
pins the row-start reset to that commit, forces `--frozen-memory` and replay off,
and installs the supplied memory without deployment or judge agents. Use a
private lane and its credentials hook; shared writers and warm-controller reuse
are rejected. `--ablation-repository` overrides the repository for a local toy
proof. The fetched revision is the setup-lineage boundary as well as the restore seed:
offline variant commits are inputs, and subsequent setup commits still require
broker validation. It is recorded in `ablation-memory.json`; rows carry
`ablation_arm`. Playbooks-only selects external incident admission: after injection
completes the benchmark sends only the generic page (`operator page: users of <app> report errors`) and namespace. Detectors,
startup health/traffic gates, automatic stages and reflection are disabled. The
responder uses live reads and may run programs; its completed result ends the
response, then the benchmark oracle grades. The private outcome stays unconfirmed
and never grants program trust. Timing scope is `external_trigger_to_responder_completion`.

For an existing private hotel lane, one supported invocation is:

```bash
uv run python -m benchmarks.sregym.fastloop run \
  --run-dir "$HOTEL_LANE" --agent sdo --ablation-arm detectors-only \
  --credentials-hook "$CREDENTIALS_HOOK" \
  --problem k8s_target_port_misconfig_hotel_reservation__v_reservation_9999 \
  --run-id "$RUN_ID" -n 1
```

This is a command template requiring the lane and its credentials hook. For a
model-free toy proof, use `up --frozen-memory` to skip judge refresh and pass the
scripted worker registration explicitly on `run`; normal registrations use real
models.
