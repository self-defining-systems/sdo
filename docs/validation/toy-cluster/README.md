# Toy cluster validation, 2026-10-21

The implementation provides the requested real-cluster tier and proves invariant
acceptance and reuse. The cold startup target is still unmet: measured startup
was 129.73 s with a fully prepared node image (138.42 s with a partial cache). Warm reset with Calico took 8.70 s, a final warm repeat took 14.7 s,
and an actual periodic-sweep incident took 42.3 s. All runs used cluster
`sdo-toy`, mounted source, scripted workers and zero model tokens.

## Design decision for review

Use three tiny HTTP services on kind with pinned Calico. The original default-CNI
choice was rejected by live evidence: the production broker's default-deny
validator canary reached the API, so reflection could propose a program but could
not commit it. No production isolation check was weakened. The alternatives and
scores are in [the design note](../../toy-cluster-design.md).

`apps/toy/` owns the reusable source, Kubernetes manifests, seed, injectors and
oracles. Fastloop orchestration and stream definitions stay in
`benchmarks/sregym/`. Production code has no toy or SREGym dependency.

## Integrated architecture recheck

The toy tier was merged onto integrated drift commit `7e005bc52` in merge
`b82765c2d`. The seed now uses schema v3, per-detector manifests, logical
playbook IDs and the canonical `app-diagnostics` module. Its contract test
checks bootstrap stability and the shared structural, semantic and compile
validation layers. Production investigator and reflection core code are unchanged.

The user-authorized benchmark-only follow-up `c9df1e430` makes both policy-list
reads accept absent and null collections. The public scripted-entry-point test
covers all four open/fixed absent/null combinations; it reproduced twelve
failures before the fix. A current scripted-worker dependency image was required
for main's generated Python contracts. The mounted source and operator exercise
the integrated implementation without model calls.

| Recheck | Wall time |
| --- | ---: |
| Cold up | 126.38 s |
| Warm reset, including operator build | 8.95 s |
| First incident, validation and invariant rollout | 132.12 s |
| Backend repeat | 19.02 s |
| Catalog repeat | 16.99 s |
| Image health control | 19.52 s |
| Unowned-Service periodic sweep | 32.24 s |

Cold up used the prepared node's `unified4` images. Preparing/loading the current
worker dependency image and its 71.53 s controller replacement were separate;
the cold measurement does not include that image upgrade.

All five oracle rows have no error, and their correlated strict receipts report
`success` and `sdo_mitigated`. First incident `toy-1792610390596921597` opens on
`drift.investigator` and commits the invariant at
`3685766a33f21561b358985565fe0f7fcd6c392f`. The two service repeats open on
`drift.service-targetport-mismatch` and retain a matching hint binding. The
separate sweep incident `toy-1792610605321991968` records
`source="periodic program sweep"`, no investigator verdict, and hint reuse.
Scripted workers establish integration and reuse, not model learning.

Formatting, static/protobuf checks, all five operator Go modules, the hermetic
gate and Python unit tests pass (1121 passed, 18 skipped). Each invocation was
bounded below ten minutes. The detailed report, `integration-summary.json`,
`schema-v3-run/` and gate logs live outside Git in
`results/toy-cluster-evidence/`.

## Original toy-branch evidence

The raw evidence is kept outside the repository in
`results/toy-cluster-evidence/`: `rows.jsonl`, `outcomes.json`,
`ledgers.json`, `summary.json`, and `manual-oracles.json`.
Outcome records are immutable and precede reflection; reflection commits belong
to the correlated native ledger.

| Required step | Evidence |
| --- | --- |
| First latent incident uses investigator | Incident `toy-1792606985431338397`: finding `drift/investigator`, severity `latent`, investigation verdict `bug`, source contradiction and violated relation, health detector clear, classification `success`, oracle true. |
| Reflection commits invariant | Same incident's ledger: `learning_decision=updated`, outcome commit `ef429e10594cdeec59a7c55859d6b10c5d49ae97`, reflection commit `d447ff8fb8c2aa3a53d906d084b96586ad812326`. The accepted program is `service-targetport-mismatch`. |
| Later services use accepted program | Backend incident `toy-1792607048452102911` and catalog incident `toy-1792607124269891940`: finding `drift/service-targetport-mismatch`, program `binding_source=hint`, `RESULT_MATCH`, classification `success`, oracle true. |
| Actual periodic sweep finds an unowned Service | Incident `toy-1792607631248303314`: drift event source `periodic program sweep`, no investigator verdict, same learned detector and hint binding, classification `success`, oracle true. The fault cleared healthy ownership before setting the actual bad targetPort; no controller feed or scheduling change was made. |
| Health-visible control | Catalog image incident `toy-1792607197353695754`: finding `toy-endpoints`, classification `success`, oracle true. |

The supplied deterministic program was accepted through the real Go controller,
broker, captured corpus, isolated validator and memory edge. This proves the
learning machinery and reuse, not model learning.

## Measured costs

Earlier cold setup (`cold-setup.json`) used a partly cached node image: kind creation
20.47 s, image load/unpack 72.00 s, Calico 18.39 s, app deployment 3.14 s,
controller installation/readiness 22.84 s, preparation 1.57 s. Total: 138.42 s.
The final fully cached measurement (`fully-cached-cold-setup.json`) took 129.73 s:
20.75 s cluster creation, 59.77 s image verification/unpacking, 22.77 s Calico,
3.21 s application, 21.79 s controller, 1.44 s host preparation.
Complete node-image content preparation took 153 s separately.
Reset (`reset.json`) includes a restarted controller with fresh clear evaluations.

| Incident | Detection | Mitigation from injection | Full wall |
| --- | ---: | ---: | ---: |
| First frontend targetPort | 25.1 s | 27.2 s | 87.7 s |
| Backend repeat, before transport cleanup | 0.1 s | 3.0 s | 76.3 s |
| Catalog repeat, before transport cleanup | 0.1 s | 3.0 s | 73.1 s |
| Bad-image health control, before transport cleanup | 0.2 s | 5.7 s | 72.2 s |
| Periodic sweep, after transport cleanup | 24.0 s | 26.6 s | 42.3 s |
| Fresh first incident with transport cleanup | see recorded row | see recorded row | 93.5 s |
| Optimized backend repeat | see recorded row | see recorded row | 14.9 s |
| Optimized catalog repeat | see recorded row | see recorded row | 16.1 s |
| Optimized bad-image control | see recorded row | see recorded row | 18.7 s |
| Final fresh-cluster repeat | 0.1 s | 2.5 s | 14.7 s |
| Separate warm repeat | row timing unavailable | row timing unavailable | 16.2 s |

The last warm outcome is still `success`; its row lacks detection/mitigation
timestamps despite outcome evidence. No production timing/classification change
was made. The first periodic attempt also used a verification deadline of only
60 s after time scaling and expired as the sweep opened its incident; the valid
retry used `--timeout 900 --time-scale 0.1` (120 s verification budget).

## Other live checks

The NetworkPolicy fault made the actual frontend-to-backend journey return
HTTP 503; removing the policy restored HTTP 200. Its state oracle was false
before repair and true after repair. The broken-readiness oracle likewise
rejected the changed probe and accepted its restored spec and completed rollout.
The image fault uses `imagePullPolicy: Never`, so its nonexistent tag never pulls.

## Assumptions and limits

No production hotel namespace, service-list or seed-path assumption was found.
The production requirement that mattered is generic: isolated Go validation
requires enforced NetworkPolicy (`sdo/operator/broker/kubernetes_validator.go`).
Fastloop's hotel-oriented external worker/config selection was bypassed through
a separate worker implementation and a local kind config; production behavior
and the drift feed remain unchanged.

Fastloop assumes flat problem IDs in checkpoint directory handling
(`benchmarks/sregym/fastloop/sdo_agent.py`); toy IDs follow that convention.
Optional investigator replay export still has a missing capture-reference issue
at `benchmarks/sregym/fastloop/replay.py:294`; the toy commands use explicit
`--no-checkpoints`, preserving real receipts, corpus validation and outcomes.

Temporary sync Pods sleep with a 30-second grace, delaying consecutive exports
(`sdo/operator/install/resources.go`, `sdo/operator/install/install.go`). The toy
adapter waits for the real export and deletion request, then removes only that
retired read-only Pod. It does not skip evidence collection. Reset similarly
retires the stopped controller's leadership lease before restarting its trusted
Job template.

The 60-second cold target remains open. Prepared image content reduces registry
and archive work, but unpacking, CNI startup and initial controller build remain.
A prototype embedding unpacked snapshots failed on filesystem device permissions;
it was not added to the implementation.

## Validation

- Python benchmark unit suite: 775 passed, 18 skipped. Twelve skips are explicit
  optional external-harness fixtures absent in this worktree; their assertions
  still run when the external checkout exists.
- Focused fastloop and skill command checks: 193 passed, 1 skipped.
- Full Go operator suite and real broker seed validation passed.
- `scripts/test_fastloop_hermetic.sh` passed.
- Formatting, Ruff and Tach passed. Buf lint and breaking passed; remote Python
  generation was rate limited. Local Go generation matched committed output,
  and no generated files were left deleted or changed.
- The iterate-sdo skill lint and command/path test passed.

The first commit is the design note (`9dc335f4f`); implementation is recorded in
small conventional commits on `vic-lsh/exp/toy-cluster`. No commits were pushed.

## Implementation commits

```text
9dc335f4f docs: decide toy cluster validation tier
b43a399d9 feat: add reusable toy app and fastloop kind lane
60d83e137 fix: initialize toy application baseline before capture
192911d86 fix: reuse the ready toy controller and restore source probes
6a8aa0b4e fix: bind toy operational memory to a real source commit
f1a8087c4 perf: prepare kind image caches for the toy tier
f4291a22e fix: validate the complete toy memory seed before cluster runs
8438e8b89 perf: restart toy resets from the installed controller template
0a49f3077 fix: preserve the kind node entrypoint in prepared images
eebf2cd6c fix: use flat toy fault IDs and reuse verified image manifests
115c267b0 fix: require fresh toy readiness and clear retired reset leases
41ed800fd feat: allow fastloop runs without offline replay checkpoints
0f0d29b3b fix: enforce validator isolation in the toy cluster
6f6460ec1 feat: add toy NetworkPolicy fault injection and state oracle
f63b08b72 fix: preload only the host platform for toy cluster images
d1a2f22c0 perf: poll real Calico readiness every second in the toy lane
c170ee836 feat: exercise periodic invariant sweeps on unowned toy services
94e11f515 test: mark external SREGym fixtures as optional prerequisites
```

## Files added

- `apps/toy/`: Go HTTP app, scratch image definition, three manifest pairs,
  deterministic memory seed, fault injectors/oracles and usage notes.
- `benchmarks/sregym/fastloop/toy{,_cni,_images,_ops,_reset}.py` plus toy selection
  in CLI/environment/dev setup and optional checkpoint export control.
- `tests/fixtures/sregym/fastloop/toy_latent_repeat.json` and `toy_sweep.json`.
- `scripts/prepare_toy_node.sh`, Python contract tests and Go broker seed validation.
- Design, architecture, testing-guide, iteration ladder and analysis-reference
  updates, plus this recorded validation evidence.

The final running cluster retains an accepted invariant program and healthy app.
A node-local source/binary snapshot keeps it functional after host temporary
artifacts are removed. Fresh dev lanes use the normal live worktree mount.
