# Offline closed-loop checks

From `sdo/operator`, run:

```sh
go run ./cmd/sdo-replay -sdo /app/.sdo -operator "$PWD" -fixtures /fixtures
```

The command stages detector/program registrations with production `builder.BuildWorkspace`, compiles an offline driver, and emits JSONL to stdout plus role-specific outcome tables/timing to stderr. It never starts a controller or opens a kubeconfig. Its build is source inspection and compilation, not the separate broker corpus-provenance acceptance gate.

Each fixture directory contains `sdktest.LoadCorpusCase` inputs: `open.json`, `fixed.json`, `healthy.json`, `recordings.json`, `labels.json`. Add `accepted.yaml` or `accepted.json` (multi-document YAML, JSON array, or Kubernetes List). An explicit empty inventory is `[]`. A shared `-source file` supplies defaults; per-case files override it. Historical manifests for Rollback go in `revisions/<revision>.yaml` or `.json`. Secret manifests are refused.

`root_cause_program_ids` declares expected positive programs. Detector `Spec().Playbooks` links detectors to those programs. Corpus bindings provide the expected object identities; matching detector bindings must agree. The CLI requires a linked matching detector; program-only API checks can omit detectors. Matrix mode is on by default and includes every detector, even those without a registered program. Missing bindings and Unknown fail rather than count as negative evidence. `-matrix=false` checks positive program pairs only.

The existing manifest-backed `Spec().Class` determines detector roles; detector observations include `class`. Health detectors must stay silent on Healthy and fire on Open whenever optional `labels.json` field `health_impact` is true (the default). Set `health_impact: false` for latent faults or healthy-only cases where health silence is allowed. Health firing on another family's Open is overlap and passes, including when impact is marked false. Incident detectors and Diagnose must remain silent/NoMatch on Healthy and other families, and fire/Match on their own family regardless of health impact. Detector family membership comes from Playbooks intersecting root-cause labels. Health `expected_match` follows health impact; incident and Diagnose expectations follow family labels.

JSONL separates trigger lists `health_false_positives`, `health_false_negatives`, `incident_false_positives`, `incident_false_negatives`, and `overlap` (empty lists omitted). The legacy `false_positives` combines the two FP lists and excludes overlap. Unknown and observation errors remain failures without counting as false negatives. Stderr prints five separate tables and counts each (category, case, trigger) once per repetition; linked observations can repeat on program and detector JSONL rows, so raw row lists need the same deduplication when aggregating.

The production PlanExecutor performs all action semantics using a client-go fake seeded from Open. FromSource and source diffs use explicit accepted manifests; FromObserved reads the original frozen snapshot. Fixed never supplies post-state or probe/log answers. Source- and parameter-backed writes must also equal the accepted value at the target path; frozen observed provenance remains valid. Post-state source diffs are recomputed. Unsupported post-plan primitive observations remain Unknown. Fresh post-state must make Diagnose return NoMatch and linked detectors silent; unrelated fields/objects are checked against executor after-images and disclosed controller reactions.

Effects prefixed `real:` identify production executor writes through the fake API. Effects prefixed `simulated:` identify API-server identity metadata, workload rollout status, controller-owned pod recreation and assumed pod readiness. Pod templates come from current owners, not the Fixed fixture. Readiness/rollout completion is assumed only when the executable template matches accepted source (ignoring the restart marker); otherwise the report names `simulated:pod-readiness-unresolved` or `simulated:rollout-unresolved`. Standalone pods have no recreation model. These checks do not prove actual scheduling, image startup, connectivity, or working credentials. Exec has no transport or new RBAC grant and reports refused.

Program statuses: `replayed`, `negative`, `failed`, `refused`. Independent detector rows use `program_id: detector:<id>` and `detected` or `negative` when correct. Exit 0 means all checks pass (overlap allowed), 1 means failures/refusals/false positives/false negatives, and 2 means setup or build error. `-repeat 50` measures repeated replay after one build; distinguish repeated fixture execution from distinct fixture coverage.

Go callers can use `Check`, `Matrix`, `LoadCases`, and `Run`. The SDK cannot import the nested runtime module, so SDK-only `sdktest.CheckMitigation` retains its existing compatibility contract. The runtime checker never uses or emits its legacy skipped-effect result. Neither helper grants live program trust or writes operational memory.

MongoDB catalog actions use the production executor against an explicit catalog simulator seeded only by
successful Open `DatabaseUsers` observations. `EnsureDatabaseUser` projects membership and exact direct
roles while preserving unrelated users; credentials, auth behavior, password changes and arbitrary Exec
remain outside simulation. The exact recorded target auth/shell configuration is required. Projected
catalog observations are available to post-plan diagnoses and linked detectors. `RemoveServicePort`
uses the ordinary conditional Service patch and direct list verification.
