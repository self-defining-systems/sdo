# Fastloop failure modes

Signs seen in live runs, what they meant, and where to look. Each fix has a regression test; if a sign returns, a test is missing.

| Sign | Meaning | Look at |
| --- | --- | --- |
| Grading or judge crashes on application files | The frozen worktree's SREGym applications checkout is empty | Pass a populated `--sregym-dir` to `up` |
| A learned match is absent from the responder coverage view | Diagnosis did not match a loaded program or the finding has no resource relation to its bindings | The outcome's `playbook_coverage`, `program_runs` and opening finding resources; counters never gate offering or execution |
| A program matched but the responder's `sdo plan` returned an empty plan | The plan read a later snapshot than the match | The outcome's `program_runs` against `program_tool_calls` |
| Verified success counters stay zero although programs are applied | The revision changed or the application lacks independently bound repair evidence | The outcome's `reflection_eligibility`, `plan_applications[].verification` and `program_runs[].program_revision`; counters are information for the responder |
| A latent fault times out with drift seen but no incident | The investigator never ran, or ran blind | Drift deferral reasons in controller status; drift result ConfigMaps (`sdo-drift-*-result`); a verdict reason that mentions blocked `kubectl` or a read-only filesystem |
| The same pre-existing drift is investigated every incident | Verdicts were lost across resets | The drift verdict ConfigMap's age against the control namespace's age |
| `learning failed` with a receipt validation error | Outcome derivation and the receipt disagree on the classification | The shared closure-classification policy and the outcome's `classification`; current receipts do not revalidate diagnosis evidence |
| `learning_decision: failed` with `invalid reflection proposal after 3 attempts` | The proposal shape is rejected at materialization | The error text in the outcome's `reflection_decision`; `sdo-operator proposal validate` on the draft |
| Fail-soft reset loses learned programs or drift verdicts | The reset deleted the repository or the control namespace | Rows after the failed incident; the age of the controller-owned ConfigMaps |
| Reflection checkpoint has no result or model I/O after latent drift | Export stopped on a hard-linked corpus capture; the post-reflection export error was ignored | The resolution's stage-end artifact error and `sdo_runtime/sessions/<incident-hash>/`; export must copy allowlisted hardlink targets and preserve worker checkpoints and every correction turn |
| Provider auth files under `results/` | The session recorder exported credentials | Delete them at once and fix the export allowlist |
| A run waits forever with the host idle | The waiter matches its own command line, or `sleep` was exec'd and lost its marker | `pgrep -af` output for the pattern |
| Builds and tests take many times longer than usual | Caches are on a network filesystem under contention | iowait and processes in disk sleep; the cache environment from host state |
| Baseline traffic fails while every Pod is Ready and every SDO detector is clear | A restore changed the service registry (consul) after the app services registered, so they cannot find each other; waiting does not heal it | Restore from one accepted installed manifest set; the adapter rejects any workload generation, Pod identity or restart change after Ready; independent traffic must pass |
| SDO reports traffic health clear right after a controller restart while real traffic fails | No synthetic scenario has succeeded since the prober started, so none qualifies and the detector has nothing to judge | The adapter requires current controller clear states plus every accepted scenario qualified in the native prober window and independent traffic; `pending` with a reason is unproven |
| A fresh-memory admission guard rejects a slot right after setup | The health judge's setup refresh changed health-owned artifacts; that is normal onboarding, not incident learning | The adapter validates setup actor, phase, validation and health-owned paths; the post-setup commit becomes the memory start |

| A probe triggers unexplained application drift | A benchmark object entered the app namespace | Probe Pods and Wrk2 Jobs/ConfigMaps belong to the harness namespace; admission rejects every benchmark-labelled app object |
| Independently verified health never produces an outcome | The eval paused before pending closure work finished | The adapter keeps SDO active while an incident request or pending closure exists; early-report ordering has a regression test |
| Restored workloads cannot create Pods after a stale lane is adopted | An unlabelled admission fault or generated FailedCreate state survived reset | Reset compares webhook identities to captured/accepted sets and retires stalled generated ReplicaSets, then bounds readiness waiting |
| DNS or PVC-mount timeouts worsen as a warm lane ages | Unbounded observer or app memory, or a leftover app, can starve the shared host | Admission names components exceeding working-set budgets (8Gi per container, 2Gi for observer and app all-in-one Jaeger), low host `MemAvailable`, and foreign namespaces with age. Cap only the benchmark observer; detect app growth without rewriting accepted app manifests |
