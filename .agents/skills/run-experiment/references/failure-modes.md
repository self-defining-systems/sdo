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
