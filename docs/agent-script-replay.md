# Agent-script replay inventory

Agent scripts are the only no-model agent execution path. This inventory separates
execution from reading evidence; detector/program corpus replay is a separate
production validation contract and is unchanged.

| User | Need | Decision |
| --- | --- | --- |
| `fastloop.cli` responder/reflection stages | Rerun a recorded agent turn | Convert model I/O to scripts; run current workers through the stand-in. Remove agent selection and live cache fallback. |
| `fastloop.cli` broker stage, `replay_go` | Validate recorded worker edits/results with Go | Retain isolated real broker validation; no model execution. |
| `fastloop.replay_worker` | Execute worker and captured commands | Use stand-in CLI; retain generated scripts for synthetic worker tests. Remove recorded-event executor injection. |
| `fastloop.model_recording` | Capture live turns | Keep write-only `ModelRecorder`; delete lookup, digest matching, ordered event playback, fake process handle and cache-miss fallback. |
| `fastloop.recording_worker`, `recording_registrations` | Capture live production-worker inputs/outputs | Retain recording-only registrations; these cannot replay. No separate replay registrations. |
| `fastloop.replay`, checkpoint export, `file_edits` | Retain tasks, results, captures, edits | Keep bounded content-addressed storage. Convert each exported dispatch into `script.json`. Archived conversion uses temporary storage. |
| `agent_script.converter` and CLI | Convert recordings into executable actions | Keep one converter and one stand-in; commands execute again, file edits become write/delete actions. |
| `analysis.reflection_replay` | Execute a fresh model reflection from a saved workspace | Delete alternate execution tool; preserve ledger/delta readers and their tests as `reflection_artifacts`. Use stage scripts for offline execution. |
| `analysis.reflection_prompt_replay` | Read prompts and estimate size/cost changes | Keep: no agent execution or model call. |
| `analysis.incident_cost`, `stream_curve`, `run_validity` | Read rollouts, receipts and outcomes | Keep evidence readers unchanged. |
| `run-experiment`, `test-sdo` skills | Select validation/replay workflow | Point offline worker replay to scripts; retain live experiment and detector replay instructions. |
| `analyze-experiment` skill and benchmark-results reference | Explain artifacts and broker evidence | Describe scripts, preserve prompt/tool/outcome analysis and independent verdict checks. |
| Analyze references: telemetry, outcome-records, native-broker, fastloop-results, failure-patterns, drift-failures | Read evidence or explain production corpus validation | Retain; these do not replay model turns. |
| Run-experiment failure-modes reference | Diagnose recorded evidence | Retain; no recorded-turn execution. |
| Testing guide and fastloop README | Document stage replay and checkpoints | Replace alternate agents/cache fallback with script export/conversion and explicit missing-turn failure. |
| Architecture, lifecycle-state-machine, feature-flags, validation/toy-cluster and design docs | Describe boundaries, capture evidence or program validation | Update architecture replay boundary; retain unrelated evidence/program replay contracts. |
| `test_model_recording` | Capture fidelity, correction ordering, exhaustion | Replace cache assertions with live capture → converter → script selection/execution and exhaustion. |
| `test_recorded_reflection_stage` | Worker/broker acceptance, repeated corrections, rejection, corpus provenance | Migrate all assertions to script-driven real reflection and broker. |
| `test_replay` | Checkpoint bounds/integrity, generated responder/reflection behavior | Retain storage tests and run generated scripts through real workers. |
| `test_script_replay_cli` | Reject removed public selectors | Verify both agent variants and live fallback are unavailable. |
| `test_recording_registration`, `test_recording_export`, `test_file_edits` | Live capture and evidence fidelity | Retain; no playback registrations/executor. |
| `test_generated_worker*`, `test_reflection_script`, `test_scripted_networkpolicy`, `test_agent_script`, hermetic tests | Synthetic scripts, real workers, transport and validation | Retain the common stand-in path. |
| Worker Dockerfile and image/CI-scope tests | Package live recorder in benchmark image | Retain capture-only packaging. Production code has no benchmark imports. |

Missing or unsupported recordings fail explicitly. Broker verdict equivalence is
separate from typed-result equality: session identifiers, usage and clocks can
change when commands execute again. Archived run inputs must remain read-only.

Archived checkpoints retain their declared contract. For example, a retired
`ReflectionRequest.cause_work` field fails current strict decoding before agent
execution; script conversion does not silently rewrite task or broker evidence.
