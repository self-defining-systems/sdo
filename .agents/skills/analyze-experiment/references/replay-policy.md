# Replay policy in run evidence

Read the row's policy before comparing learned memory. Fastloop `IncidentRecord.replay`
is `on` or `off`; null means an older row or a participant without this setting.
Offline skilleval rows and `run-config.json` record `replay_mode` and `replay`,
respectively. Default remains on. Never infer off from a missing corpus.

With off, capture/replay and every corpus requirement are disabled. A successful memory
check still compiles and vets source, enforces imports, ownership and formatting, and authored-file hygiene. Missing labels,
snapshots and replay observations are expected. A skipped replay is unmeasured, not
Match/NoMatch, and does not count as a replay pass. Program stages and fresh live
observations are unchanged; offline save success alone cannot establish live reliability.

Historical script receipt links can explain an observed transition in an offline replay. A live Match is the planning gate for RunScript, and absent script evidence leaves replay unmeasured.

For a paired offline comparison use identical frozen SKILLEVAL2 scenarios, skills,
source overlay, binary, provider/model/effort and two runs per arm. Report model calls,
raw and weighted tokens, phase USD, native reflection minutes, memory-check passes,
changed programs per hand repair, failures and blocked saves. Retain every run,
including failures. Source overlays must identify the frozen code and executable;
never run an off-policy worker with an old on-only validator by accident.

Live confirmation is repeat-only on already accepted memory with an independently
verified restore and the same fault. The orchestrator schedules it after the stream
build includes this feature. Compare independently verified health and cost, not the
presence of replay notes.
