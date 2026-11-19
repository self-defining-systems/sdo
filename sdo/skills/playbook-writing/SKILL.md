---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

A playbook is `.sdo/playbooks/<id>/` with `diagnose.go` and `mitigate.go`. Copy the closer of
`examples/template-drift/` or `examples/stuck-pods/`. Write only Go.

- `Spec()`: the `ID` and no object role. SDO tries it on every pass, so `Diagnose` finds its own subject through `ReadView` (app map,
  owners, `DiffAgainstSource`) and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed (a row, a setting, a key, an object field), read live, not a symptom. A log line is the symptom's echo (it can rotate away or never repeat): never require it to match; at most attach it as `Evidence` once the state matched. Select the fault's own state exactly as you saw it (the same column type, constraint text or key), and say in your reply when you could not run the check against the fault. Filter on object state first; `v.Logs` costs a read.
  Return `program.Match`, `program.NoMatch`, or `program.Unknown` (failed read, unbaselined store, unscanned `ScanTruncated`). Stay `NoMatch` on a healthy install or another fault. A gone object is not a failed read: it is missing from its list (`v.ConfigMaps()`), and `DiffAgainstSource` of it errors. Find a log line with `v.LogMatches` (`v.Logs` keeps only 64 KiB).
- `Mitigate`: the changes that fixed it, in order, as you made them: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- The SDK can run commands, not only restart and patch: a fix with no typed action (SQL, a CLI) is `program.EnsureExec` (check, then repair, in a Pod; credentials from the container's environment). All actions: `/opt/sdo/sdo/skills/sdo-playbook-authoring/references/sdk-card.md`. A plan that is only `program.Escalate` repairs nothing.
- Prefer a mitigation that sets the known-good value directly over one that depends on controller history (a promoted baseline): a repeat may start without it.
- Carry a source restore you ran as `program.SetField` with `program.FromSource`. Omit changes that did not help.
- Memory is for this application and may name its own tables, resources and commands; generalize over what varies, not away from the application. A partial program beats none. No `RunScript`, no passwords.

If the cause has a signal of its own, add a detector that names the playbook: [detector.md](detector.md).

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Run each in the foreground; it can take a few minutes. Your Bash tool waits for it, so do not background it or `sleep` to wait.

Then reply with one line: what it does, or why nothing here is worth one.
