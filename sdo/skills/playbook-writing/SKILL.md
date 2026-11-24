---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

Copy the closer of `examples/template-drift/` or `examples/stuck-pods/` to
`.sdo/playbooks/<id>/` as `diagnose.go` and `mitigate.go`.

- `Spec()`: the `ID` and no object role. SDO tries it on every pass, so `Diagnose` finds its own subject through `ReadView` (app map,
  owners, `DiffAgainstSource`) and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed (a row, setting, key or object field), read live, not a symptom. A log line is the symptom's echo (it can rotate away): never require it; at most `Evidence` once matched. Select the fault's own state exactly as you saw it (column type, constraint text, key).
  Return `program.Match`, `program.NoMatch`, or `program.Unknown` (failed read, unbaselined store, unscanned `ScanTruncated`). It must Match its own incident before you save it; stay `NoMatch` on a healthy install or another fault. A gone object is not a failed read: it is missing from its list (`v.ConfigMaps()`), and `DiffAgainstSource` of it errors.
- `Mitigate`: the changes that fixed it, in order: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- The SDK can run commands, not only restart and patch: a fix with no typed action (SQL, a CLI) is `program.EnsureExec` (check, then repair, in a Pod; credentials from the container's environment). All actions: `/opt/sdo/sdo/skills/sdo-playbook-authoring/references/sdk-card.md`. `program.Escalate` alone repairs nothing.
- Use the fastest supported operation with equivalent effects, including cache invalidation.
- Ensure serving processes use the new state. Replace them if needed.
- Fix plan advisories from the recorded check.
- Set the known-good value directly, not from controller history a repeat may lack.
- Carry a whole source restore as `program.ReconcileFromSource`. Omit changes that did not help.
- Memory is for this application and may name its own tables, resources and commands; generalize over what varies, not away from the application. A program for just the mechanical part beats none; leave judgement to the README. No `RunScript`, no passwords.

If the cause has a signal of its own, add a detector that names the playbook: [detector.md](detector.md).

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Run each in the foreground; never `sleep` or background it.

Then reply with one line: what it does, or why nothing here is worth one.
