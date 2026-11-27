---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

Start from `examples/template-drift/` or `examples/stuck-pods/`. Write `diagnose.go` and `mitigate.go` in `.sdo/playbooks/<id>/`.

- `Spec()`: the `ID` and no object role. SDO tries it on every pass, so `Diagnose` finds its own subject through `ReadView` (app map,
  owners, normalized `DiffAgainstSource`) and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed (a row, setting, key or object field), read live, not a symptom. A log line is the symptom's echo (it can rotate away): never require it; at most `Evidence` once matched. Select the fault's own state exactly as you saw it (column type, constraint text, key).
  Return `program.Match`, `program.NoMatch`, or `program.Unknown` (failed read, unbaselined store, unscanned `ScanTruncated`). It must Match its own incident before you save it; stay `NoMatch` on a healthy install or another fault. An absent object is not a failed read. Check its list.
- Secret drift: test only declared keys via `DiffAgainstSource`. Deliberate rotation can match and be reverted.
- `Mitigate`: the changes that fixed it, in order: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- For SQL or CLI repairs, see `program.EnsureExec` in `sdk-card.md`.
- Use the fastest equivalent action. Verify serving processes use the new state.
- Scope absence claims to where you looked. Before recreating or overwriting lost data, require a live check for the authoritative copy and restore it if found. If the check is unavailable, leave the write to the responder.
- Set the known-good value directly, not from controller history a repeat may lack.
- Carry a whole source restore as `program.ReconcileFromSource`. Omit changes that did not help.
- Memory is for this application and may name its own tables, resources and commands; generalize over what varies, not away from the application. Automate mechanical steps; leave judgement to the README. No passwords.
- Save commands that worked with waits and checks, recheck state first, and find names through relations. A matching Diagnose can select scripts or typed actions.
- Script output is discarded; signal by exit code.

If the cause has a signal of its own, add a detector that names the playbook: [detector.md](detector.md).

Check against recorded evidence:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Run both in the foreground. At the closing turn, memory check also runs a new or edited playbook's read-only checks on the recovered cluster, bound as in this incident; a check must exit 0 when the state holds and 1 when it needs the change, and any other exit fails it. A check that could not run is only a note.

Reply in one line: what it does, or why none is warranted.
