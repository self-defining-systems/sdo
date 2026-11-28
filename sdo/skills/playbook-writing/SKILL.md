---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

Start from `examples/template-drift/` or `examples/stuck-pods/`. Write `diagnose.go` and `mitigate.go` in `.sdo/playbooks/<id>/`.

- `Spec()`: the `ID` and no object role. Every pass tries it, so `Diagnose` finds its subject through `ReadView` (app map, owners, normalized `DiffAgainstSource`) and returns `program.NoMatch` when absent.
- `Diagnose`: test the state your fix changed (row, setting, key or field), read live, not a symptom. Never require a log line; at most `Evidence` once matched. Select the fault's state exactly as seen (column type, constraint text, key). Return `program.Match`, `program.NoMatch` or `program.Unknown` (failed read, unbaselined store, unscanned `ScanTruncated`). Before saving, it must Match its incident; stay `NoMatch`, not an error, when healthy or on another fault. An absent object is not a failed read; check its list.
- Secret drift: test only declared keys via `DiffAgainstSource`. Deliberate rotation can match and be reverted.
- `Mitigate`: the changes that fixed it, in order (`program.DeletePod`, `program.RestartRollout`, `program.SetField`); for an `sdo mitigate X`, `program.X`, same fields.
- Signatures, `program.EnsureExec` (SQL/CLI): [sdk-card.md](../sdo-playbook-authoring/references/sdk-card.md).
- Use the fastest equivalent action. Verify serving processes use the new state.
- Scope absence claims to where you looked. Before recreating or overwriting lost data, require a live check for the authoritative copy; restore it if found, else leave the write to the responder.
- Repair app-owned state through the app's own tools. A playbook that writes app internals by hand says so in its README.
- Set the known-good value directly, not from controller history a repeat may lack.
- Carry a whole source restore as `program.ReconcileFromSource`. Omit changes that didn't help.
- Memory may name this application's commands and fields; generalize over what varies, not away from the application: bind components and stores by relation or parameter. Automate mechanical steps; leave judgement to the README. No passwords.
- Save working commands with waits and checks; recheck state first. A matching Diagnose may pick scripts or typed actions. Script output is dropped; signal by exit code.

A cause with its own signal gets a detector naming the playbook: [detector.md](detector.md).

Write all playbooks, then run `sdo-operator memory check` once in the foreground (fix all it finds), then `memory test <id>`.
At closing it also runs new or edited playbooks on the recovered cluster: a Diagnose Match fails; checks exit 0 holds, 1 needs change; another exit or a read failing twice fails.

Reply in one line: what it does, or why none.
