---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

A playbook is `.sdo/playbooks/<id>/` with `diagnose.go` and `mitigate.go`. Copy the closer of
`examples/template-drift/` or `examples/stuck-pods/`. Write only Go.

- `Spec()`: the `ID` and no object role. SDO tries it on every pass, so `Diagnose` finds its own subject through `ReadView` (app map,
  owners, `DiffAgainstSource`) and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed, not a symptom. Filter on object
  state first; `v.Logs` costs a read, so call it only for the few objects that pass, as
  `Evidence`. Return `program.Match`, `program.NoMatch`, or `program.Unknown` (failed read, unbaselined store).
  Stay `NoMatch` on a healthy install and other faults.
- `Mitigate`: the changes that fixed it, in order, as you made them: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- Carry a source restore you ran as `program.SetField` with `program.FromSource`. Omit changes that did not help.
- Memory is for this application and may name its own tables, resources and commands; generalize over what varies, not away from the application. A partial program beats none. No `RunScript`, no passwords.

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Then reply with one line: what it does, or why nothing here is worth one.
