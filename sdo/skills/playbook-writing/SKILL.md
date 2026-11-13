---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

A playbook is `.sdo/playbooks/<id>/` with `diagnose.go` and `mitigate.go`. Copy the closer of
`examples/template-drift/` and `examples/stuck-pods/` and edit it. Write only Go.

- `Spec()`: the `ID` and no object role. SDO tries a role-less playbook on every pass, so `Diagnose` finds its own subject through `ReadView` by relation (app map,
  owners, `DiffAgainstSource`), never by name, and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed (the cause), not a symptom. Filter on object
  state first; `v.Logs` costs a read, so call it only for the few objects that pass, as
  `Evidence`. Return `program.Match`, `program.NoMatch`, or `program.Unknown` if a read fails.
  Stay `NoMatch` on a healthy install and other faults, within seconds.
  Follow investigators' "How to diagnose" notes.
- `Mitigate`: the changes that fixed it, in order, as you made them: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- Restoring accepted source is not automatic: carry a restore you ran as `program.SetField`
  with `program.FromSource`. Leave out changes that did not help.
- No `RunScript`, no app names, no passwords.

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Then reply with one line: what it does, or why nothing here is worth one.
