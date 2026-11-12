---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

A playbook is `.sdo/playbooks/<id>/` with `diagnose.go` and `mitigate.go`. Copy the closer of
`examples/stale-process/` and `examples/stuck-pods/` and edit it. Write only Go.

- `Spec()`: the `ID` and one role, `component`: the workload you fixed. Reach its Pods and
  neighbors through `ReadView` (`v.Pods()`, `v.Deployments()`), never by name.
- `Diagnose`: the check that confirmed the cause: `v.Logs`, a Pod status, an object field.
  Return `program.Match` with `Evidence`, `program.NoMatch`, or `program.Unknown` if a read fails.
- `Mitigate`: the changes that fixed it, in order, as you made them: `program.DeletePod`,
  `program.RestartRollout`, `program.PatchObject`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- Leave out restores to accepted source (SDO does those) and changes that did not help.
- No `RunScript`, no app names, no passwords.

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Then reply with one line: what the playbook does, or why nothing here is worth one.
