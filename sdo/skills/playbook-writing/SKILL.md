---
name: playbook-writing
description: Turn the fix you just made into a playbook SDO can run by itself next time.
---

# Writing a playbook

A playbook is `.sdo/playbooks/<id>/` with `diagnose.go` and `mitigate.go`. Copy the closer of
`examples/template-drift/` or `examples/stuck-pods/`. Write only Go.

- `Spec()`: the `ID` and no object role. SDO tries it on every pass, so `Diagnose` finds its own subject through `ReadView` (app map,
  owners, `DiffAgainstSource`) and returns `program.NoMatch` when it is absent.
- `Diagnose`: test the state your fix changed (a row, a catalog entry, a setting, a key, an object field), read live, not a symptom. A log line is the symptom's echo: it can be rotated away, land on another pod or container, fall outside the window or never be written on a repeat, so never make it a condition the match requires; at most attach it as `Evidence` after the state matched. Write each state check so it selects the fault's own state exactly as you saw it (the same column type, constraint text or key you read), and say in your reply when you could not run it against the fault. Filter on object
  state first; `v.Logs` costs a read, so call it only for the few objects that pass, as
  `Evidence`. Return `program.Match`, `program.NoMatch`, or `program.Unknown` (failed read, unbaselined store).
  Stay `NoMatch` on a healthy install and other faults. To find a log line use `v.LogMatches` (grep over the whole window); `v.Logs` keeps only the newest 64 KiB. Unscanned (`ScanTruncated`) is `Unknown`, not `NoMatch`.
- `Mitigate`: the changes that fixed it, in order, as you made them: `program.DeletePod`,
  `program.RestartRollout`, `program.SetField`; for an `sdo mitigate X`, the same
  `program.X` with the same fields.
- The SDK can run commands, not only restart and patch: a fix with no typed action (SQL, a CLI) is `program.EnsureExec` (check, then repair, in a Pod; credentials from the container's environment). All actions: `/opt/sdo/sdo/skills/sdo-playbook-authoring/references/sdk-card.md`. A plan that is only `program.Escalate` repairs nothing.
- Prefer a mitigation that sets the known-good value directly over one that depends on controller history, such as a promoted baseline: a repeat may start without it.
- Carry a source restore you ran as `program.SetField` with `program.FromSource`. Omit changes that did not help.
- Memory is for this application and may name its own tables, resources and commands; generalize over what varies, not away from the application. A partial program beats none. No `RunScript`, no passwords.

Check:
1. `sdo-operator memory check`
2. `sdo-operator memory test <id>`

Then reply with one line: what it does, or why nothing here is worth one.
