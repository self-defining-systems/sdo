## menu

list fixes as commands

```text
sdo menu [-h] [--bind NAME=VALUE] [NAME]
```

List the fixes you can apply as commands; NAME narrows to one playbook.

- `--bind`: a playbook parameter: an object is NAME=Kind/NAMESPACE/NAME; repeat for each

Key JSON fields: `menu.options[].{command, program_id, program_revision, disabled_reason, parameters}, unexplained_removal_blockers[]`.
