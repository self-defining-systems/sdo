## detect

run fresh detectors; pending with a reason means health is unproven

```text
sdo detect [-h] [--detector ID [ID ...]]
```

- `--detector`: detector IDs to run; repeat for more IDs (default: all)

Key JSON fields: `healthy, findings[], detector_states[]`.
