## detect

run fresh detectors; pending health is unproven

```text
sdo detect [-h] [--detector ID [ID ...]]
```

run fresh detectors; pending health is unproven. External incidents have no detector gate; return your completed result after live checks.

- `--detector`: detector IDs to run; repeat for more IDs (default: all)

Key JSON fields: `healthy, findings[], detector_states[]`.
