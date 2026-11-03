## detect

run fresh detectors; external incidents await responder completion

```text
sdo detect [-h] [--detector ID [ID ...]]
```

- `--detector`: detector IDs to run; repeat for more IDs (default: all)

Key JSON fields: `healthy, findings[], detector_states[]`.
