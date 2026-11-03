## check

run detectors and verify closure; external incidents await completion; --detector ID selects detectors

```text
sdo check [-h] [--detector ID [ID ...]]
```

- `--detector`: detector IDs to run; repeat for more IDs (default: all)

Key JSON fields: `healthy, findings[], detector_states[], detection (fresh detect response)`.
