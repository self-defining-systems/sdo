## check

run detectors and verify closure; --detector ID selects detectors

```text
sdo check [-h] [--detector ID [ID ...]]
```

run detectors and verify closure; --detector ID selects detectors. External incidents have no detector gate; return your completed result after live checks.

- `--detector`: detector IDs to run; repeat for more IDs (default: all)

Key JSON fields: `healthy, findings[], detector_states[], detection (fresh detect response)`.
