# Accepted-memory ablation variants

Generate one of `full`, `detectors-only`, or `playbooks-only` in an isolated
checkout outside this repository. The command audits the whole accepted tree
with replay off before pushing. Existing remote branches are never force pushed.

```bash
SDO_OPERATOR_ROOT="$PWD/sdo/operator" uv run python -m benchmarks.sregym.ablation \
  --repository git@github.com:vibesys-playground/hotel-reservation-sdo.git \
  --source-branch port/20261102-streamrun-pass5-v3 \
  --arm detectors-only \
  --directory /tmp/ablate-hotel-detectors \
  --report /tmp/ablate-hotel-detectors.json --push
```

`full` preserves every byte. `detectors-only` removes playbooks (including
scripts and programs), shared helpers and corpus, and preserves detector code, tests and manifests unchanged.
Detector playbook hints are advisory when the playbook catalog is absent. `playbooks-only` removes every health and incident detector;
playbook Markdown, program code, helpers and corpus are preserved byte for byte.
Reports record source and output commits, artifact counts, metadata edits and
whether the verified branch was pushed. No Markdown is rewritten.

These commands prepare memory; incident triggering and frozen-run behavior are
separate fastloop concerns.
