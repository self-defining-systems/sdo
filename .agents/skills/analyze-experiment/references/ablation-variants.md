# Ablation memory variants

`python -m benchmarks.sregym.ablation` prepares three arms from a pinned accepted
memory branch, with replay off: full, detectors-only, playbooks-only.
The JSON report records input and output SHAs, counts; retained artifacts are unchanged.
Counts come from `sdo-operator memory ls --json`; `program` counts Go playbook
packages once each, alongside `playbook`, which counts README documents.
Missing detector or playbook classes are valid memory. Existing artifacts still
pass normal validation. Neither a zero-detector tree nor variant generation
alone proves an incident can trigger, close or run without learning.
