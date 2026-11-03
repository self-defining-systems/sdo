## status

read findings and detector confirmation

```text
sdo status [-h]
```

Read fresh findings and detector confirmation without running programs or planning repairs. Use diagnose for program verdicts and plan or menu to prepare repairs. prior_stages lists what automatic stages already did.

Key JSON fields: `healthy, findings[], detector_states[], drift[], drift_events[], program_runs[]`.
