## plan

plan a bound program repair

```text
sdo plan [-h] [--finding FINGERPRINT] [--bind NAME=Kind/namespace/name] program_id
```

SetField updates a schema-checked field by index or unique name, optionally from accepted source; ConfigMap JSON/YAML entries preserve unrelated keys.
Object edits write atomically per object; use strategic patches for keyed lists.
CreateFromSource restores a named missing object, including declared file-generated ConfigMaps, from accepted source. Source/config restores may recycle blocked dependent Pods once; Pod deletion receipts are irreversible.
ReconcileFromSource converges source-owned configuration, removes additions since acceptance, and reports residual source differences and remaining old-revision ReplicaSets/Pods. The Deployment controller and responder handle the rollout.
DeleteObject irreversibly removes an allowed named source-absent object and never auto-recreates it.
Program commands use the current checkout, including shared libraries and new playbook IDs. Edits compile on demand; fix compile errors in place and retry. Revision receipts use working-tree:<source digest> for unpublished code. Reusable API reads use program.Kubernetes(view) with upstream client-go options; reads are recorded for replay.

- `--finding`: target finding fingerprint; repeat for each intended finding
- `--bind`: named program parameter; repeat for each binding

Key JSON fields: `plan_id, patches[], program_runs[], program_revisions`.
