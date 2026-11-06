# Applying fixes

`sdo diagnose NAME --bind ROLE=Kind/NAMESPACE/NAME` and `sdo mitigate NAME --bind ...` take the same name and
flags. A match prints `To fix: sdo mitigate ...`; run that line. `--bind` also carries plain values
(`--bind replicas=3`); structured values are JSON text. A wrong or missing role is an error that names what the
playbook takes. Built-in actions use the same form: `sdo mitigate DeletePod --bind ref=Pod/app/api-0`.
`sdo menu` lists fixes as commands; `sdo menu NAME` narrows to one.

Without `--bind`, `sdo mitigate NAME` uses the match SDO already recorded for that playbook. If several
bindings match, the error lists each as a command.

## Several fixes together

Separate independent fixes with `--` in one call: `sdo mitigate A --bind x=Kind/ns/a -- B --bind y=Kind/ns/b`.
Writes that overlap are refused before anything is written. `--finding FINGERPRINT` scopes verification to one
finding (default: the findings the playbook explains). `--rounds N` allows a rollout follow-up within the
remaining allowance; it belongs to a single fix.

## Results

- `fix applied; waiting for traffic to recover`: the writes landed and the playbook no longer matches; run
  `sdo verify --wait 60`. Independent verification decides recovery.
- Playbook still matching after its writes: failed as `diagnosis_persists`; its writes are released to your repair.
- A pending rollout keeps its application ID and allows reads; `rollout_stalled` releases writes once stuck.
- Source reconciliation waits for old objects to drain; configuration differences return `source_residual`.
  Old rollout objects and unavailable readback stay pending and unverified.
- Script fixes are learnable; unlinked bytes are refused as `script_unprovenanced`.
- Learned preconditions are rechecked against live state before any write.

## Playbook programs

Programs use the current checkout, including shared libraries and new playbook IDs. Edits compile on demand;
fix compile errors in place and retry. Revision receipts use `working-tree:<source digest>` for unpublished code.

## Built-in actions

`SetField` updates a schema-checked field by index or unique name, optionally from accepted source; ConfigMap
JSON/YAML entries preserve unrelated keys. Object edits write atomically per object; use strategic patches for
keyed lists. `CreateFromSource` restores a named missing object, including declared file-generated ConfigMaps, from
accepted source; source/config restores may recycle blocked dependent Pods once, and Pod deletion receipts are
irreversible. `ReconcileFromSource` converges source-owned configuration, removes additions since acceptance, and
reports residual source differences and remaining old-revision ReplicaSets/Pods. `DeleteObject` irreversibly
removes an allowed named source-absent object and never auto-recreates it. Webhook repairs requeue affected
admission-blocked ReplicaSets automatically; after raw repairs use `RetryAdmission` in the incident namespace and
verify recovery. `RetireOldRevision` scales an old-revision ReplicaSet to zero; stalled rollouts name it as their
release.

### Parameters

Parameters accept JSON objects, arrays, numbers, booleans and strings. `sdo menu` lists each action's
`parameters[].schema` and example. Learned CA plans resolve missing references only from one observed serving
relation. `SyncWebhookCA` privately copies a named Secret key into one webhook `caBundle`; only its reference and
digest are returned. `EnsureDatabaseUser` repairs a MongoDB user in one call; its password and `target.auth`
username/password take exactly one credential reference: `{env: NAME}` or
`{config_map: {name: CONFIGMAP, key: KEY, assignment: optional_NAME}}` in the target Pod namespace. The executor
resolves values privately; never put credential values in arguments, evidence or memory. The result reads the
catalog back (`action_results[].database_user`: created, updated or already_correct). Failures use
`execution_failed`, `authorization_denied` or `outcome_unknown`. A verified user does not mean the application
recovered.
