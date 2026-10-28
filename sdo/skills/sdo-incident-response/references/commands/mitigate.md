## mitigate

apply independent options in one call; overlaps refused; shows progress and full JSON

```text
sdo mitigate [-h] [--finding FINGERPRINT] [--rounds ROUNDS] [--selection FILE] [--selection-set FILE] [--option ID] [--param NAME=VALUE] [--match ID] [--bind NAME=REF] [option]
```

Apply several independent options in one call; overlapping object write sets are refused before writes. Use repeated --option ID followed by that option's --param NAME=VALUE, or --selection-set FILE with version 1 and options [{option_id, bindings, parameters, finding_fingerprints}]. --match ID applies a ready playbook_coverage match; --bind narrows it. Each option has its own receipt; one call uses one round. Pending rollout returns its application ID; reads stay open; past its bound it frees its writes as rollout_stalled. A single option also accepts --NAME VALUE parameters. Five rounds per incident. Script fixes are learnable mitigations; unlinked script bytes are refused as script_unprovenanced. Program commands use the current checkout, including shared libraries and new playbook IDs. Edits compile on demand; fix compile errors in place and retry. Revision receipts use working-tree:<source digest> for unpublished code. Reusable API reads use program.Kubernetes(view) with upstream client-go options; reads are recorded for replay.

- `option`: advertised option ID or unambiguous program ID
- `--finding`: target finding fingerprint; repeat for each intended finding
- `--rounds`: maximum rounds within the remaining incident allowance
- `--selection-set`: version 1 selection set with per-option bindings and parameters
- `--option`: independent option ID; repeat, with per-option --param arguments
- `--match`: playbook ID of a ready playbook_coverage match
- `--bind`: with --match: only the match binding NAME to REF (Kind/namespace/name); repeat

```text
Advertised option IDs remain valid for this session across filtered menu calls. Learned program preconditions are rechecked against live state before any batch writes.
```

```text
Parameters accept ordinary JSON objects, arrays, numbers and booleans, as well as strings. sdo menu exports recursive parameters[].schema for every option, plus selection_schema and selection_set_schema; nested properties, required fields and oneOf choices describe accepted shapes. parameters[].example supplies typed synthetic inputs; replace names/values using incident evidence. CLI --param NAME=VALUE accepts JSON text for structured values. SyncWebhookCA privately copies a named Secret key into one webhook caBundle; only its reference and digest are returned. EnsureDatabaseUser repairs a MongoDB user in one call. Its password and target.auth username/password take exactly one credential reference: {env: NAME} or {config_map: {name: CONFIGMAP, key: KEY, assignment: optional_NAME}} in the target Pod namespace. Assignment selects one literal NAME=value line. The executor resolves values privately; never put credential values in arguments, evidence or memory. Direct Secret keys are unavailable. The result reads the catalog back: action_results[].database_user states the verified user and outcome (created, updated or already_correct). Failures use execution_failed, authorization_denied or outcome_unknown; details retain the safe database diagnostic. A verified user does not mean the application recovered.
```

Complete report fields (`details_file`): `actions[].{application_id, targeted_healthy, mitigation_health, rollout_pending, program_revisions}, option_reports[]`.
