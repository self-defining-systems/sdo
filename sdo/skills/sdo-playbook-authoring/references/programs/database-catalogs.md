## Database catalogs

`ReadView.DatabaseUsers(DatabaseTarget)` observes nonsecret user names and directly granted roles.
Targets select `Engine: program.MongoDB`, a bound application Pod, container, database, and optional
`Shell` (`mongo` default or `mongosh`). Optional `DatabaseAuth` names the authentication database and
username/password references. `CredentialRef` selects exactly one of `Env: name` or
`ConfigMap: &program.ConfigMapCredentialRef{Name: name, Key: key, Assignment: optionalName}`.
ConfigMaps are read in the target Pod namespace. An omitted assignment selects the entire key;
an assignment selects exactly one literal `NAME=value` line (shell or dotenv, quoted or bare).
Missing/duplicate assignments, expansions and command substitution fail with a typed reference error.
Only the executor resolves ConfigMap bytes and sends them through exec stdin; programs, receipts,
recordings and corpus retain references only. Env refs resolve inside the container, including
Secret-backed env. Programs never read Secret data into plans, receipts, logs or memory;
do not work around that.

`EnsureDatabaseUser{Target, User, Roles, Password: reference, Reason}` creates a missing
user with a password resolved privately by the executor. For an existing user it restores exact direct roles
and preserves its password. Other users remain unchanged. Local unauthenticated bootstrap requires the
database's existing policy; the action creates no Kubernetes grants. It is irreversible and never
re-executed from a durable receipt. The controller reads the catalog back after the write and reports the user as created, updated or already_correct, or a typed failure reason. Diagnosis must inspect the typed catalog and propagate unavailable
reads as Unknown. Build replay from recorded open `DatabaseUsers` results; fixed-phase replies never
prove a repair. The simulator changes only the named user's direct roles/catalog membership and does
not simulate credentials, arbitrary Exec, authentication policy, connectivity, or server availability.
Include `RestartRollout` for a dependent bound Deployment when recovery also needs a reconnect.
Independent live health verification remains required.

General object edits, replayable commands/HTTP, and one-off scripts use the [general action vocabulary](../general-actions.md).


Keep RunScript bash inline in Mitigate;
application inputs are relation-derived `program.Value` entries in Env, resolved
at apply time and injected as process environment. Typed actions remain optional.
Scripts default to 60 seconds, bounded by five minutes and the incident budget;
script and each output stream retain at most 64 KiB.

For opaque scripts, label `script_origins` in the corpus case with the script
SHA-256 digest mapped to its original successful repair receipt ID. The
controller adds timed `RepairReceipt` observations to protected recordings before
reflection. Only a controller-recorded RunScript receipt with the exact executed
script digest authorizes those bytes; legacy receipts and API writes cannot. Check the cause work and bound receipts
before drafting executables.
Replay records open and fixed diagnoses with their bindings; neither Match nor NoMatch is required to save. The replay report uses `observed-transition`: it does not execute bash,
claim exact external-effect simulation, or assert the rewritten script ran.
The live apply→Diagnose check verifies that version; global health owns closure.
Preimages and API receipts are retained, with explicit gaps for opaque effects;
undo remains an explicit responder decision.

Raw document merge requests resolve a built-in schema from their API URL; any supplied reference must match. Unknown schemas and subresources return `RawMergeSchemaError`. Media-type parameters do not bypass keyed-list validation. Apply-style named Service ports merge by name, preserving unspecified fields when their port numbers change.
