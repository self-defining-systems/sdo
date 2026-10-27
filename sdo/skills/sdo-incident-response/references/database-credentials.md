# Database credential references

Use the typed database action to restore a missing user or its direct roles. Keep
credential values out of arguments, evidence and memory; select their declared
source by reference. The executor resolves values privately at execution time.

`CredentialRef` selects exactly one source:

- `Env`: a variable in the database container, including a Secret-backed variable.
- `ConfigMap`: a named key in the target Pod namespace. An optional `Assignment`
  selects exactly one literal `NAME=value` line in a shell script or dotenv file.
  Quoted and bare values are supported; expansion, commands, missing assignments
  and duplicates are refused with a typed error naming the reference.

JSON uses `env` or `config_map: {name, key, assignment}`. The same references work
for database authentication and the repaired user's password. ConfigMap data need
not be exposed as container env. Secret data is never read into plans, logs, prompts or
memory; direct Secret keys require a separate security review.

A successful catalog write is action evidence. Check application recovery through
SDO's independent verification; include a dependent Deployment restart when the
repair requires reconnection. Learn executable diagnosis and mitigation from the
recorded open catalog and measured fixed-phase readback, then replay to NoMatch. Replay preserves references and
projects catalog membership and roles; it does not prove live authentication.

Catalog authentication is separate from the password for creating a user.
Set `target.auth` to the source-declared authentication database and username/password
references when the server requires authentication; `password` alone does not
authenticate the catalog read. A nil `target.auth` uses local unauthenticated
access only. Failures identify the safe stage (container lookup, credential
reference, exec transport, client binary, authentication, catalog read or parse).
Raw server messages and credential values are excluded. A catalog-read rejection
with no authentication configured may require authentication references.

The model-free setup conformance report includes reference-only authentication
inputs discovered from declared root env variables or an unambiguous literal
bootstrap pair in a ConfigMap mounted by the database container. Its target can
be reused as source evidence. Discovery never reads Secret objects or evaluates
shell. Explicit target auth overrides discovery; ambiguous or unavailable source
references fail setup. Nil auth on a typed action still means unauthenticated
local access; conformance does not change that action contract.
