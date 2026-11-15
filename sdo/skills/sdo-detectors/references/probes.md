# Strict probes

A probe catches only what its checks can tell apart from the healthy answer. A wrong, empty, stale or partial
answer usually still comes with a success status, so every probe checks content, and the most exact content it
can know. The memory check rejects a probe scenario whose steps check nothing but statuses (`map.schema`).

## Check exact content

Use the strictest check the step's data allows ([traffic reference](traffic.md) for signatures):

| The step reads | Check |
|---|---|
| a value the probe wrote | `JSONEquals(path, "{key}")`: exactly the value, never `Contains` |
| a collection of the probe's own data (its identity's items, relations, members) | `ExactSet(path, field, ...)`: every expected member, none missing, none extra, no duplicates |
| a collection of data the probe does not own | `Each(path, field, pattern)`: non-empty, and every member well-formed (an id is an id, a time is a time) |
| a member of a returned collection | save one returned id (`SaveJSON`) and read it in a next step, so it resolves |
| an HTML page or plain text | `Contains` of a text the page always shows when healthy |

- Derive each expected value from the source and confirm it live during calibration. A check that passes on
  an empty, null or error body is not a check.
- A probe on one identity sees only that identity's data. Prefer reads whose healthy answer covers many
  records (a listing, a search, a count) with `Each`, over a single fixed record.

## Write-read: verify the change just made

Every iteration must make a change and read back exactly that change. Re-applying the same write (the same
value, the same relation) every iteration proves nothing: it re-creates the very state it then reads, so lost
or corrupted state is healed by the probe before it is read.

- Write a fresh marked value each iteration (from the rng, with the `Marker`) and read that exact value back
  (`JSONEquals`).
- For a relation or membership (add and remove a member, grant and revoke, join and leave), alternate the two
  operations on the probe's identity and read the exact set after each (`ExactSet`). Pick add or remove from
  the iteration's rng: after an add the set holds the member, after a remove it does not, whatever came before.
- When a write fans out to other records (a write by one identity that other identities read), create the
  reading identities and their relations once in Setup, and read the write at every one of them.

## Read-later: data written once

- Setup writes the fixture once, on an identity that no other scenario writes; steps only read it.
- No step, cleanup or other scenario writes, restores or re-creates it. A verify burst reuses the continuous
  probe's setup, so a lost fixture stays failing until the data comes back; only a controller restart runs
  setup again.
- Check the fixture exactly (`JSONEquals`, `ExactSet`), so a changed or partial fixture fails too.

## Users' own records

Fixtures a probe wrote itself prove the mechanism works; they cannot see damage to the records users created or
the deployment seeds (a status flipped on one stored object, a record left in a state its list or download
filters out, a lock that never released). Two silent outages of that kind passed every probe because no scenario
touched the affected records.

- Mark the step that does this `ReadsExistingRecords(path, field, pattern)` (`path` is the JSON array, `field` the
  member value, `pattern` its RE2 shape). The check requires one in a gated probe when the map holds data
  (`map.records-unread`). An empty list passes, so a fresh install with no user data stays healthy; the engine
  only notes whether the step ever saw a record the probe did not create. The step must not template a key a write
  step of its scenario saved: list live and save the id from the list, then open it. Only an app whose users keep
  no records sets `no_user_records: <why>` at the top level of the map.
- Onboarding acceptance also needs one marked step to have returned a record the probe did not write. A probe
  identity that owns only its own records (a listing of the projects or containers it created) never does, so
  grant it read access to existing users' containers (the probe-identity procedure) and list those.
- Every list, open and download the health objective names has a call or read-later scenario that reads
  existing records the probe never wrote, on every iteration, over every such record (or the first
  `traffic.MaxSteps` of them): save their ids in a Setup step or list them live, and open each one a user opens.
- Check the answer a user relies on: a listing is non-empty and well-formed (`Each`), an opened or downloaded
  record answers with its healthy status and content. A record that vanishes from a list or stops opening fails.
  Pick records that are non-empty when calibrating; a list a healthy application leaves empty is not a check.
- A record that later disappears must fail, not shrink the expected set: require each listing to be non-empty
  every iteration, or save what existed in Setup (`SaveJSON` ids, names, files) and read exactly it again.
  Either way the scenario fails until data exists to read.
- A probe that only creates fresh records (a new name, a new version) never reuses a record users keep writing
  into: read, and where it is safe append to, the existing container.
- Probe the writes users make in the same containers, not only in the probe's own (a commit or push into a
  user's repository runs that repository's hooks and storage path; an upload goes into the package users use):
  a marked record, such as a branch of its own, created and removed in the same iteration (`Cleanup`), never
  an edit of a user's record. Cover every write the health objective names; a read cannot see a hung hook.
- When the identity cannot reach these records (private to their owner), the probe-identity procedure grants
  the probe user the lowest role that can do the above, never admin; a failed grant must not stop the user from
  being provisioned.

## What traffic cannot show

- Wrong content (exact checks), operations users rarely call, and each
  protocol and direction an entry serves (upload and download, non-HTTP).
- Background work: a worker, scheduler or consumer (often a workload with no Service) exists because some user
  operation hands work to it. Find that hand-off in source and config, start the work through the user
  operation and poll for its effect (a status moves on, a result becomes readable) within a bound. A
  worker is never `unprobed` because its effect arrives after the request returns.
- Existing records: besides listing them, open them and make the writes users make into them.

## Cost

Without a delete or overwrite route, keep every write on the fixed test identity, declare
`SideEffectIdempotentWrite`, use the lowest rate that fills the window, and record the growth under
`Probe costs` in the health-objective playbook.

## Identities and sessions

- Never forge credentials, tokens, cookies or sessions, and never read secrets from the application's config,
  manifests or Secrets to sign one.
- When the application has no self-service sign-up that yields a usable account (accounts created by an
  operator, sign-up pending admin approval), declare the map's [probe identity](map.md#probe-identity) procedure
  and list the users the scenario signs in as in `Users: []traffic.ProbeUser{{Name, As}}` (each name contains the
  scenario's `Marker`; several users, such as a writer and a second member, are several entries). SDO provisions
  each user with a password it generates and puts it in State under `As`; a Setup step signs in through the
  application's normal login with `{As}` (form, JSON or `WithBasicAuth`) and saves the session or a token it
  creates. Until the user is provisioned the scenario's samples are invalid, not failures. Never read an
  application Secret for a probe.
- A user operation the application serves over SSH (for example a version-control push or fetch) is probed with
  an ssh step: give the step its own `Target: &traffic.Target{Service, Port, Scheme: "ssh", User}` (the Service
  is in `DependsOn`) and write an `Endpoint` whose `Build` returns `traffic.Request{Method: traffic.MethodExec,
  Path: <command line>, Body: <stdin>}` and whose `Check` judges `Status` (the exit status) and `Body` (stdout).
  The probe user that signs it in sets `SSHKey: "<state key>"`; register `{<state key>}` (its public key line)
  through the application's own API in a Setup step with `OrExisting` (the key is the same after every restart).
  An EXEC is a write: it carries the `Marker` and runs only in Setup, Cleanup or a write scenario's Steps.
  Failure reasons hide 40-character hex runs, so name object IDs by a short prefix in `Fail`.
- Create the test identity in Setup through the application's own sign-up operation (an ensure step), and log
  in through its own login operation: save the session with `SaveCookie` or `SaveJSON` and send it with
  `WithHeader` (`{key}` placeholders) or `FromPrevious`.
- A read scenario logs in in its Setup (setup may write). When the application's sessions expire sooner than
  a prober runs, log in as the first step of a write-read scenario instead, and say in `Calibration` which
  read-later data no probe can read without a session.
