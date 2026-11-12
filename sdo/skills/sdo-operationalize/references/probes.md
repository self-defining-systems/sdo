# Strict probes

A probe catches only what its checks can tell apart from the healthy answer. A wrong, empty, stale or partial
answer usually still comes with a success status, so every probe checks content, and the most exact content it
can know. The memory check rejects a probe scenario whose steps check nothing but statuses (`map.schema`).

## Check exact content

Use the strictest check the step's data allows (`sdo-detector-authoring` traffic reference for signatures):

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
