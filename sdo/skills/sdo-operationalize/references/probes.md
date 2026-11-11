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
- When the application has no self-service sign-up (accounts created by an operator, an API that takes only
  tokens), name a Secret key the application already keeps in the scenario's `Credentials` (`{Secret, Key, As}`)
  and send `{As}` in a header or form, or with `WithBasicAuth`. Only the prober reads the value, the way a Pod
  takes a Secret by env; memory holds the reference. Check the key exists without reading values (`kubectl describe
  secret` lists key names and sizes), and calibrate the step from a Pod that takes the key by env, never printing
  it.
- Create the test identity in Setup through the application's own sign-up operation (an ensure step), and log
  in through its own login operation: save the session with `SaveCookie` or `SaveJSON` and send it with
  `WithHeader` (`{key}` placeholders) or `FromPrevious`.
- A read scenario logs in in its Setup (setup may write). When the application's sessions expire sooner than
  a prober runs, log in as the first step of a write-read scenario instead, and say in `Calibration` which
  read-later data no probe can read without a session.
