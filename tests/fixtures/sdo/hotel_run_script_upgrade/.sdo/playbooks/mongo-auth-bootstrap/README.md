---
schema_version: 1
owner: responder
fault_class: mongo-auth-bootstrap
originating_incident: mongo-auth-upgrade
---

Diagnose the bound database `<POD>` through its current mounted ConfigMap relation and
recorded catalog. A missing or ambiguous relation remains Unknown. Run the related
initialization routine with relation-derived Values, then diagnose again.

This synthetic hotel-shaped application uses database `geo-db`, user `admin`,
authentication database `admin`, and init assignments `ROOT_USER` / `ROOT_PWD`.
Those app facts are read from the current related ConfigMap's structured annotations;
the shared configuration helper is their single logical access point. Names and
mount paths come from the current Pod and its volume relations, with no Go defaults.
The inline script is linked to the original successful manual exec receipt.
Replay basis: observed-transition; no opaque database effect is simulated.
