# Data checks

A data check reads a small sample of one store's records and asserts what the code that reads them needs.
Probes see only the records they wrote; a data check sees records nobody's probe touches: a renamed or retyped
field, a poisoned record, a reference to nothing, a cache that disagrees with its store.

Write one file per check at `.sdo/diagnostics/data/<check-id>.yaml` (lowercase, digits, dashes). Go installs a
`data-<check-id>` health detector for it when you run the memory check: it fails like any health detector, and
an incident closes only when it clears. Do not write detector code for it.

```yaml
component: post-store              # map component with a data entry; engine must match it
engine: mongodb                    # mongodb | postgres | redis | memcached
target: {labels: {app: post-store}}  # selects the store's Pods (labels, and image to pick the container)
read:                              # what to sample; exactly one of ids, range, first (at most 32 records)
  database: posts
  collection: post
  key: post_id                     # the field that identifies a record
  key_type: number                 # how ids are stored: string (default) or number
  first: 16                        # the first 16 records in key order
min_records: 1                     # fewer is a finding (an emptied or renamed collection); caches set 0
assert:
  - shape: {post_id: integer, creator.user_id: integer, text: string, "media[]": object}
  - bounds: {field: likes, min: 0}
```

Reads by engine (all read-only; Go builds the command, bounds it and injects the limit):

| engine | read fields | record |
|---|---|---|
| mongodb | `database`, `collection`, `key`, `key_type`, `filter` (comparison and logic operators only) | the document's fields you assert on |
| postgres | `table` (`[schema.]name`), `key` (column), `database`, `where` (columns, literals, `AND/OR/NOT`, `IS NULL`, `IN`, `LIKE`, `BETWEEN`; no functions) | the columns you assert on |
| redis | `pattern` (one `{id}`, e.g. `"{id}:followers"`), `type` (string, hash, set, zset, list), `decode` for strings, `key_type: number` to order ids numerically | hash: its fields; set, zset, list: `members` (at most 64; a zset highest score first) and `size`; string: `value` or the decoded JSON |
| memcached | `pattern`, `decode`; `first` lists keys with `lru_crawler metadump` (in key order) | `value` or the decoded JSON |

`decode` reads a string value as `text` (the field `value`), `json` (one document) or `ndjson` (one JSON document per
line, as the list `lines`: assert `"lines[].field"`, or `bounds` with `count` on `lines`).

Paths: `a.b` walks objects, `list[]` takes each element. Write a key with `[]` as a YAML string (`"members[]": digits`).
Shape types: string, number, integer, digits (an integer or a string of digits), boolean, object, array, null,
any. A missing field, a wrong type, or a cached value that is not JSON fails the record.

Assertions over a second store add `lookup:` (component, engine, target, read without a selection). Each check
holds at most one of them:

- `reference: {field: "members[]"}`: every value must be the key of a lookup record (a timeline entry that names
  a post, an order line that names a product). The read passes at most 32 values per cycle.
- `equivalence: [{field: "members[]", source: "followers[].user_id"}]`: each sampled record must equal the
  lookup record with the same id on the listed fields (sets compare as sets). Only for a component the map marks
  `role: derived` with the lookup in its `derived_from`.

When to write one (keep it to a few, each tied to code that reads the records):

- a field or type the read path decodes and fails without: shape on the authoritative store;
- a key a component resolves in another store: reference;
- a derived store (a cache, a fan-out copy): equivalence with its source, `min_records: 0`;
- counters with a known range: bounds.

Choose `ids` or `first` so the sample includes records the deployment seeds and real users read. Confirm each
check passes on the healthy application (run its read by hand through the store's CLI) before you finish; a
check that fires on healthy data is your fault to correct. Output is capped at 4 KiB per read: sample fewer
records or assert on fewer fields if the read is cut.
