# Traffic coverage

Health traffic sees only what it calls. A link probe proves only that a fresh connection to a dependency opens, so a
dependency that accepts connections but fails requests stays invisible unless a continuously probed scenario sends a
request across that edge.

- Give each service-to-service edge in `links.yaml` at least one scenario of the `health` (health-probe) workload whose
  target or `DependsOn` names both ends and whose request really crosses the edge.
- Reads rarely reach services that only writes call. Include the application's own write journeys (an order, a
  booking, a post), modelled on its tests or clients, so a broken write-path dependency fails a scenario.
- Writes use dedicated synthetic data: set a `Marker` that appears in every write request, prefer an idempotent write
  (`SideEffectIdempotentWrite`, one whose repeats leave the same synthetic state) or a self-cleaning one
  (`SideEffectWriteWithCleanup` with `Cleanup` steps), and never touch real users' data.
- Send form bodies with `WithForm(params)` and JSON bodies with `WithJSON(template, params)`: each `{key}` in the
  template is filled from `params` or from an earlier step's `SaveJSON`, escaped for a JSON string; write `"{key}"`
  for a string and `{key}` for a number.
- Journey workloads run only on demand and never feed detection, so a write placed only there detects nothing.
- `sdo-operator memory check` lists each edge no health-probe scenario drives as a `traffic.undriven-edge` coverage
  gap. It never fails the check. Drive the edge, or write a comment above that link in `links.yaml` saying why no
  synthetic request can safely cross it.
