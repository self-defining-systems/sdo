## Pod admission fails while existing Pods remain Ready

**Signature:** generic health detector `pod-admission`, rule `pod-admission-rejected`, primary resource the application Namespace. Evidence is the API error from a server-side dry-run Pod create. Existing readiness does not establish that admission works: temporarily permissive admission can restore workloads without repairing the admission path. Rejection still blocks closure after that policy is restored. No webhook or fault identity is required. Network/timeout failures and missing observations are detector errors, never clearance. Captured snapshots carry the frozen `pod_admission` result for replay; older snapshots cannot establish admission health. The probe checks one deterministically selected active app Pod spec (or a minimal Pod if none exists), not every possible selector or admission identity.

Detector history status `pending` means an unconfirmed observation window. It is valid in durable ControllerState, incident requests and reflection/outcome inputs. It proves neither firing nor clear; exclude it from healthy/repair evidence and wait for confirmed evaluations. Preserve pending-to-clear/firing/error transitions when compacting history. A missing checkpoint accompanied by a pending-status validation error is controller persistence failure, not health evidence.

The bounded `--evaluate-once` health command reports `status: unavailable` and an `unavailable` list of detector IDs/reasons when service reachability has no prober. This command-level verdict is not persisted detector history and cannot establish onboarding, seed health or repair authority. `pending` still means an incomplete observation window. `--source-drift-report` returns inventory-only `healthy`/findings before probing; its healthy value is no claim about detector health, and combining it with `--accept-seed` is rejected.

Steady synthetic traffic uses `pending` when a scenario has never succeeded since observation began or lacks enough recent usable SLO samples. `DetectorEvaluation.reason` (protobuf field 6; `Reason` in the Go tool-state JSON) names unproven scenarios and is informational, separate from `error`. Partial qualification blocks aggregate health clearance; qualified failures still fire. Pending alone never dispatches an incident and is not independent recovery evidence. Qualification remains process-local: after a mid-incident restart, fresh successful steady samples and ordinary clearing persistence restore closure eligibility. `sdo verify` reports the controller's verdict, not the burst aggregate alone. Preserve reason transitions during history compaction.


`fastloop eval-dry-run --run-dir <lane>` writes `eval-dry-run/report.json` with per-invariant pass/fail, elapsed seconds, and explicit `fault_injected:false` / `live_model_calls:false`. It executes the real pre-injection path using scripted registrations. A five-minute process deadline marks unfinished invariants failed; this evidence establishes eval plumbing, never learning. Admission evidence includes setup lineage, exact responder RBAC against the real installer plan, current controller detector states (clear since the resume), qualified native prober scenarios and stable workload identity; independent traffic is proven once, by the restore that precedes admission. Stabilization is recorded before the incident injection window and excluded from incident clocks. Preserve the report and the `admission/` evidence when a slot is rejected.

## Lane resource evidence

`admission/lane-resources.json` records namespace/Pod/container working-set
measurements and budgets (default 8GiB; observer and app all-in-one Jaeger
2GiB), host available memory, and `host_disk` evidence for the filesystem
backing local Docker kind-node storage. The disk object names `mount`,
`storage_path`, `total_bytes`, `free_bytes`, `used_percent`,
`floor_bytes`; default admission requires at least100GiB free, with used
percentage informational. Checks run before and after stabilization; the dry run
reports the same `lane_resources` invariant. Resource rejection is an eval
infrastructure failure, never evidence of agent or incident mitigation.
Only the benchmark observer is capped by SREGym: app growth is detected,
and application workload documents are preserved from accepted source.

New prepared-lane captures seal `fastloop_application.namespaces[].objects` with `installed_digest`. This installed set takes precedence over an inherited seed lock while source is unchanged; accepted source repairs restore from the current lock. Digest mismatches reject loading. Dry-run receipts include `code_revision` and per-stage resource, restoration, RBAC, setup and traffic results.

Dry-run timing separates one-time setup (`setup_seconds`, at most900s) from steady admission (`steady_seconds`, at most300s). Native onboarding plus the first complete detector observation ends setup; every existing health, traffic, lineage, placement and closure check remains mandatory during steady proof.

Hotel script isolation (`hotel-script-free/v1`): generic first-party fastloop hotel
apps use the harness's public `FailureScripts.ABSENT` profile. Before every worker
injection (including compositions), a read-only guard rejects the two
`failure-admin-{geo,rate}` ConfigMaps, known helper script keys in any ConfigMap,
and helper volume/ConfigMap references in workload templates or Pods. SDO lane
admission records `admission/hotel-script-profile.json` with profile, passed, and
objects_checked. A failed check invalidates admission; it grants no learning credit.

Warm capture migration retains the existing `fastloop_application` format and
records additive `hotel_helper_migrations` entries in the baseline document:
contract, source_path, source_sha256 (entire pre-migration file), namespace,
before_digest, after_digest, and removed identities. Original installed seals
must validate first. Only the two known helper ConfigMaps and the exact harness
Mongo Deployment mounts are removed; source, memory, grants, data and fault/oracle
implementations remain untouched. Unknown shapes/ownership fail closed. Compare
these migration receipts when adopting a warm lane; never recapture live fault
state to manufacture a clean seal. Old helper-assisted repairs are eval evidence,
not learned SDK repairs.

Host disk admission uses an absolute free-byte floor (default100GiB) on the Docker storage mount. The receipt names `rule: absolute-free-bytes`, the measured free bytes and floor; used percent is informational. A large volume with hundreds of GiB free must not fail solely on percentage. For historical comparisons retain the previous percent-and-byte rule and its rejection as eval evidence.

## Reused no-fault setup proof

A passing no-fault report now records `fingerprint` and `setup_proof`.
`setup-proof/latest.json` points to an immutable content-addressed receipt
containing the full passing report and `sdo.setup-proof/v1` fingerprint.
`eval-dry-run/report.json.setup_proof` and each live result's `setup-proof.json`
record `reused`, the original `receipt`, and named `changed` components.
A hit also records `message: "reused <receipt>"`. Missing, corrupt, dirty or
unavailable evidence requires full setup, never a silent skip.

The fingerprint binds the clean code revision, each configured role's image
digests on every node, app/control Pod image and process identities, accepted
workspace and installed accepted-memory commits, installed baseline/selected
manifest bytes, app/control object UIDs and configuration, cluster authority,
and proof source/operator hashes, including deployment configuration. Collection uses three GET inventories plus one read-only Git HEAD read
through the installed controller Pod, sharing a 15-second deadline; no Secret
values or Docker inspection are read. The persisted drift-inventory revision is
a content hash and is never interpreted as the accepted Git commit.
Runtime-state observations and maintenance generations are deliberately omitted
from configuration hashes. A recreated object or changed configuration after
reset invalidates reuse even when its name is unchanged.

Reuse skips the dry run's reset/deploy/onboarding and the live SDO runner's deploy
request. Admission still checks exact RBAC, clean baseline placement, fresh
controller detector clearance/no incident work, qualified probe scenarios,
stable workloads and lane memory/disk floors. The reused
dry run additionally requires active intake and the same fingerprint after
these checks; failure cannot fall back to a passing cached health verdict.
Inherited setup invariants name `reused_from`; fresh admission evidence is
written under the current `admission/` directory. The whole-learning-loop
report remains an independent gate and is never satisfied by a setup receipt.

## Install environment observations and restored drift

`NamespaceOnboarding.environment_objects` stores typed resource references and
`sha256:` digests of the captured UID, configuration and ownership, excluding
status and API bookkeeping. These create-once installer observations persist
across failed healthy onboarding and restart; they grant no healthy or learning
authority. Name-only legacy records cannot exempt a new incarnation. Changes to
UID, rules or owner references make the object visible to inventory again.

A complete accepted/live comparison treats an admitted leaf absent on both
sides as restored, including removed selector keys and install-only metadata.
Missing aggregate leaves with present descendants, absent objects and unavailable
source evidence do not establish restoration. Status and independent closure
share this comparison; historical fingerprints alone cannot hold restored drift.
