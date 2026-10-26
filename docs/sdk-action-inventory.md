# SDK operation coverage inventory

LE-X baseline: `151ac41b4`, 2026-10-25. The floor is kubectl's operation
vocabulary, including arbitrary discovered resources. RBAC is the execution
boundary. Reading Kubernetes Secret data is excluded; commands use credentials
from container environment references. Detector execution remains deterministic.

The coverage table is a contract: every row needs a production primitive and a
replay contract test, or an explicit remaining gap. General API operations must
not require accepted-source values, a fault-specific schema, or a safety grant.
Replay models API writes with the production executor against a fake API.
External commands need recorded observations **and an exact recorded effect**;
expected fixed snapshots alone never establish command semantics or success.

| Operation | Baseline primitive / gap | General primitive / replay model |
| --- | --- | --- |
| apply | ReconcileFromSource only; arbitrary desired object missing | ApplyObject; create or merge desired object, API replay |
| create | CreateFromSource only | ApplyObject (create mode); API replay |
| replace | source-owned fields only | ApplyObject (replace mode); API replay |
| patch-json | SetField limited; remove/insert missing | PatchObject JSON patch; API replay |
| patch-merge | SetField limited | PatchObject merge patch; API replay |
| patch-strategic | missing | PatchObject strategic merge for registered Kubernetes kinds; API replay |
| patch-list-match | RemoveServicePort only | PatchObject list edit by unique field match; API replay |
| delete | DeleteObject; namespace restriction | DeleteObject through general execution; API replay |
| scale | ScaleWorkload (Deployment/StatefulSet/ReplicaSet) | PatchObject spec.replicas for any scale target; API replay |
| rollout-restart | RestartRollout (Deployment only) | PatchObject template restart annotation; API replay |
| rollout-undo | Rollback with explicit revision | ApplyObject previous template / Rollback; API replay |
| rollout-pause | missing | PatchObject spec.paused; API replay |
| rollout-resume | missing | PatchObject spec.paused; API replay |
| set-image | SetField limited | PatchObject container image by identity; API replay |
| set-env | SetField env insertion | PatchObject env list by identity; API replay |
| set-resources | SetField limited | PatchObject container resources; API replay |
| label | missing metadata coverage | PatchObject metadata.labels; API replay |
| annotate | missing metadata coverage | PatchObject metadata.annotations; API replay |
| taint | missing cluster Node coverage | PatchObject Node.spec.taints; API replay |
| cordon | missing cluster Node coverage | PatchObject Node.spec.unschedulable; API replay |
| drain | missing eviction/subresource coverage | KubernetesRequest eviction plus Node patch; recorded API effect required |
| autoscale | HPA fields only, source-only create | ApplyObject HPA; API replay |
| exec | Exec exit code, replay refused | EnsureExec observe/ensure commands; exact recorded transition replay |
| cp | missing byte-stream input | EnsureExec tar/base64 command with input; recorded transition replay |
| get | Object / scoped reads | Object and KubernetesRequest; recorded observation |
| describe | typed objects / events | Object, Events, related objects; recorded observation |
| logs | Logs | Logs; recorded observation |
| events | Events | Events; snapshot replay |
| top | QueryMetrics; metrics API unavailable | KubernetesRequest metrics API; recorded observation |
| container-admin | EnsureDatabaseUser Mongo only | EnsureExec; exact recorded transition replay |
| container-reload | Exec, no replay | EnsureExec; exact recorded transition replay |
| container-files | Exec, no replay/input stream | EnsureExec; exact recorded transition replay |
| in-cluster-http | ProbeHTTP read only | EnsureHTTP observe/ensure requests; exact recorded transition replay |
| one-off-job | source-only Pod creation | RunJob desired Job/Pod plus completion observation; API + recorded completion |
| wait | rollout waiter only | WaitFor field/condition with bounded context; recorded observation |
| secret-data-read | excluded | Never read Kubernetes Secret data; credentials by reference only |

## Verification and residual gaps

Risk classes: Kubernetes protocol behavior, irreversible effects, secret
boundaries, replay classification. Required gates: fail-first SDK contracts,
fake-API production execution, exact replay (including missing/wrong effect
rejection), codec round trips, focused race tests, formatting/static checks and
the hermetic controller/worker gate. No live cluster or model is authorized.

An arbitrary subprocess or HTTP endpoint cannot be simulated from its name.
Recorded effects must bind the exact operation, target and observation, preserve
failure/unknown outcomes, and never infer a successful transition from an
expected answer. Missing evidence remains an evidence gap, not an SDK refusal.

SDK gaps recorded by reflection must be enumerable offline with incident ID,
fault class, requested primitive, reason and scratch operation. Inventory status
and executable coverage are updated with the implementation; a planned mapping
in this baseline document is not a claim that the operation already works.


## Implemented coverage contract

`docs/sdk-action-inventory.json` is the machine-readable inventory.
`runtime/replay.TestGeneralKubectlCoverageFloor` executes every non-excluded row
and rejects missing, duplicate, removed or newly unimplemented operations. General
object changes run the production executor against the fake API; external and
subresource effects require exact recorded transitions. Transport tests separately
exercise stdin-only SPDY, API options, redirect rejection and Secret boundaries.
See the [authoring vocabulary](../sdo/skills/sdo-playbook-authoring/references/general-actions.md)
for signatures and replay limitations. Token issuance can run through RBAC, but
credential responses are discarded; consume generated values inside a container.
The inventory's cp case covers input streams and container-side transfer; command
stdout is intentionally unavailable because credential data stays in containers.
