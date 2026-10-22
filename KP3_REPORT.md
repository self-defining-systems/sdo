# KP3: reviewed operational fields in proxy history

The proxy now records operational state useful to reflection while keeping a
positive, per-kind allowlist in `sdo/operator/apihistory/projection.go`. Every
projected object still has `redacted: true`. Fields absent from this table remain
opaque; omission is not evidence of absence in Kubernetes. Nothing changes the
store format, writer, lease, admission budgets, latency implementation, or CLI
continuity rules.

## Allowlist and justification

Paths below are relative to the object unless a shared shape is named. Each
nested object is projected field by field; no raw spec/status subtree is copied.
Strings, numbers and booleans must have the reviewed JSON shape. Malformed
identity/pagination/list shapes reject the representation instead of silently
becoming an empty list that could establish absence.

| Kind / shape | Retained fields | Justification |
| --- | --- | --- |
| All supported objects | `apiVersion`, `kind`; `metadata.name`, `.namespace`, `.resourceVersion`; `redacted: true` | Bind observations to known API identities and versions without interpreting opaque revisions. |
| Supported lists | Same identity; projected `items`; nonempty `metadata.continue` becomes `redacted` | Preserve pagination presence without retaining opaque cursor contents. Item schemas come from the enclosing list kind/version. |
| Labels, except Secret/ConfigMap | `metadata.labels` keys with empty values; workload template label keys | Values are unconstrained credential channels. Keys explain declared labeling dimensions without preserving arbitrary values. |
| Selectors | Service selector keys; structured `matchLabels` keys with empty values and `matchExpressions[].key/operator` | Explain selection dimensions and operators without persisting arbitrary match values. Expressions' `values` are excluded. These projections cannot prove selector equality or backend membership. |
| Service | `spec.type`; `spec.ports[].name/port/targetPort/protocol/nodePort`; selector keys | Explain routing and numeric/named targetPort mutations. |
| Service IP | Derived `spec.clusterIPPresent` from a present string `clusterIP` | Show whether an IP field was populated without retaining its address. Empty string is false; headless `None` is a populated field and therefore true. Missing/wrong-type input stays omitted. |
| Deployment | `spec.replicas`, `.strategy.type`, `.selector`, `.template` | Explain desired capacity, rollout mode and template changes. Strategy parameters remain opaque. |
| StatefulSet | `spec.replicas`, `.updateStrategy.type`, `.selector`, `.template` | Explain desired capacity and StatefulSet update mode. |
| ReplicaSet | `spec.replicas`, `.selector`, `.template` | Explain desired capacity and the pod template. There is no rollout strategy field. |
| Workload template | `spec.template.metadata.labels` keys; `.spec` uses the shared Pod spec shape | Compare rollout intent with observed Pods without annotations or command/env contents. |
| Shared Pod spec | `nodeName`; `containers` and `initContainers` use the container shape; `volumes[].name` | Identify placement, normal/init workloads and declared volume names. Volume sources and content remain opaque. |
| Container identity / ports | `name`, `image`; `ports[].name/containerPort/hostPort/protocol` | Explain image mutations, declared listeners and host-port conflicts. Host addresses are excluded. |
| Container resources | `resources.requests/limits` for `cpu`, `memory`, `ephemeral-storage`, `storage` only | Explain resource budgets; arbitrary extended-resource map keys/values are excluded. |
| Container env references | `env[].name`; `valueFrom.secretKeyRef/configMapKeyRef.name/key/optional`; `fieldRef.apiVersion/fieldPath`; `resourceFieldRef.containerName/resource/divisor` | Identify declared configuration dependencies and downward API sources, never their resolved values. `env[].value` is always excluded. |
| Container bulk env refs | `envFrom[].secretRef/configMapRef.name/key/optional` | Identify referenced objects without their payloads. Prefixes are excluded. |
| Container mounts | `volumeMounts[].name` | Correlate declarations with named volumes. Mount paths, subpaths and expressions are excluded. |
| Probes | `readinessProbe`, `livenessProbe`, `startupProbe`; HTTP `path/port/scheme`, TCP `port`, gRPC `port`; exec handler as `{}` only | Explain handler choice and target shape. Exec commands, HTTP headers/hosts and gRPC service strings are excluded. |
| Probe timing | `initialDelaySeconds`, `timeoutSeconds`, `periodSeconds`, `successThreshold`, `failureThreshold`, `terminationGracePeriodSeconds` | Explain startup grace and health-check cadence/thresholds. |
| Workload status | `replicas`, `readyReplicas`, `availableReplicas`, `unavailableReplicas`, `updatedReplicas`, `currentReplicas`, `fullyLabeledReplicas`; `conditions[].type/status/reason` when present | Explain capacity/readiness/availability and machine-readable condition reasons. Condition messages remain opaque. |
| Pod status | `phase`; conditions as above; `containerStatuses` and `initContainerStatuses` | Explain why a Pod was not ready without diagnostic message text. |
| Container status | `name`, `image`, `ready`, `restartCount`; `state` and `lastState`: waiting `reason`, running `{}`, terminated `reason/exitCode` | Explain crash loops, prior failure and completion. Messages, container/image IDs and termination details are excluded. |
| Pod IP / ownership | Derived `status.podIPPresent`; `metadata.ownerReferences[].kind/name` | Explain scheduling/network initialization and workload ancestry without Pod addresses, owner UIDs or write metadata. |
| Endpoints | `subsets[].readyAddressCount/notReadyAddressCount`; `subsets[].ports[].name/port/protocol` | Count ready/not-ready backend addresses without persisting IPs, hostnames or target references. Counts derive from the actual address arrays. |
| EndpointSlice | Top-level `readyAddressCount/notReadyAddressCount`; `ports[].name/port/protocol` | Count address entries by endpoint readiness. False is not ready; missing/null readiness counts as ready under Kubernetes semantics. Addresses and other endpoint fields are excluded. |
| Event | `type`, `reason`, `involvedObject.kind/namespace/name/apiVersion`, `count`, `firstTimestamp`, `lastTimestamp`, `eventTime`, `series.count/lastObservedTime` | Correlate machine-readable event categories and frequency/time with an object. `message` and unreviewed source/reporting fields are excluded. |
| HPA, v1/v2 | `spec.scaleTargetRef.kind/namespace/name/apiVersion`, `minReplicas`, `maxReplicas`; v1 `targetCPUUtilizationPercentage` | Explain scaling target and bounds. No HPA status or diagnostic text is copied. |
| HPA metrics | `metrics[].type`; resource `name`, containerResource `name/container`; pods/object/external `metric.name/selector`; object `describedObject` identity; targets `type/value/averageValue/averageUtilization` | Explain reviewed metric identifiers, selectors and numeric/quantity targets. No arbitrary metric/status mapping is copied. |
| HPA behavior | `scaleUp/scaleDown.stabilizationWindowSeconds/selectPolicy`; `policies[].type/value/periodSeconds` | Explain scaling speed and stabilization. |
| PDB | `spec.minAvailable/maxUnavailable`, selector shape, `unhealthyPodEvictionPolicy` | Explain disruption constraints using numeric/percentage values and selector structure. Status is excluded. |
| NetworkPolicy | `spec.podSelector`, `policyTypes`; ingress `from` / egress `to` peers with pod/namespace selectors and `ipBlock.cidr/except`; ports `protocol/port/endPort` | Explain declared traffic restrictions. CIDR rules are retained as policy declarations; observed endpoint addresses are not. Status and arbitrary text remain opaque. |
| Secret / ConfigMap | Unchanged identity and key names in `data`, `stringData`, `binaryData`, values replaced by empty strings | Preserve existing credential boundary. No labels, annotations, types, immutable flags or payloads are added. |

Reviewed strings such as names, images, paths and reasons are deliberately
retained for their operational meaning; this is field-based projection, not
content-based credential detection. All annotations, environment values,
commands/arguments, probe exec commands, Event/status/condition messages and
unlisted future fields are excluded regardless of nesting. Unsupported kinds,
versions and representations remain unavailable. Logs, watches, upgrades,
request bodies and headers keep their existing metadata-only handling.

## Tests and validation

All inputs are constructed synthetic fixtures and loopback HTTP servers. No
credentials were inspected and no live cluster was contacted.

- Failing tests first: `TestOperationalFieldAllowlist` and
  `TestEvidenceRewindsOperationalFieldsBeforeFirstMutation` failed against the
  metadata-only parent because spec/status fields were absent.
- `TestOperationalFieldAllowlist`: all 13 supported kinds, both single objects
  and lists; exact projected state and unchanged upstream bytes. Canary env
  values, args, commands, annotations, Event/status messages and probe exec
  commands are injected for every kind, including forbidden spec placements.
  Recursive injection covers templates, init containers, conditions, prior
  container state, probe headers/hosts, mount paths, labels/selector values and
  unknown fields. Every persisted file is scanned after recorder shutdown.
- Every pre-existing permanent synthetic credential/upgrade/log regression in
  `operatorcli/proxy_regression_test.go` remains intact and passes. Upstream
  admission/latency test changes arrive only through the parent rebase.
- `TestEvidenceRewindsOperationalFieldsBeforeFirstMutation`: real proxy GET,
  PATCH and GET for Service `targetPort: 8080 -> 9090` and Deployment image
  `old -> new`; offline `RunEvidence state --at before-first-mutation` returns
  the old operational state, while a later RFC3339 cut returns the new state.
  Both retain `unknown: true` and `redacted: true`.
- `TestProjectionRejectsUnreviewedShapes`: object/array smuggling into scalar
  leaves, empty/headless IP presence, empty endpoint arrays, unspecified slice
  readiness, list item schema authority and HPA v1.
- `TestMalformedIdentityNeverBecomesEmptyList`: failing regression followed by
  rejection of malformed list/identity/pagination fields to preserve Unknown.
- Existing apihistory review fixture now keeps a synthetic operational Event
  reason (`BackOff`) while retaining its message canary, matching the new
  explicitly reviewed reason contract.
- Normal and race suites: `go test [ -race ] ./apihistory ./operatorcli ./kubeproxy -count=1`.
- Required gates: `bash scripts/format_code.sh`, `bash scripts/check_errors.sh`.
  Static checks include ruff, tach, pyright, proto generation, Go core purity
  and benchmark main compilation. Pyright has one existing `__all__` warning.
- The changed `analyze-experiment` skill passes the maintain-skills linter.
  The full skills directory has 15 pre-existing size/layout/link findings in
  unrelated skills; none concern this skill or its changed telemetry reference.
- `git diff --check` passes.

## Stack and head

Branch: `vic-lsh/dev/kube-proxy-4`; PR target: `vic-lsh/dev/sdo-improve`.
Rebased onto PR #130 head `8fd80b8909c71db52adad191d336000e5753b5cf`, which
contains PR #128 head `1799087caeb82a955a56d1bd11b165cb042d2974`.

Implementation head and final validation results are recorded below after the
last code commit. This report is committed separately so its recorded code SHA
is stable; the PR head also includes this documentation-only report commit.

Validated implementation head: `bec735b3c6ffde372670e9c16f4ffd260afbbed4`.

Final results after rebase and the malformed-identity fix:

- Normal: apihistory 0.031s, operatorcli 34.276s, kubeproxy 0.336s — PASS.
- Race: apihistory 1.073s, operatorcli 31.478s, kubeproxy 1.058s — PASS.
- Formatting and required static checks — PASS (existing pyright warning above).
- Changed skill lint and whitespace check — PASS.

No merge was performed. Unrelated app submodule state was left untouched.
