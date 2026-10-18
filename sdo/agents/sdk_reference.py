"""Compact, trusted reference for the Go detector SDK that agents author against.

Detector authors (the health judge and the responder's reflection) receive this text
instead of exploring ``sdo/operator/sdk``, which is outside the application checkout.
"""

DETECTOR_SDK_REFERENCE = """Trusted sdo/operator/sdk API reference (do not search outside the application checkout):

- import path `sdo.dev/operator/sdk`; test helper import path `sdo.dev/operator/sdk/sdktest`.
- `type Detector interface { Spec() DetectorSpec; Detect(context.Context, DetectionContext) ([]Finding, error) }`.
- `DetectionContext` exposes `Namespace() string`, `ConfigMaps() []corev1.ConfigMap`,
  `Services() []corev1.Service`, `Pods() []corev1.Pod`, `Deployments() []appsv1.Deployment`,
  `ReplicaSets() []appsv1.ReplicaSet`, `Endpoints() []corev1.Endpoints`,
  `EndpointSlices() []discoveryv1.EndpointSlice`, `NetworkPolicies() []networkingv1.NetworkPolicy`,
  `Events() []corev1.Event`, `ReadyEndpointCountForService(namespace, service string) int`,
  `PodsForService(namespace, service string) []corev1.Pod`, and
  `RecentEventsFor(namespace, kind, name string) []corev1.Event`.
- `sdk.ConfigMapReferencesForDeployment(appsv1.Deployment) []sdk.ConfigMapReference` returns sorted,
  deduplicated volume, projected-volume, envFrom, and env ConfigMap references; each reference has `Name string`
  and `Optional bool`.
- `sdk.ServiceExpectsEndpoints(corev1.Service) bool` is false for ExternalName Services. They are DNS aliases that
  never have Endpoints, EndpointSlices, or selected pods, and the runtime environment may replace any source Service
  with one. Skip them before endpoint or pod readiness checks, whatever the health objective says about endpoints;
  the independent validator rejects a health detector that reports an ExternalName Service for missing endpoints.
- `sdk.Finding` has string fields `RuleID`, `Summary`, `Evidence`, and `Fingerprint`; enum fields `Status` and
  `Severity`; `PrimaryResource sdk.ObjectRef`; `RelatedResources []sdk.ObjectRef`; `Playbooks []string`;
  `ParameterBindings map[string]sdk.ObjectRef`; and `Metadata map[string]any`. Use `sdk.FindingActive`,
  `sdk.SeverityCritical`, and stable fingerprints. In particular, never use `map[string]string` for Metadata.
- `sdk.ObjectRef` fields are `APIVersion`, `Kind`, `Namespace`, and `Name`.
- `sdktest.Snapshot` implements DetectionContext. Its fields are `NamespaceName`, `ConfigMapList`, `ServiceList`,
  `PodList`, `DeploymentList`, `ReplicaSetList`, `EndpointList`, `EndpointSliceList`, `NetworkPolicyList`, and
  `EventList`.
"""
