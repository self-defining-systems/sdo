package sdktest

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

type Snapshot struct {
	ObjectSnapshotVersion int                         `json:"object_snapshot_version,omitempty"`
	Admission             *sdk.PodAdmissionResult     `json:"pod_admission,omitempty"`
	Objects               []unstructured.Unstructured `json:"objects,omitempty"`
	// SourceName names the file a loaded snapshot came from, for messages.
	SourceName         string `json:"-"`
	sdk.ScopedSnapshot `json:"scoped_observation,omitempty"`
	NamespaceName      string                                  `json:"namespace"`
	ConfigMapList      []corev1.ConfigMap                      `json:"configMaps"`
	ServiceList        []corev1.Service                        `json:"services"`
	PodList            []corev1.Pod                            `json:"pods"`
	DeploymentList     []appsv1.Deployment                     `json:"deployments"`
	StatefulSetList    []appsv1.StatefulSet                    `json:"statefulSets"`
	ReplicaSetList     []appsv1.ReplicaSet                     `json:"replicaSets"`
	HPAList            []autoscalingv2.HorizontalPodAutoscaler `json:"horizontalPodAutoscalers"`
	PDBList            []policyv1.PodDisruptionBudget          `json:"podDisruptionBudgets"`
	EndpointList       []corev1.Endpoints                      `json:"endpoints"`
	EndpointSliceList  []discoveryv1.EndpointSlice             `json:"endpointSlices"`
	NetworkPolicyList  []networkingv1.NetworkPolicy            `json:"networkPolicies"`
	EventList          []corev1.Event                          `json:"events"`
	ObservedAtTime     *time.Time                              `json:"observedAt,omitempty"`
	// Traffic holds synthetic-traffic observations by workload name.
	Traffic map[string]traffic.Window `json:"traffic,omitempty"`
	// Metrics holds frozen metric query results by query ID.
	Metrics map[string]metrics.Result `json:"metrics,omitempty"`
	// Alerts holds the frozen alerts of the evaluation cycle.
	Alerts *metrics.AlertsResult `json:"alerts,omitempty"`
	// Readings are the runtime observations made on this exact snapshot.
	Readings     []PrimitiveCall                   `json:"readings,omitempty"`
	Reachability *servicehealth.ReachabilityWindow `json:"reachability,omitempty"`
}

// ObservedAt implements sdk.ObservationClock; it is zero when unrecorded.
func (s Snapshot) ObservedAt() time.Time {
	if s.ObservedAtTime == nil {
		return time.Time{}
	}
	return *s.ObservedAtTime
}

func (s Snapshot) StatefulSets() []appsv1.StatefulSet {
	items := make([]appsv1.StatefulSet, len(s.StatefulSetList))
	for index := range s.StatefulSetList {
		items[index] = *s.StatefulSetList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	items := make([]autoscalingv2.HorizontalPodAutoscaler, len(s.HPAList))
	for index := range s.HPAList {
		items[index] = *s.HPAList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	items := make([]policyv1.PodDisruptionBudget, len(s.PDBList))
	for index := range s.PDBList {
		items[index] = *s.PDBList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

// TrafficWindow implements traffic.Source.
func (s Snapshot) TrafficWindow(workload string) (traffic.Window, bool) {
	window, ok := s.Traffic[workload]
	return window, ok
}

// MetricResult implements metrics.Source.
func (s Snapshot) MetricResult(id string) (metrics.Result, bool) {
	result, ok := s.Metrics[id]
	return result, ok
}

// AlertsResult implements metrics.AlertSource.
func (s Snapshot) AlertsResult() (metrics.AlertsResult, bool) {
	if s.Alerts == nil {
		return metrics.AlertsResult{}, false
	}
	return *s.Alerts, true
}

func (s Snapshot) Namespace() string {
	return s.NamespaceName
}

func (s Snapshot) ConfigMaps() []corev1.ConfigMap {
	configMaps := make([]corev1.ConfigMap, len(s.ConfigMapList))
	for index := range s.ConfigMapList {
		configMaps[index] = *s.ConfigMapList[index].DeepCopy()
	}
	sort.Slice(configMaps, func(left int, right int) bool {
		if configMaps[left].Namespace != configMaps[right].Namespace {
			return configMaps[left].Namespace < configMaps[right].Namespace
		}
		return configMaps[left].Name < configMaps[right].Name
	})
	return configMaps
}

func (s Snapshot) Services() []corev1.Service {
	items := make([]corev1.Service, len(s.ServiceList))
	for index := range s.ServiceList {
		items[index] = *s.ServiceList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) Pods() []corev1.Pod {
	items := make([]corev1.Pod, len(s.PodList))
	for index := range s.PodList {
		items[index] = *s.PodList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) Deployments() []appsv1.Deployment {
	items := make([]appsv1.Deployment, len(s.DeploymentList))
	for index := range s.DeploymentList {
		items[index] = *s.DeploymentList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) ReplicaSets() []appsv1.ReplicaSet {
	items := make([]appsv1.ReplicaSet, len(s.ReplicaSetList))
	for index := range s.ReplicaSetList {
		items[index] = *s.ReplicaSetList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) Endpoints() []corev1.Endpoints {
	items := make([]corev1.Endpoints, len(s.EndpointList))
	for index := range s.EndpointList {
		items[index] = *s.EndpointList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) EndpointSlices() []discoveryv1.EndpointSlice {
	items := make([]discoveryv1.EndpointSlice, len(s.EndpointSliceList))
	for index := range s.EndpointSliceList {
		items[index] = *s.EndpointSliceList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) NetworkPolicies() []networkingv1.NetworkPolicy {
	items := make([]networkingv1.NetworkPolicy, len(s.NetworkPolicyList))
	for index := range s.NetworkPolicyList {
		items[index] = *s.NetworkPolicyList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) Events() []corev1.Event {
	items := make([]corev1.Event, len(s.EventList))
	for index := range s.EventList {
		items[index] = *s.EventList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s Snapshot) ReadyEndpointCountForService(namespace string, service string) int {
	ready := 0
	for _, endpoints := range s.Endpoints() {
		if endpoints.Namespace != namespace || endpoints.Name != service {
			continue
		}
		for _, subset := range endpoints.Subsets {
			ready += len(subset.Addresses)
		}
	}
	return ready
}

func (s Snapshot) PodsForService(namespace string, service string) []corev1.Pod {
	var selector labels.Selector
	for _, candidate := range s.Services() {
		if candidate.Namespace == namespace && candidate.Name == service {
			selector = labels.SelectorFromSet(candidate.Spec.Selector)
			break
		}
	}
	if selector == nil || selector.Empty() {
		return nil
	}

	pods := make([]corev1.Pod, 0)
	for _, pod := range s.Pods() {
		if pod.Namespace == namespace && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods
}

func (s Snapshot) RecentEventsFor(namespace string, kind string, name string) []corev1.Event {
	events := make([]corev1.Event, 0)
	for _, event := range s.Events() {
		ref := event.InvolvedObject
		if ref.Namespace == namespace && ref.Kind == kind && ref.Name == name {
			events = append(events, event)
		}
	}
	return events
}

func lessObject(leftNamespace string, leftName string, rightNamespace string, rightName string) bool {
	if leftNamespace != rightNamespace {
		return leftNamespace < rightNamespace
	}
	return leftName < rightName
}

func (s Snapshot) QueryMetrics(q metrics.Query) metrics.Result {
	if err := q.Validate(); err != nil {
		return metrics.Result{Query: q, Error: err.Error()}
	}
	if r, ok := s.Metrics[q.ID]; ok && r.Query == q {
		return r
	}
	return metrics.Result{Query: q, Error: "metric query not recorded"}
}

// ObservedView binds detector reads to this snapshot's recorded evidence.
func (s Snapshot) ObservedView() (sdk.DetectionContext, func() error) {
	return program.CheckReads(&ReplayReadView{Snapshot: s, Calls: s.Readings})
}

func (s Snapshot) RuntimeReadView() program.ReadView {
	return &ReplayReadView{Snapshot: s, Calls: s.Readings}
}

func (s Snapshot) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if s.Reachability == nil {
		return servicehealth.ReachabilityWindow{}, false
	}
	return *s.Reachability, true
}

func (s Snapshot) PodAdmission() (sdk.PodAdmissionResult, bool) {
	if s.Admission == nil {
		return sdk.PodAdmissionResult{}, false
	}
	return *s.Admission, true
}

func (s Snapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	// Generic objects are authoritative; old fixtures fall back to typed arrays.
	objects := append(append([]unstructured.Unstructured(nil), s.Objects...), sdk.TypedObjects(s)...)
	return sdk.FindObject(objects, ref)
}
func (s Snapshot) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	return append(sdk.TypedObjects(s), s.Objects...), nil
}
