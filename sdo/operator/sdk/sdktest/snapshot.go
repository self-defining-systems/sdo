package sdktest

import (
<<<<<<< HEAD
	batchv1 "k8s.io/api/batch/v1"
=======
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
<<<<<<< HEAD
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
=======
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
>>>>>>> origin/vic-lsh/dev/sdo-improve
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
)

type Snapshot struct {
<<<<<<< HEAD
	Reachability *servicehealth.ReachabilityWindow `json:"-"`
	// Usage holds a synthetic resource usage window.
	Usage              *usage.Result                  `json:"-"`
	SecretList         []sdk.SecretMetadata           `json:"secrets"`
	NodeList           []corev1.Node                  `json:"nodes"`
	IngressList        []networkingv1.Ingress         `json:"ingresses"`
	ServiceAccountList []corev1.ServiceAccount        `json:"serviceAccounts"`
	PVCList            []corev1.PersistentVolumeClaim `json:"persistentVolumeClaims"`
	CronJobList        []batchv1.CronJob              `json:"cronJobs"`
	JobList            []batchv1.Job                  `json:"jobs"`
	DaemonSetList      []appsv1.DaemonSet             `json:"daemonSets"`
	// Logs holds synthetic bounded log windows by query ID.
	Logs map[string]logs.Result `json:"-"`
=======
	// ObjectSnapshotVersion 1 stores namespace-local objects in Objects and in
	// the typed lists; ObjectsOnlySnapshotVersion stores them in Objects only.
	ObjectSnapshotVersion int                         `json:"object_snapshot_version,omitempty"`
	Admission             *sdk.PodAdmissionResult     `json:"pod_admission,omitempty"`
	Objects               []unstructured.Unstructured `json:"objects,omitempty"`
>>>>>>> origin/vic-lsh/dev/sdo-improve
	// SourceName names the file a loaded snapshot came from, for messages.
	SourceName         string `json:"-"`
	sdk.ScopedSnapshot `json:"scoped_observation,omitempty"`
	NamespaceName      string                                  `json:"namespace"`
	ConfigMapList      []corev1.ConfigMap                      `json:"configMaps,omitempty"`
	ServiceList        []corev1.Service                        `json:"services,omitempty"`
	PodList            []corev1.Pod                            `json:"pods,omitempty"`
	DeploymentList     []appsv1.Deployment                     `json:"deployments,omitempty"`
	StatefulSetList    []appsv1.StatefulSet                    `json:"statefulSets,omitempty"`
	ReplicaSetList     []appsv1.ReplicaSet                     `json:"replicaSets,omitempty"`
	HPAList            []autoscalingv2.HorizontalPodAutoscaler `json:"horizontalPodAutoscalers,omitempty"`
	PDBList            []policyv1.PodDisruptionBudget          `json:"podDisruptionBudgets,omitempty"`
	EndpointList       []corev1.Endpoints                      `json:"endpoints,omitempty"`
	EndpointSliceList  []discoveryv1.EndpointSlice             `json:"endpointSlices,omitempty"`
	NetworkPolicyList  []networkingv1.NetworkPolicy            `json:"networkPolicies,omitempty"`
	EventList          []corev1.Event                          `json:"events,omitempty"`
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

<<<<<<< HEAD
// UsageResult implements usage.Source with isolated synthetic windows.
func (s Snapshot) UsageResult() (usage.Result, bool) {
	if s.Usage == nil {
		return usage.Result{}, false
	}
	return usage.Clone(*s.Usage), true
}

func (s Snapshot) DaemonSets() []appsv1.DaemonSet {
	items := make([]appsv1.DaemonSet, len(s.DaemonSetList))
	for i := range s.DaemonSetList {
		items[i] = *s.DaemonSetList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) Jobs() []batchv1.Job {
	items := make([]batchv1.Job, len(s.JobList))
	for i := range s.JobList {
		items[i] = *s.JobList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) CronJobs() []batchv1.CronJob {
	items := make([]batchv1.CronJob, len(s.CronJobList))
	for i := range s.CronJobList {
		items[i] = *s.CronJobList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) PersistentVolumeClaims() []corev1.PersistentVolumeClaim {
	items := make([]corev1.PersistentVolumeClaim, len(s.PVCList))
	for i := range s.PVCList {
		items[i] = *s.PVCList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) ServiceAccounts() []corev1.ServiceAccount {
	items := make([]corev1.ServiceAccount, len(s.ServiceAccountList))
	for i := range s.ServiceAccountList {
		items[i] = *s.ServiceAccountList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) Ingresses() []networkingv1.Ingress {
	items := make([]networkingv1.Ingress, len(s.IngressList))
	for i := range s.IngressList {
		items[i] = *s.IngressList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) Nodes() []corev1.Node {
	items := make([]corev1.Node, len(s.NodeList))
	for i := range s.NodeList {
		items[i] = *s.NodeList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

func (s Snapshot) Secrets() []sdk.SecretMetadata {
	items := make([]sdk.SecretMetadata, len(s.SecretList))
	for i := range s.SecretList {
		items[i] = *s.SecretList[i].DeepCopy()
	}
	sort.Slice(items, func(i, j int) bool {
		return lessObject(items[i].Namespace, items[i].Name, items[j].Namespace, items[j].Name)
	})
	return items
}

// LogResult implements logs.Source.
func (s Snapshot) LogResult(id string) (logs.Result, bool) { r, ok := s.Logs[id]; return r, ok }

=======
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

>>>>>>> origin/vic-lsh/dev/sdo-improve
func (s Snapshot) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if s.Reachability == nil {
		return servicehealth.ReachabilityWindow{}, false
	}
	return *s.Reachability, true
}
<<<<<<< HEAD
=======

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

// DiffAgainstSource replays only observations recorded on this exact snapshot.
func (s Snapshot) DiffAgainstSource(ref sdk.ObjectRef) (sdk.SourceDiff, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).DiffAgainstSource(ref)
}
>>>>>>> origin/vic-lsh/dev/sdo-improve
