package sdktest

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// The kinds a snapshot may withhold are the trimmable ones (capture_budget.go) and ConfigMaps, whose values it
// redacts. A replay notes each read of them so DiagnoseOnIncident can tell a NoMatch the capture may explain from
// one it cannot.

func (v *ReplayReadView) noteRead(kind string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.kindsRead == nil {
		v.kindsRead = map[string]bool{}
	}
	v.kindsRead[kind] = true
}

// KindsRead names the object kinds the replayed program read from the snapshot.
func (v *ReplayReadView) KindsRead() map[string]bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]bool, len(v.kindsRead))
	for kind := range v.kindsRead {
		out[kind] = true
	}
	return out
}

// Object notes the kind it reads, then answers from the snapshot.
func (v *ReplayReadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	v.noteRead(ref.Kind)
	object, err := v.Snapshot.Object(ref)
	v.noteReadFailure(err)
	return object, err
}

// RecentEventsFor notes the Event read it makes.
func (v *ReplayReadView) RecentEventsFor(namespace string, kind string, name string) []corev1.Event {
	v.noteRead("Event")
	return v.Snapshot.RecentEventsFor(namespace, kind, name)
}

// ReadyEndpointCountForService notes the Endpoints and EndpointSlice reads it makes.
func (v *ReplayReadView) ReadyEndpointCountForService(namespace string, service string) int {
	v.noteRead("Endpoints")
	v.noteRead("EndpointSlice")
	return v.Snapshot.ReadyEndpointCountForService(namespace, service)
}

func (v *ReplayReadView) ConfigMaps() []corev1.ConfigMap {
	v.noteRead("ConfigMap")
	return v.Snapshot.ConfigMaps()
}

func (v *ReplayReadView) Events() []corev1.Event {
	v.noteRead("Event")
	return v.Snapshot.Events()
}

func (v *ReplayReadView) ServiceAccounts() []corev1.ServiceAccount {
	v.noteRead("ServiceAccount")
	return v.Snapshot.ServiceAccounts()
}

func (v *ReplayReadView) ReplicaSets() []appsv1.ReplicaSet {
	v.noteRead("ReplicaSet")
	return v.Snapshot.ReplicaSets()
}

func (v *ReplayReadView) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	v.noteRead("PodDisruptionBudget")
	return v.Snapshot.PodDisruptionBudgets()
}

func (v *ReplayReadView) NetworkPolicies() []networkingv1.NetworkPolicy {
	v.noteRead("NetworkPolicy")
	return v.Snapshot.NetworkPolicies()
}

func (v *ReplayReadView) Ingresses() []networkingv1.Ingress {
	v.noteRead("Ingress")
	return v.Snapshot.Ingresses()
}

func (v *ReplayReadView) PersistentVolumeClaims() []corev1.PersistentVolumeClaim {
	v.noteRead("PersistentVolumeClaim")
	return v.Snapshot.PersistentVolumeClaims()
}

func (v *ReplayReadView) Jobs() []batchv1.Job {
	v.noteRead("Job")
	return v.Snapshot.Jobs()
}

func (v *ReplayReadView) CronJobs() []batchv1.CronJob {
	v.noteRead("CronJob")
	return v.Snapshot.CronJobs()
}

func (v *ReplayReadView) DaemonSets() []appsv1.DaemonSet {
	v.noteRead("DaemonSet")
	return v.Snapshot.DaemonSets()
}

func (v *ReplayReadView) EndpointSlices() []discoveryv1.EndpointSlice {
	v.noteRead("EndpointSlice")
	return v.Snapshot.EndpointSlices()
}

func (v *ReplayReadView) Endpoints() []corev1.Endpoints {
	v.noteRead("Endpoints")
	return v.Snapshot.Endpoints()
}

func (v *ReplayReadView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	v.noteRead("HorizontalPodAutoscaler")
	return v.Snapshot.HorizontalPodAutoscalers()
}

func (v *ReplayReadView) Secrets() []sdk.SecretMetadata {
	v.noteRead("Secret")
	return v.Snapshot.Secrets()
}

func (v *ReplayReadView) Nodes() []corev1.Node {
	v.noteRead("Node")
	return v.Snapshot.Nodes()
}

func (v *ReplayReadView) noteReadFailure(err error) {
	if v.checkRecoveredReads && err != nil {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.readFailed = true
	}
}

func (v *ReplayReadView) hadReadFailure() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.readFailed
}

// Additional coverage tracking is advisory and cannot alter the open replay verdict.
func (v *ReplayReadView) noteRecoveredRead(kind string) {
	if v.checkRecoveredReads {
		v.noteRead(kind)
	}
}

func (v *ReplayReadView) Pods() []corev1.Pod {
	v.noteRecoveredRead("Pod")
	return v.Snapshot.Pods()
}

func (v *ReplayReadView) PodsForService(namespace, name string) []corev1.Pod {
	v.noteRecoveredRead("Pod")
	v.noteRecoveredRead("Service")
	return v.Snapshot.PodsForService(namespace, name)
}

func (v *ReplayReadView) Services() []corev1.Service {
	v.noteRecoveredRead("Service")
	return v.Snapshot.Services()
}

func (v *ReplayReadView) Deployments() []appsv1.Deployment {
	v.noteRecoveredRead("Deployment")
	return v.Snapshot.Deployments()
}

func (v *ReplayReadView) StatefulSets() []appsv1.StatefulSet {
	v.noteRecoveredRead("StatefulSet")
	return v.Snapshot.StatefulSets()
}

func (v *ReplayReadView) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if !v.checkRecoveredReads {
		return v.Snapshot.CaptureObjects(refs)
	}
	// Snapshot.CaptureObjects returns the whole inventory.
	for kind := range v.TrimmedObjects {
		v.noteRecoveredRead(kind)
	}
	v.noteRecoveredRead("ConfigMap")
	objects, err := v.Snapshot.CaptureObjects(refs)
	v.noteReadFailure(err)
	for i := range objects {
		objects[i] = *objects[i].DeepCopy()
	}
	return objects, err
}
