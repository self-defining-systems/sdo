package runtime

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// Programs cannot confuse an unobserved kind with a successfully empty list.
// program.RunDiagnose turns this bounded, content-free panic into Unknown.
// Detector callers must independently convert it into an evaluation error.
type causalDetectionView struct {
	core.DetectionSnapshot
	listed map[sdk.WatchKind]bool
}

func (v causalDetectionView) require(version, kind string) {
	if !v.listed[sdk.WatchKind{APIVersion: version, Kind: kind, Namespace: v.Namespace()}] {
		panic("causal_dependency_unobserved")
	}
}
func (v causalDetectionView) ConfigMaps() []corev1.ConfigMap {
	v.require("v1", "ConfigMap")
	return v.DetectionSnapshot.ConfigMaps()
}
func (v causalDetectionView) Services() []corev1.Service {
	v.require("v1", "Service")
	return v.DetectionSnapshot.Services()
}
func (v causalDetectionView) Pods() []corev1.Pod {
	v.require("v1", "Pod")
	return v.DetectionSnapshot.Pods()
}
func (v causalDetectionView) Deployments() []appsv1.Deployment {
	v.require("apps/v1", "Deployment")
	return v.DetectionSnapshot.Deployments()
}
func (v causalDetectionView) ReplicaSets() []appsv1.ReplicaSet {
	v.require("apps/v1", "ReplicaSet")
	return v.DetectionSnapshot.ReplicaSets()
}
func (v causalDetectionView) StatefulSets() []appsv1.StatefulSet {
	v.require("apps/v1", "StatefulSet")
	return v.DetectionSnapshot.StatefulSets()
}
func (v causalDetectionView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	v.require("autoscaling/v2", "HorizontalPodAutoscaler")
	return v.DetectionSnapshot.HorizontalPodAutoscalers()
}
func (v causalDetectionView) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	v.require("policy/v1", "PodDisruptionBudget")
	return v.DetectionSnapshot.PodDisruptionBudgets()
}
func (v causalDetectionView) Endpoints() []corev1.Endpoints {
	v.require("v1", "Endpoints")
	return v.DetectionSnapshot.Endpoints()
}
func (v causalDetectionView) EndpointSlices() []discoveryv1.EndpointSlice {
	v.require("discovery.k8s.io/v1", "EndpointSlice")
	return v.DetectionSnapshot.EndpointSlices()
}
func (v causalDetectionView) NetworkPolicies() []networkingv1.NetworkPolicy {
	v.require("networking.k8s.io/v1", "NetworkPolicy")
	return v.DetectionSnapshot.NetworkPolicies()
}
func (v causalDetectionView) Events() []corev1.Event {
	v.require("v1", "Event")
	return v.DetectionSnapshot.Events()
}
func (v causalDetectionView) ReadyEndpointCountForService(namespace, service string) int {
	v.require("v1", "Endpoints")
	v.require("discovery.k8s.io/v1", "EndpointSlice")
	return v.DetectionSnapshot.ReadyEndpointCountForService(namespace, service)
}
func (v causalDetectionView) PodsForService(namespace, service string) []corev1.Pod {
	v.require("v1", "Service")
	v.require("v1", "Pod")
	return v.DetectionSnapshot.PodsForService(namespace, service)
}
func (v causalDetectionView) RecentEventsFor(namespace, kind, name string) []corev1.Event {
	v.require("v1", "Event")
	return v.DetectionSnapshot.RecentEventsFor(namespace, kind, name)
}
func (v causalDetectionView) TrafficWindow(string) (traffic.Window, bool) {
	panic("causal_traffic_reader_unavailable")
}
