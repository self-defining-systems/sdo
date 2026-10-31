package sdk

// IsSupportedWatchKind reports whether a watch names a snapshot or controller input.
// Kubernetes kinds require their exact served API version.
func IsSupportedWatchKind(watch WatchKind) bool {
	switch watch.APIVersion + "/" + watch.Kind {
	case "apps/v1/StatefulSet", "apps/v1/DaemonSet", "batch/v1/Job", "batch/v1/CronJob", "v1/PersistentVolumeClaim", "v1/ServiceAccount", "autoscaling/v2/HorizontalPodAutoscaler", "policy/v1/PodDisruptionBudget", "networking.k8s.io/v1/Ingress", "v1/Node", "v1/Secret", "v1/ConfigMap", "v1/Service", "v1/Pod", "v1/Endpoints", "v1/Event", "apps/v1/Deployment", "apps/v1/ReplicaSet", "discovery.k8s.io/v1/EndpointSlice", "networking.k8s.io/v1/NetworkPolicy", "sdo.dev/v1alpha1/SyntheticTraffic", "sdo.dev/v1alpha1/ServiceReachability", "sdo.dev/v1alpha1/Metrics", "sdo.dev/v1alpha1/ResourceUsage":
		return true
	default:
		return false
	}
}
