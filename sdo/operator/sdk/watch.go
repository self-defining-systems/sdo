package sdk

import "sort"

var supportedWatchKinds = map[string]struct{}{
	"admissionregistration.k8s.io/v1/MutatingWebhookConfiguration":   {},
	"admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration": {},
	"apps/v1/DaemonSet":                      {},
	"apps/v1/Deployment":                     {},
	"apps/v1/ReplicaSet":                     {},
	"apps/v1/StatefulSet":                    {},
	"autoscaling/v2/HorizontalPodAutoscaler": {},
	"batch/v1/CronJob":                       {},
	"batch/v1/Job":                           {},
	"discovery.k8s.io/v1/EndpointSlice":      {},
	"networking.k8s.io/v1/Ingress":           {},
	"networking.k8s.io/v1/NetworkPolicy":     {},
	"policy/v1/PodDisruptionBudget":          {},
	// Source-drift inventory kinds; learned drift playbooks pair with them.
	"rbac.authorization.k8s.io/v1/ClusterRole":        {},
	"rbac.authorization.k8s.io/v1/ClusterRoleBinding": {},
	"rbac.authorization.k8s.io/v1/Role":               {},
	"rbac.authorization.k8s.io/v1/RoleBinding":        {},
	"v1/ConfigMap":             {},
	"v1/Endpoints":             {},
	"v1/Event":                 {},
	"v1/Node":                  {},
	"v1/PersistentVolumeClaim": {},
	"v1/Pod":                   {},
	"v1/Secret":                {},
	"v1/Service":               {},
	"v1/ServiceAccount":        {},
	// Emitted by controller observers rather than by a Kubernetes informer.
	"sdo.dev/v1alpha1/Logs":                {},
	"sdo.dev/v1alpha1/Metrics":             {},
	"sdo.dev/v1alpha1/ResourceUsage":       {},
	"sdo.dev/v1alpha1/ServiceReachability": {},
	"sdo.dev/v1alpha1/SyntheticTraffic":    {},
}

// IsSupportedWatchKind reports whether a watch names a snapshot or controller input.
// Kubernetes kinds require their exact served API version.
func IsSupportedWatchKind(watch WatchKind) bool {
	_, ok := supportedWatchKinds[watch.APIVersion+"/"+watch.Kind]
	return ok
}

// SupportedWatchKinds lists, sorted, the "apiVersion/Kind" resources a
// detector may watch.
func SupportedWatchKinds() []string {
	keys := make([]string, 0, len(supportedWatchKinds))
	for key := range supportedWatchKinds {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
