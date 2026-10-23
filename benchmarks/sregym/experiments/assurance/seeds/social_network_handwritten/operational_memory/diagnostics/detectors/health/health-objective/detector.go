// Package objective checks the human health objective against the deployed
// Kubernetes objects of the application. It is purely observational: it names
// the required objects the application's own manifests deploy and reports when
// one is missing, scaled to zero, unavailable, without ready endpoints, or
// isolated by a NetworkPolicy. It encodes no knowledge of any particular fault.
package objective

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
)

const healthObjectiveDigest = "84a6ee657419e25a179d4bd3fbc11bf3e88aae1693ed4e3d1ceaf8d8b4fb4e18"

var requiredDeployments = map[string]struct{}{
	"compose-post-service":   {},
	"home-timeline-redis":    {},
	"home-timeline-service":  {},
	"jaeger":                 {},
	"media-frontend":         {},
	"media-memcached":        {},
	"media-mongodb":          {},
	"media-service":          {},
	"nginx-thrift":           {},
	"post-storage-memcached": {},
	"post-storage-mongodb":   {},
	"post-storage-service":   {},
	"social-graph-mongodb":   {},
	"social-graph-redis":     {},
	"social-graph-service":   {},
	"text-service":           {},
	"unique-id-service":      {},
	"url-shorten-memcached":  {},
	"url-shorten-mongodb":    {},
	"url-shorten-service":    {},
	"user-memcached":         {},
	"user-mention-service":   {},
	"user-mongodb":           {},
	"user-service":           {},
	"user-timeline-mongodb":  {},
	"user-timeline-redis":    {},
	"user-timeline-service":  {},
}

var requiredServices = map[string]struct{}{
	"compose-post-service":   {},
	"home-timeline-redis":    {},
	"home-timeline-service":  {},
	"jaeger":                 {},
	"media-frontend":         {},
	"media-memcached":        {},
	"media-mongodb":          {},
	"media-service":          {},
	"nginx-thrift":           {},
	"post-storage-memcached": {},
	"post-storage-mongodb":   {},
	"post-storage-service":   {},
	"social-graph-mongodb":   {},
	"social-graph-redis":     {},
	"social-graph-service":   {},
	"text-service":           {},
	"unique-id-service":      {},
	"url-shorten-memcached":  {},
	"url-shorten-mongodb":    {},
	"url-shorten-service":    {},
	"user-memcached":         {},
	"user-mention-service":   {},
	"user-mongodb":           {},
	"user-service":           {},
	"user-timeline-mongodb":  {},
	"user-timeline-redis":    {},
	"user-timeline-service":  {},
}

var requiredConfigMaps = map[string]struct{}{
	"compose-post-service":   {},
	"home-timeline-redis":    {},
	"home-timeline-service":  {},
	"jaeger":                 {},
	"media-frontend":         {},
	"media-memcached":        {},
	"media-mongodb":          {},
	"media-service":          {},
	"nginx-thrift":           {},
	"post-storage-memcached": {},
	"post-storage-mongodb":   {},
	"post-storage-service":   {},
	"social-graph-mongodb":   {},
	"social-graph-redis":     {},
	"social-graph-service":   {},
	"text-service":           {},
	"unique-id-service":      {},
	"url-shorten-memcached":  {},
	"url-shorten-mongodb":    {},
	"url-shorten-service":    {},
	"user-memcached":         {},
	"user-mention-service":   {},
	"user-mongodb":           {},
	"user-service":           {},
	"user-timeline-mongodb":  {},
	"user-timeline-redis":    {},
	"user-timeline-service":  {},
}

// requiredWorkloadLabels are the pod template labels of the required
// Deployments; a NetworkPolicy that selects any of them can isolate the
// application.
var requiredWorkloadLabels = []map[string]string{
	{"service": "compose-post-service", "app": "compose-post-service"},
	{"service": "home-timeline-redis", "app": "home-timeline-redis"},
	{"service": "home-timeline-service", "app": "home-timeline-service"},
	{"service": "jaeger", "app": "jaeger"},
	{"service": "media-frontend", "app": "media-frontend"},
	{"service": "media-memcached", "app": "media-memcached"},
	{"service": "media-mongodb", "app": "media-mongodb"},
	{"service": "media-service", "app": "media-service"},
	{"service": "nginx-thrift", "app": "nginx-thrift"},
	{"service": "post-storage-memcached", "app": "post-storage-memcached"},
	{"service": "post-storage-mongodb", "app": "post-storage-mongodb"},
	{"service": "post-storage-service", "app": "post-storage-service"},
	{"service": "social-graph-mongodb", "app": "social-graph-mongodb"},
	{"service": "social-graph-redis", "app": "social-graph-redis"},
	{"service": "social-graph-service", "app": "social-graph-service"},
	{"service": "text-service", "app": "text-service"},
	{"service": "unique-id-service", "app": "unique-id-service"},
	{"service": "url-shorten-memcached", "app": "url-shorten-memcached"},
	{"service": "url-shorten-mongodb", "app": "url-shorten-mongodb"},
	{"service": "url-shorten-service", "app": "url-shorten-service"},
	{"service": "user-memcached", "app": "user-memcached"},
	{"service": "user-mention-service", "app": "user-mention-service"},
	{"service": "user-mongodb", "app": "user-mongodb"},
	{"service": "user-service", "app": "user-service"},
	{"service": "user-timeline-mongodb", "app": "user-timeline-mongodb"},
	{"service": "user-timeline-redis", "app": "user-timeline-redis"},
	{"service": "user-timeline-service", "app": "user-timeline-service"},
}

type Detector struct{}

func New() sdk.Detector { return Detector{} }

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "health-objective", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: 30 * time.Second,
		Watches: []sdk.WatchKind{
			{APIVersion: "v1", Kind: "Pod"},
			{APIVersion: "v1", Kind: "ConfigMap"},
			{APIVersion: "v1", Kind: "Service"},
			{APIVersion: "apps/v1", Kind: "Deployment"},
			{APIVersion: "apps/v1", Kind: "ReplicaSet"},
			{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
			{APIVersion: "v1", Kind: "Endpoints"},
			{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"},
		},
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
	}
}

func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	namespace := snapshot.Namespace()
	findings := make([]sdk.Finding, 0)
	deployments := make(map[string]struct{})
	for _, deployment := range snapshot.Deployments() {
		if deployment.Namespace != namespace {
			continue
		}
		deployments[deployment.Name] = struct{}{}
		for _, ref := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if ref.Optional || hasConfigMap(snapshot.ConfigMaps(), deployment.Namespace, ref.Name) {
				continue
			}
			findings = append(findings, healthFinding(
				"required-configmap-missing",
				"A required deployment ConfigMap reference is missing",
				fmt.Sprintf("deployment %s/%s requires ConfigMap %s", deployment.Namespace, deployment.Name, ref.Name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: deployment.Namespace, Name: ref.Name},
			))
		}
		if _, required := requiredDeployments[deployment.Name]; !required {
			continue
		}
		if deployment.Spec.Selector == nil || !selectorMatchesLabels(deployment.Spec.Selector.MatchLabels, deployment.Spec.Template.Labels) {
			findings = append(findings, healthFinding(
				"deployment-selector-mismatch",
				"A required deployment selector does not match its pod template labels",
				fmt.Sprintf("deployment %s/%s selector does not match template labels", deployment.Namespace, deployment.Name),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
		}
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		if desired == 0 {
			findings = append(findings, healthFinding(
				"deployment-scaled-to-zero",
				"A required deployment has no desired replicas",
				fmt.Sprintf("deployment %s/%s is explicitly scaled to zero", deployment.Namespace, deployment.Name),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
			continue
		}
		if deployment.Status.AvailableReplicas < desired {
			findings = append(findings, healthFinding(
				"deployment-unavailable",
				"A required deployment has unavailable replicas",
				fmt.Sprintf("deployment %s/%s has %d/%d available replicas",
					deployment.Namespace, deployment.Name, deployment.Status.AvailableReplicas, desired),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
		}
	}
	for _, name := range sortedNames(requiredDeployments) {
		if _, ok := deployments[name]; !ok {
			findings = append(findings, healthFinding(
				"deployment-missing", "A required deployment is missing",
				fmt.Sprintf("deployment %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: name},
			))
		}
	}
	services := make(map[string]struct{})
	for _, service := range snapshot.Services() {
		if service.Namespace != namespace {
			continue
		}
		services[service.Name] = struct{}{}
		if _, required := requiredServices[service.Name]; !required || !sdk.ServiceExpectsEndpoints(service) {
			continue
		}
		if snapshot.ReadyEndpointCountForService(service.Namespace, service.Name) == 0 {
			findings = append(findings, healthFinding(
				"service-without-ready-endpoints",
				"A selected service has no ready endpoints",
				fmt.Sprintf("service %s/%s has zero ready endpoints", service.Namespace, service.Name),
				sdk.ObjectRefFrom("Service", "v1", &service),
			))
		}
	}
	for _, name := range sortedNames(requiredServices) {
		if _, ok := services[name]; !ok {
			findings = append(findings, healthFinding(
				"service-missing", "A required service is missing",
				fmt.Sprintf("service %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: name},
			))
		}
	}
	for _, name := range sortedNames(requiredConfigMaps) {
		if !hasConfigMap(snapshot.ConfigMaps(), namespace, name) {
			findings = append(findings, healthFinding(
				"configmap-missing", "A required ConfigMap is missing",
				fmt.Sprintf("configmap %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: name},
			))
		}
	}
	for _, policy := range snapshot.NetworkPolicies() {
		if policy.Namespace != namespace || !policySelectsRequiredWorkload(policy.Spec.PodSelector) {
			continue
		}
		ruleID, summary, evidence := policyIsolation(policy)
		if ruleID == "" {
			continue
		}
		findings = append(findings, healthFinding(
			ruleID, summary,
			fmt.Sprintf("networkpolicy %s/%s %s", policy.Namespace, policy.Name, evidence),
			sdk.ObjectRefFrom("NetworkPolicy", "networking.k8s.io/v1", &policy),
		))
	}
	return findings, nil
}

// policyIsolation reports a policy that admits no traffic at all in a
// direction it isolates.
func policyIsolation(policy networkingv1.NetworkPolicy) (string, string, string) {
	isolatesIngress, isolatesEgress := false, false
	for _, policyType := range policy.Spec.PolicyTypes {
		isolatesIngress = isolatesIngress || policyType == networkingv1.PolicyTypeIngress
		isolatesEgress = isolatesEgress || policyType == networkingv1.PolicyTypeEgress
	}
	if len(policy.Spec.PolicyTypes) == 0 {
		// The API server defaults an unset list to Ingress, plus Egress when egress rules exist.
		isolatesIngress = true
		isolatesEgress = len(policy.Spec.Egress) > 0
	}
	deniesIngress := isolatesIngress && len(policy.Spec.Ingress) == 0
	deniesEgress := isolatesEgress && len(policy.Spec.Egress) == 0
	switch {
	case deniesIngress && deniesEgress:
		return "network-policy-total-isolation", "A network policy completely isolates selected application pods", "denies all ingress and egress"
	case deniesIngress:
		return "network-policy-denies-all-ingress", "A network policy denies all ingress to selected application pods", "denies all ingress"
	case deniesEgress:
		return "network-policy-denies-all-egress", "A network policy denies all egress from selected application pods", "denies all egress"
	}
	return "", "", ""
}

func policySelectsRequiredWorkload(podSelector metav1.LabelSelector) bool {
	selector, err := metav1.LabelSelectorAsSelector(&podSelector)
	if err != nil {
		return false
	}
	for _, workload := range requiredWorkloadLabels {
		if selector.Matches(labels.Set(workload)) {
			return true
		}
	}
	return false
}

func selectorMatchesLabels(selector, podLabels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if podLabels[key] != value {
			return false
		}
	}
	return true
}

func hasConfigMap(configMaps []corev1.ConfigMap, namespace, name string) bool {
	for _, cm := range configMaps {
		if cm.Namespace == namespace && cm.Name == name {
			return true
		}
	}
	return false
}

func sortedNames(values map[string]struct{}) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func healthFinding(ruleID string, summary string, evidence string, resource sdk.ObjectRef) sdk.Finding {
	return sdk.Finding{
		DetectorID: "health-objective", RuleID: ruleID, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: summary, Evidence: evidence,
		PrimaryResource: resource,
		Playbooks:       []string{"health-objective"},
		Fingerprint:     "health-objective/" + ruleID + "/" + resource.Namespace + "/" + resource.Name,
	}
}
