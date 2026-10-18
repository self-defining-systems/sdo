package sdktest

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
)

const (
	externalNameCheckNamespace = "sdo-externalname-check"
	externalNameCheckTarget    = "sdo-externalname-target.invalid"
	externalNameCheckLabel     = "sdo.dev/externalname-check"
	externalNameCheckAddress   = "10.255.0.10"
)

type externalNameShape struct {
	description string
	selector    bool
}

var externalNameShapes = []externalNameShape{
	{description: "without a selector", selector: false},
	{description: "with a selector left over from a type change", selector: true},
}

// ExternalNameEndpointViolations checks that a detector never reports an
// ExternalName Service because it lacks ready endpoints or backing pods.
//
// For each Service name it compares two snapshots that differ only in the
// Service's backends: the realistic one, where the ExternalName Service has no
// Endpoints, EndpointSlices, or pods, and a counterfactual one that supplies
// ready backends. A finding about the Service that appears only in the
// realistic snapshot can only come from an endpoint or pod readiness check,
// which Kubernetes never satisfies for a DNS alias. Findings that do not depend
// on backends, such as a Service type check, are allowed. Pass the
// application's own Service names so inventory-scoped detectors are exercised.
func ExternalNameEndpointViolations(detector sdk.Detector, serviceNames []string) []string {
	spec := detector.Spec()
	violations := make([]string, 0)
	for _, name := range distinctSorted(serviceNames) {
		for _, shape := range externalNameShapes {
			without, withoutErr := detector.Detect(context.Background(), externalNameSnapshot(name, shape, false))
			with, withErr := detector.Detect(context.Background(), externalNameSnapshot(name, shape, true))
			subject := fmt.Sprintf("ExternalName Service %s/%s %s", externalNameCheckNamespace, name, shape.description)
			if withoutErr != nil && withErr == nil {
				violations = append(violations, fmt.Sprintf(
					"detector %q failed on %s only when it had no ready endpoints or pods: %v; %s",
					spec.ID, subject, withoutErr, externalNameRemedy,
				))
				continue
			}
			if withoutErr != nil || withErr != nil {
				continue
			}
			backed := findingKeysFor(with, name)
			for _, key := range sortedKeys(findingKeysFor(without, name)) {
				if _, alsoBacked := backed[key]; alsoBacked {
					continue
				}
				violations = append(violations, fmt.Sprintf(
					"detector %q reported %s (%s) only when it had no ready endpoints or pods; %s",
					spec.ID, subject, key, externalNameRemedy,
				))
			}
		}
	}
	return violations
}

const externalNameRemedy = "ExternalName Services are DNS aliases that Kubernetes never backs with " +
	"Endpoints, EndpointSlices, or selected pods, so skip them with sdk.ServiceExpectsEndpoints(service) " +
	"before any endpoint or pod readiness check"

func externalNameSnapshot(name string, shape externalNameShape, backed bool) Snapshot {
	service := corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: externalNameCheckNamespace},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: externalNameCheckTarget,
		},
	}
	if shape.selector {
		service.Spec.Selector = map[string]string{externalNameCheckLabel: name}
	}
	snapshot := Snapshot{NamespaceName: externalNameCheckNamespace, ServiceList: []corev1.Service{service}}
	if !backed {
		return snapshot
	}
	ready := true
	snapshot.EndpointList = []corev1.Endpoints{{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: externalNameCheckNamespace},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: externalNameCheckAddress}},
		}},
	}}
	snapshot.EndpointSliceList = []discoveryv1.EndpointSlice{{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-sdo-check",
			Namespace: externalNameCheckNamespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: name},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{externalNameCheckAddress},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
	}}
	if shape.selector {
		snapshot.PodList = []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name + "-sdo-check",
				Namespace: externalNameCheckNamespace,
				Labels:    map[string]string{externalNameCheckLabel: name},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      externalNameCheckAddress,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}}
	}
	return snapshot
}

// findingKeysFor returns stable identities of findings that concern the named
// Service, either through a structured object reference or by naming it.
func findingKeysFor(findings []sdk.Finding, name string) map[string]struct{} {
	mention := regexp.MustCompile(`(^|[^A-Za-z0-9_.-])` + regexp.QuoteMeta(name) + `($|[^A-Za-z0-9_-])`)
	keys := make(map[string]struct{})
	for _, finding := range findings {
		if !referencesService(finding, name) && !mention.MatchString(finding.Summary) &&
			!mention.MatchString(finding.Evidence) {
			continue
		}
		rule := finding.RuleID
		if rule == "" {
			rule = finding.Summary
		}
		keys[fmt.Sprintf("rule %q on %s %q", rule, finding.PrimaryResource.Kind, finding.PrimaryResource.Name)] = struct{}{}
	}
	return keys
}

func referencesService(finding sdk.Finding, name string) bool {
	matches := func(ref sdk.ObjectRef) bool { return ref.Kind == "Service" && ref.Name == name }
	if matches(finding.PrimaryResource) {
		return true
	}
	for _, ref := range finding.RelatedResources {
		if matches(ref) {
			return true
		}
	}
	for _, ref := range finding.ParameterBindings {
		if matches(ref) {
			return true
		}
	}
	return false
}

func distinctSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
