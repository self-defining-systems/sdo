package sdktest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/sdk"
)

type serviceRule func(snapshot sdk.DetectionContext, service corev1.Service) (string, bool)

type serviceRuleDetector struct {
	class sdk.DetectorClass
	rules map[string]serviceRule
	names map[string]struct{}
}

func (d serviceRuleDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "fixture", Class: d.class, Interval: time.Minute}
}

func (d serviceRuleDetector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := make([]sdk.Finding, 0)
	for _, service := range snapshot.Services() {
		if d.names != nil {
			if _, selected := d.names[service.Name]; !selected {
				continue
			}
		}
		for ruleID, rule := range d.rules {
			if evidence, fires := rule(snapshot, service); fires {
				findings = append(findings, sdk.Finding{
					RuleID: ruleID, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
					Summary: evidence, Evidence: evidence,
					PrimaryResource: sdk.ObjectRefFrom("Service", "v1", &service),
				})
			}
		}
	}
	return findings, nil
}

func zeroReadyEndpoints(snapshot sdk.DetectionContext, service corev1.Service) (string, bool) {
	if snapshot.ReadyEndpointCountForService(service.Namespace, service.Name) != 0 {
		return "", false
	}
	return fmt.Sprintf("service %s/%s has zero ready endpoints", service.Namespace, service.Name), true
}

func zeroReadyEndpointSlices(snapshot sdk.DetectionContext, service corev1.Service) (string, bool) {
	for _, slice := range snapshot.EndpointSlices() {
		if slice.Namespace != service.Namespace || slice.Labels["kubernetes.io/service-name"] != service.Name {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				return "", false
			}
		}
	}
	return "service has no ready EndpointSlice endpoints", true
}

func noSelectedPods(snapshot sdk.DetectionContext, service corev1.Service) (string, bool) {
	if len(service.Spec.Selector) == 0 {
		return "", false
	}
	for _, pod := range snapshot.PodsForService(service.Namespace, service.Name) {
		if pod.Status.Phase == corev1.PodRunning {
			return "", false
		}
	}
	return "service selects no running pods", true
}

func guarded(rule serviceRule) serviceRule {
	return func(snapshot sdk.DetectionContext, service corev1.Service) (string, bool) {
		if !sdk.ServiceExpectsEndpoints(service) {
			return "", false
		}
		return rule(snapshot, service)
	}
}

func externalNameIsDrift(_ sdk.DetectionContext, service corev1.Service) (string, bool) {
	return "service type drifted to ExternalName", service.Spec.Type == corev1.ServiceTypeExternalName
}

func TestExternalNameEndpointViolationsCatchesUnguardedEndpointChecks(t *testing.T) {
	detector := serviceRuleDetector{class: sdk.DetectorClassHealth, rules: map[string]serviceRule{
		"service-without-ready-endpoints": zeroReadyEndpoints,
	}}

	violations := ExternalNameEndpointViolations(detector, []string{"jaeger"})

	if len(violations) == 0 {
		t.Fatal("expected an unguarded ready-endpoint check to violate the ExternalName exemption")
	}
	joined := strings.Join(violations, "\n")
	for _, want := range []string{"jaeger", "service-without-ready-endpoints", "ExternalName"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("violation does not name %q: %s", want, joined)
		}
	}
}

func TestExternalNameEndpointViolationsCatchesInventoryScopedChecks(t *testing.T) {
	// Judge-authored detectors usually restrict checks to a source inventory,
	// so the check must use the application's own Service names.
	detector := serviceRuleDetector{
		class: sdk.DetectorClassHealth,
		rules: map[string]serviceRule{"service-without-ready-endpoints": zeroReadyEndpoints},
		names: map[string]struct{}{"frontend": {}, "jaeger": {}},
	}

	if got := ExternalNameEndpointViolations(detector, []string{"sdo-externalname-probe"}); len(got) != 0 {
		t.Fatalf("inventory-scoped detector ignores unknown probe names, got %v", got)
	}
	if got := ExternalNameEndpointViolations(detector, []string{"frontend", "jaeger"}); len(got) == 0 {
		t.Fatal("expected source Service names switched to ExternalName to expose the missing guard")
	}
}

func TestExternalNameEndpointViolationsCatchesSliceAndPodBackedChecks(t *testing.T) {
	for ruleID, rule := range map[string]serviceRule{
		"service-without-ready-slices": zeroReadyEndpointSlices,
		"service-without-running-pods": noSelectedPods,
	} {
		detector := serviceRuleDetector{class: sdk.DetectorClassHealth, rules: map[string]serviceRule{ruleID: rule}}
		if got := ExternalNameEndpointViolations(detector, []string{"jaeger"}); len(got) == 0 {
			t.Fatalf("%s: expected an unguarded backend check to violate the ExternalName exemption", ruleID)
		}
	}
}

func TestExternalNameEndpointViolationsAcceptsGuardedAndTypeChecks(t *testing.T) {
	detector := serviceRuleDetector{class: sdk.DetectorClassHealth, rules: map[string]serviceRule{
		"service-without-ready-endpoints": guarded(zeroReadyEndpoints),
		"service-without-ready-slices":    guarded(zeroReadyEndpointSlices),
		"service-without-running-pods":    guarded(noSelectedPods),
		// Reporting the Service type itself does not depend on endpoints, so
		// the exemption does not forbid it.
		"service-type-drift": externalNameIsDrift,
	}}

	if got := ExternalNameEndpointViolations(detector, []string{"jaeger", "frontend"}); len(got) != 0 {
		t.Fatalf("guarded detector must satisfy the ExternalName exemption, got %v", got)
	}
}

type erroringDetector struct{}

func (erroringDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "erroring", Class: sdk.DetectorClassHealth}
}

func (erroringDetector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	for _, service := range snapshot.Services() {
		if snapshot.ReadyEndpointCountForService(service.Namespace, service.Name) == 0 {
			return nil, fmt.Errorf("service %s has no endpoints", service.Name)
		}
	}
	return nil, nil
}

func TestExternalNameEndpointViolationsReportsErrorsCausedByMissingEndpoints(t *testing.T) {
	if got := ExternalNameEndpointViolations(erroringDetector{}, []string{"jaeger"}); len(got) == 0 {
		t.Fatal("expected a detector that errors only without endpoints to violate the exemption")
	}
}
