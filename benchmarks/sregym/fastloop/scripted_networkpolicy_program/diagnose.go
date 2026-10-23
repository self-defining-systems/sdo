package networkpolicy_service_isolation

import (
	"context"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "networkpolicy-service-isolation", Params: []program.ParamSpec{{Name: "policy", Kind: "NetworkPolicy", Required: true}}}
}

// Diagnose checks the verified class: a source-absent policy completely
// isolates a pod serving an unchanged accepted Service. Partial rules need
// judgement; this program does not pretend to implement a network-policy engine.
func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	ref := bindings.Objects["policy"]
	ref.APIVersion = "networking.k8s.io/v1"
	for _, policy := range view.NetworkPolicies() {
		if policy.Namespace != ref.Namespace || policy.Name != ref.Name {
			continue
		}
		diff, err := view.DiffAgainstSource(ref)
		if err != nil {
			return program.Verdict{Result: program.Unknown}, nil
		}
		if !diff.SourceAbsent {
			return program.Verdict{Result: program.NoMatch}, nil
		}
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			return program.Verdict{Result: program.Unknown}, nil
		}
		for _, service := range view.Services() {
			if service.Namespace != ref.Namespace || len(service.Spec.Ports) == 0 {
				continue
			}
			serviceRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
			declared, err := view.DiffAgainstSource(serviceRef)
			if err != nil || declared.SourceAbsent || declared.Changed {
				continue
			}
			for _, pod := range view.PodsForService(service.Namespace, service.Name) {
				if !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}
				for _, direction := range []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress} {
					if !isolates(policy, direction) || hasRules(policy, direction) {
						continue
					}
					// Allow rules are additive across selecting policies. Conservatively
					// abstain if any selecting policy has rules in this direction.
					allowed := false
					for _, other := range view.NetworkPolicies() {
						if other.Namespace != pod.Namespace || !isolates(other, direction) {
							continue
						}
						otherSelector, err := metav1.LabelSelectorAsSelector(&other.Spec.PodSelector)
						if err != nil {
							allowed = true
							break
						}
						if otherSelector.Matches(labels.Set(pod.Labels)) && hasRules(other, direction) {
							allowed = true
							break
						}
					}
					if !allowed {
						return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "source Service pod isolated", Observed: string(direction) + " has no selecting allow rules", Expected: "connectivity on declared Service ports"}, {Resource: serviceRef, Fact: "accepted Service", Observed: "declared ports select an isolated pod", Expected: "selected pods remain reachable"}}}, nil
					}
				}
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func isolates(policy networkingv1.NetworkPolicy, direction networkingv1.PolicyType) bool {
	for _, kind := range policy.Spec.PolicyTypes {
		if kind == direction {
			return true
		}
	}
	if len(policy.Spec.PolicyTypes) > 0 {
		return false
	}
	return direction == networkingv1.PolicyTypeIngress || direction == networkingv1.PolicyTypeEgress && len(policy.Spec.Egress) > 0
}
func hasRules(policy networkingv1.NetworkPolicy, direction networkingv1.PolicyType) bool {
	if direction == networkingv1.PolicyTypeIngress {
		return len(policy.Spec.Ingress) > 0
	}
	return len(policy.Spec.Egress) > 0
}
