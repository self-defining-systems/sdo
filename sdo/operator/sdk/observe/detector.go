// Package observe provides snapshot-only candidate symptom detectors.
// Candidates do not attribute causes or certify health recovery.
package observe

import (
	"context"
	"sdo.dev/operator/sdk"
)

type detector struct {
	spec sdk.DetectorSpec
	kind string
}

// NewDetector observes candidate symptoms for Service, Deployment, ConfigMap,
// or NetworkPolicy. The caller owns policy, playbooks and provenance in spec.
// NetworkPolicy candidates merely identify existing policies, not harmful ones.
// Unsupported kinds produce no findings. Findings bind the candidate as target.
func NewDetector(spec sdk.DetectorSpec, kind string) sdk.Detector {
	return detector{spec: spec, kind: kind}
}
func (d detector) Spec() sdk.DetectorSpec { return d.spec }
func (d detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	var refs []sdk.ObjectRef
	switch d.kind {
	case "Service":
		for _, s := range v.Services() {
			if sdk.ServiceExpectsEndpoints(s) && len(s.Spec.Selector) > 0 && v.ReadyEndpointCountForService(s.Namespace, s.Name) == 0 {
				refs = append(refs, sdk.ObjectRefFrom("Service", "v1", &s))
			}
		}
	case "Deployment":
		for _, s := range v.Deployments() {
			if s.Status.AvailableReplicas == 0 {
				refs = append(refs, sdk.ObjectRefFrom("Deployment", "apps/v1", &s))
			}
		}
	case "ConfigMap":
		for _, s := range v.Deployments() {
			for _, r := range sdk.ConfigMapReferencesForDeployment(s) {
				if r.Optional {
					continue
				}
				found := false
				for _, c := range v.ConfigMaps() {
					if c.Namespace == s.Namespace && c.Name == r.Name {
						found = true
					}
				}
				if !found {
					refs = append(refs, sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: s.Namespace, Name: r.Name})
				}
			}
			for _, r := range sdk.ConfigMapKeyReferencesForDeployment(s) {
				if r.Optional {
					continue
				}
				for _, c := range v.ConfigMaps() {
					if c.Namespace == s.Namespace && c.Name == r.Name {
						_, text := c.Data[r.Key]
						_, binary := c.BinaryData[r.Key]
						if !text && !binary {
							refs = append(refs, sdk.ObjectRefFrom("ConfigMap", "v1", &c))
						}
					}
				}
			}
		}
	case "NetworkPolicy":
		for _, s := range v.NetworkPolicies() {
			refs = append(refs, sdk.ObjectRefFrom("NetworkPolicy", "networking.k8s.io/v1", &s))
		}
	}
	var out []sdk.Finding
	for _, ref := range refs {
		out = append(out, sdk.Finding{DetectorID: d.spec.ID, RuleID: d.spec.ID, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "Candidate symptom; family is not independently attributed", Evidence: "Snapshot relation only; run bound diagnosis and independent health verification", PrimaryResource: ref, Playbooks: append([]string(nil), d.spec.Playbooks...), ParameterBindings: map[string]sdk.ObjectRef{"target": ref}})
	}
	return out, nil
}
