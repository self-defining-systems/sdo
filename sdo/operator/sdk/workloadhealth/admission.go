package workloadhealth

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk"
)

// RulePodAdmissionRejected means a server-side dry-run Pod create failed.
const RulePodAdmissionRejected = "pod-admission-rejected"

type admissionDetector struct{ spec sdk.DetectorSpec }

// NewPodAdmissionDetector checks admission independently of existing Pod
// readiness. Its input is a frozen dry-run observation, so replay needs no API.
func NewPodAdmissionDetector(spec sdk.DetectorSpec) sdk.Detector {
	return admissionDetector{spec: spec}
}
func (d admissionDetector) Spec() sdk.DetectorSpec     { return d.spec }
func (d admissionDetector) ConsumesPodAdmission() bool { return true }
func (d admissionDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	r, ok := ctx.PodAdmission()
	if !ok || r.Namespace == "" || r.Namespace != ctx.Namespace() {
		return nil, fmt.Errorf("pod admission observation unavailable for namespace %q", ctx.Namespace())
	}
	if r.Error != "" {
		return nil, fmt.Errorf("pod admission observation: %s", r.Error)
	}
	if r.Admitted && r.Rejection == "" {
		return nil, nil
	}
	if r.Admitted || r.Rejection == "" {
		return nil, fmt.Errorf("invalid pod admission observation for namespace %q", r.Namespace)
	}
	return []sdk.Finding{{
		DetectorID: d.spec.ID, RuleID: RulePodAdmissionRejected, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary:         fmt.Sprintf("Namespace %s cannot admit a new Pod", r.Namespace),
		Evidence:        fmt.Sprintf("Server-side dry-run Pod create (source Pod %q) failed: %s", r.SourcePod, r.Rejection),
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: r.Namespace},
		Playbooks:       append([]string(nil), d.spec.Playbooks...),
	}}, nil
}
