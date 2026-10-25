package runtime

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// MitigationHealth is a single post-write observation. TargetedHealthy concerns
// only touched resources and their directly related findings; Healthy concerns
// every accepted health detector. Neither replaces controller closure evidence.
type MitigationHealth struct {
	Resources        []sdk.ObjectRef `json:"resources"`
	Findings         []sdk.Finding   `json:"findings"`
	TargetedFindings []sdk.Finding   `json:"targeted_findings"`
	TargetedHealthy  bool            `json:"targeted_healthy"`
	Healthy          bool            `json:"healthy"`
}

type mitigationHealthVerifier struct {
	health      PlanHealthVerifier
	plan        program.Plan
	observation *MitigationHealth
}

func (v *mitigationHealthVerifier) Verify(ctx context.Context) (bool, error) {
	if v.health.Provider == nil {
		return false, errors.New("mitigation snapshot provider is required")
	}
	snapshot, err := v.health.Provider.Snapshot(ctx)
	if err != nil {
		return false, fmt.Errorf("mitigation snapshot: %w", err)
	}
	observation := &MitigationHealth{Findings: []sdk.Finding{}, TargetedFindings: []sdk.Finding{}, Healthy: true, TargetedHealthy: true}
	v.observation = observation
	for _, action := range v.plan.Actions {
		ref, err := planActionRef(action)
		if err != nil {
			return false, err
		}
		observation.Resources = append(observation.Resources, ref)
	}
	var health []sdk.Detector
	for _, detector := range v.health.Detectors {
		if detector.Spec().Class == sdk.DetectorClassHealth {
			health = append(health, detector)
		}
	}
	if len(health) == 0 {
		return false, errors.New("health detectors are required")
	}
	sink := mitigationFindingSink{view: snapshot, observation: observation}
	err = (core.Runner{Detectors: health, Provider: core.StaticSnapshotProvider{SnapshotValue: snapshot}, Sink: sink}).RunOnce(ctx)
	if err != nil {
		observation.Healthy = false
		observation.TargetedHealthy = false
		return false, err
	}
	return observation.TargetedHealthy, nil
}

type mitigationFindingSink struct {
	view        sdk.DetectionContext
	observation *MitigationHealth
}

func (s mitigationFindingSink) Emit(_ context.Context, finding sdk.Finding) error {
	if finding.Status != sdk.FindingActive {
		return nil
	}
	finding.Fingerprint = FindingFingerprint(finding)
	s.observation.Findings = append(s.observation.Findings, finding)
	s.observation.Healthy = false
	if mitigationFindingRelated(s.view, finding, s.observation.Resources) {
		s.observation.TargetedFindings = append(s.observation.TargetedFindings, finding)
		s.observation.TargetedHealthy = false
	}
	return nil
}
func mitigationFindingRelated(view sdk.DetectionContext, finding sdk.Finding, refs []sdk.ObjectRef) bool {
	if finding.PrimaryResource.Kind == "" || finding.PrimaryResource.Name == "" {
		return true
	}
	subjects := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
	for _, ref := range finding.ParameterBindings {
		subjects = append(subjects, ref)
	}
	for _, subject := range subjects {
		for _, ref := range refs {
			if sameResource(subject, ref) || fastPathDirectlyRelated(view, subject, ref) || fastPathDirectlyRelated(view, ref, subject) {
				return true
			}
		}
	}
	return false
}

func scopedMitigationVerifier(verifier PlanVerifier, plan program.Plan) *mitigationHealthVerifier {
	switch health := verifier.(type) {
	case PlanHealthVerifier:
		return &mitigationHealthVerifier{health: health, plan: plan}
	case *PlanHealthVerifier:
		if health != nil {
			return &mitigationHealthVerifier{health: *health, plan: plan}
		}
	}
	return nil
}

func (l *ResponderLoop) observeMitigationHealth(result PlanApplicationResult, response *ToolResponse) {
	if result.MitigationHealth == nil {
		return
	}
	response.MitigationHealth = result.MitigationHealth
	response.TargetedHealthy = result.VerificationPassed
	response.Findings = append([]sdk.Finding(nil), result.MitigationHealth.Findings...)
	response.Healthy = result.MitigationHealth.Healthy
	l.findings = append([]sdk.Finding(nil), response.Findings...)
}
