package core

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk"
)

// NamedSnapshot is one captured state a detector is evaluated on.
type NamedSnapshot struct {
	Name string
	View sdk.DetectionContext
}

// AdmitFindings is the controller's admission of one evaluation's findings:
// it fills the detector ID, the declared playbooks and the fingerprint, then
// validates each finding with ValidateFinding. The first refused finding
// refuses the whole evaluation.
func AdmitFindings(spec sdk.DetectorSpec, findings []sdk.Finding) ([]sdk.Finding, error) {
	findings = append([]sdk.Finding(nil), findings...)
	for i := range findings {
		f := &findings[i]
		if f.DetectorID == "" {
			f.DetectorID = spec.ID
		}
		if len(f.Playbooks) == 0 {
			f.Playbooks = append([]string(nil), spec.Playbooks...)
		}
		f.Fingerprint = sdk.FindingFingerprint(*f)
		if err := ValidateFinding(spec, *f); err != nil {
			return nil, fmt.Errorf("finding (rule %q): %w", f.RuleID, err)
		}
	}
	return findings, nil
}

// FindingAdmissionViolations evaluates detector on each snapshot and returns
// one line per snapshot whose findings the controller would refuse
// (AdmitFindings, the controller's own admission). A detector that fails to
// evaluate is another check's concern and is skipped.
func FindingAdmissionViolations(detector sdk.Detector, snapshots []NamedSnapshot) []string {
	var violations []string
	for _, snapshot := range snapshots {
		findings, err := sdk.DetectObserved(context.Background(), detector, snapshot.View)
		if err != nil {
			continue
		}
		if _, err := AdmitFindings(detector.Spec(), findings); err != nil {
			violations = append(violations, fmt.Sprintf("on capture %s the controller refuses its %v; fill every field the controller requires (rule ID, severity, summary, evidence, and a primary resource with kind and name)", snapshot.Name, err))
		}
	}
	return violations
}
