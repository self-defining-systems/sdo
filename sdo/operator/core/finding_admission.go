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

// FindingAdmissionViolations evaluates detector on each snapshot and returns
// one line per snapshot whose findings the controller would refuse: the same
// defaults and ValidateFinding the controller applies to every evaluation. A
// detector that fails to evaluate is another check's concern and is skipped.
func FindingAdmissionViolations(detector sdk.Detector, snapshots []NamedSnapshot) []string {
	spec := detector.Spec()
	var violations []string
	for _, snapshot := range snapshots {
		findings, err := sdk.DetectObserved(context.Background(), detector, snapshot.View)
		if err != nil {
			continue
		}
		for _, finding := range findings {
			if finding.DetectorID == "" {
				finding.DetectorID = spec.ID
			}
			if len(finding.Playbooks) == 0 {
				finding.Playbooks = append([]string(nil), spec.Playbooks...)
			}
			if err := ValidateFinding(spec, finding); err != nil {
				violations = append(violations, fmt.Sprintf("on capture %s the controller refuses its finding (rule %q): %v; fill every field the controller requires (rule ID, severity, summary, evidence, and a primary resource with kind and name)", snapshot.Name, finding.RuleID, err))
				break
			}
		}
	}
	return violations
}
