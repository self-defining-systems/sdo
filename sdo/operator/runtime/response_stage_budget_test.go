package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
)

// An unrelated synthetic fault keeps this a real worker handoff after every
// covered write, rather than measuring the model-free completion branch.
type startupResidualDetector struct{}

func (startupResidualDetector) Spec() sdk.DetectorSpec {
	spec := incidentSpec(podWatch)
	spec.ID = "synthetic-residual"
	return spec
}

func (startupResidualDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{DetectorID: "synthetic-residual", RuleID: "uncovered", Status: sdk.FindingActive, Severity: "critical", Summary: "Synthetic uncovered fault", Evidence: "Synthetic evidence", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "uncovered"}}}, nil
}

