package runtime

import (
	"context"

	"sdo.dev/operator/sdk"
)

type coverageThreeFaultDetector struct{ fastloopDetector }

func (coverageThreeFaultDetector) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.Playbooks = []string{"service-targetport-mismatch", "image-pull-failure"}
	return spec
}

func (coverageThreeFaultDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (fastloopDetector{}).Detect(ctx, view)
	for i := range findings {
		switch findings[i].RuleID {
		case "wrong-port":
			findings[i].Playbooks = []string{"service-targetport-mismatch"}
		case "image-pull":
			findings[i].Playbooks = []string{"image-pull-failure"}
		}
	}
	for _, service := range view.Services() {
		if service.Spec.Ports[0].TargetPort.IntVal == 7777 {
			findings = append(findings, sdk.Finding{DetectorID: "availability", RuleID: "unlearned-port", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Service", "v1", &service), Summary: "unlearned fault", Evidence: "third service has wrong port"})
		}
	}
	return findings, err
}

