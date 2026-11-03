package runtime

import (
	"sdo.dev/operator/sdk"
)

func stateFinding(fingerprint string) sdk.Finding {
	return sdk.Finding{
		DetectorID: fingerprint,
		RuleID:     fingerprint, Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: fingerprint},
		Fingerprint:     fingerprint,
	}
}
