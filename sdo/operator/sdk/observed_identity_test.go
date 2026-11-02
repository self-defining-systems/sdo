package sdk_test

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type identityDetector struct{ id string }

func (d identityDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "observed-health"} }
func (d identityDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{DetectorID: d.id, RuleID: "unhealthy"}}, nil
}

func TestDetectObservedSuppliesMissingDetectorIdentity(t *testing.T) {
	for _, id := range []string{"", "authored-identity"} {
		findings, err := sdk.DetectObserved(context.Background(), identityDetector{id}, sdktest.Snapshot{})
		if err != nil {
			t.Fatal(err)
		}
		want := id
		if want == "" {
			want = "observed-health"
		}
		if len(findings) != 1 || findings[0].DetectorID != want {
			t.Fatalf("detector identity=%v, want %s", findings, want)
		}
	}
}

type retainedIdentityDetector struct{ findings []sdk.Finding }

func (d retainedIdentityDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "observed-health"}
}
func (d retainedIdentityDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.findings, nil
}

func TestDetectObservedRetainsDetectorOwnedIdentity(t *testing.T) {
	d := retainedIdentityDetector{findings: []sdk.Finding{{RuleID: "unhealthy"}}}
	result, err := sdk.DetectObserved(context.Background(), d, sdktest.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if result[0].DetectorID != "observed-health" || d.findings[0].DetectorID != "" {
		t.Fatal("normalization mutated detector-owned evidence")
	}
	result[0].RuleID = "changed"
	if d.findings[0].RuleID != "unhealthy" {
		t.Fatal("observation result aliases detector-owned evidence")
	}
}
