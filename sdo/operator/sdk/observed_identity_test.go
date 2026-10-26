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
