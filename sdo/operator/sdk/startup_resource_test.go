package sdk_test

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResourceStartupAllowanceUsesTheFindingsWorkload(t *testing.T) {
	view := sdktest.Snapshot{ObservedStartupTimes: map[string]time.Duration{"StatefulSet/gitaly": 90 * time.Second}}
	got := sdk.ResourceStartupAllowance(view, sdk.ObjectRef{Kind: "StatefulSet", Name: "gitaly"})
	if got != 90*time.Second {
		t.Fatalf("allowance = %v, want 90s", got)
	}
	if got := sdk.ResourceStartupAllowance(view, sdk.ObjectRef{Kind: "Deployment", Name: "other"}); got != 0 {
		t.Fatalf("unknown workload allowance = %v, want 0", got)
	}
}
