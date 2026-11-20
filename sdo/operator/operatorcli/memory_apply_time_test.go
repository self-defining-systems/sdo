package operatorcli

import (
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

// A playbook author sees how long each action took when recorded, so a slow
// Mitigate is visible before it runs while users fail.
func TestMemoryTestLineShowsRecordedApplyTime(t *testing.T) {
	note := &contracts.MitigationReplayNote{
		CaseId: "synthetic", Status: contracts.MitigationReplayNote_STATUS_REPLAYED,
		Effects: []string{"simulated:recorded-external-transition", sdktest.ApplyTimeEffectPrefix + "0:219800", sdktest.ApplyTimeEffectPrefix + "bad"},
	}
	got := replayCaseLine(note)
	if want := "  case synthetic: mitigation replay replayed; action 0 apply 219.8s"; got != want {
		t.Fatalf("line = %q, want %q", got, want)
	}
	if line := replayCaseLine(&contracts.MitigationReplayNote{CaseId: "c"}); strings.Contains(line, "apply") {
		t.Fatalf("no recorded time must print none: %q", line)
	}
}
