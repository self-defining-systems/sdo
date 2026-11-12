package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestRecordedMutationAcceptsGoDerivedAPIWritesNotGaps(t *testing.T) {
	for _, test := range []struct {
		id, kind string
		want     bool
	}{
		{"sdo-act-1", "kubectl-patch", false},
		{"sdo-api-abc", "api-patch", true},
		{"sdo-api-abc", "api-exec", true},
		{"sdo-api-abc", "api-helper-delete", true},
		{"sdo-api-gap-abc", "api-gap", false},
		{"sdo-api-abc", "kubectl-patch", false},
		{"agent-made", "api-patch", false},
		{"sdo-api-", "api-patch", false},
	} {
		if got := recordedMutation(&contracts.RepairActionReceipt{ActionId: test.id, Kind: test.kind}); got != test.want {
			t.Errorf("%s %s: %v want %v", test.id, test.kind, got, test.want)
		}
	}
}
