package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The shared check refuses updated learning for a closure with no verified
// repaired cause; a verified healthy closure always passes that fact.
func TestReflectionRepairVerifiedFact(t *testing.T) {
	for class, want := range map[string]bool{"success": true, "cleared_without_action": false, "failed": false} {
		if got := reflectionRepairVerified(&contracts.OutcomeRecord{Classification: class}); got != want {
			t.Errorf("%s: reflectionRepairVerified = %v, want %v", class, got, want)
		}
	}
}
