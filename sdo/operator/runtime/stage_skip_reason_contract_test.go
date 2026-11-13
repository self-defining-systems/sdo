package runtime

import (
	"testing"

	"buf.build/go/protovalidate"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Every skip reason the response stage writes into a StageMitigation must be
// one the contract accepts: a reason outside the proto list makes the next
// IncidentRequest invalid, and the agent dispatch fails every time.
func TestStageSkipReasonsAreContractValues(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}
	for _, reason := range []string{
		stagePlanNotExecutable, stageIneffective, stageAlreadyAttempted, stageBuiltinRestoreManual, "script_unprovenanced",
	} {
		mitigation := &contracts.StageMitigation{PlaybookId: "p", Result: "skipped", SkipReason: reason}
		if err := validator.Validate(mitigation); err != nil {
			t.Errorf("skip reason %q is rejected by the contract: %v", reason, err)
		}
	}
}
