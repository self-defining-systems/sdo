package runtime

import (
	"context"

	"strings"
	"testing"


	"sdo.dev/operator/sdk/program"
)

func TestCompositeRepairKeepsBoundSuccessWhileOtherFaultIsOpen(t *testing.T) {
	loop, _ := trustedPortLoop(t, true, false, false)
	loop.Executor.Verifier = PlanHealthVerifier{Detectors: loop.Detectors, Provider: loop.Provider}
	ctx := context.Background()
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": loop.Request.Findings[0].PrimaryResource}}
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings, FindingFingerprints: []string{FindingFingerprint(loop.Request.Findings[0])}})
	applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || applied.Healthy {
		t.Fatalf("expected other fault to remain: %+v", applied)
	}
	if !loop.PlanApplications()[0].Result.VerificationPassed {
		t.Fatal("unrelated fault invalidated bound repair")
	}
	for _, signal := range loop.FailureSignals() {
		if strings.HasPrefix(signal, "application_verification_failure:") || strings.HasPrefix(signal, "diagnosis_clear_detectors_firing:") {
			t.Fatalf("unrelated fault poisoned program trust: %s", signal)
		}
	}
}

