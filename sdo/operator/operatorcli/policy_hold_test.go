package operatorcli

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
)

// A refresh refused by the policy continues the controller start (the hold is
// reported); any other refresh failure still stops it.
func TestHoldRefreshOnPolicyContinuesOnlyForThePolicyHold(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hold := lifecycle.NewPolicyRefusedError("c", []operability.Check{{ID: "O6.allowlist", Status: operability.StatusFail, Cause: operability.CauseAllowlist, Detail: "off the allowlist"}})
	wrapped := fmt.Errorf("health unverified: refresh judge failed within 1m: %w", fmt.Errorf("verify accepted source: %w", hold))
	if err := holdRefreshOnPolicy(wrapped, log); err != nil {
		t.Fatalf("err = %v, want the policy hold not to stop the controller", err)
	}
	other := errors.New("judge unavailable")
	if err := holdRefreshOnPolicy(other, log); !errors.Is(err, other) {
		t.Fatalf("err = %v, want the other failure returned", err)
	}
	if err := holdRefreshOnPolicy(nil, log); err != nil {
		t.Fatal(err)
	}
}

// The operate Job ends on the policy hold with the typed report and owner
// instead of an anonymous error.
func TestPolicyHoldIsTheTerminalReportOfTheOperateJob(t *testing.T) {
	hold := lifecycle.NewPolicyRefusedError("c", []operability.Check{{ID: "O6.allowlist", Status: operability.StatusFail, Cause: operability.CauseAllowlist, Detail: "off the allowlist"}})
	report, decision, ok := terminalOperability(fmt.Errorf("deployment attempt 1: %w", hold))
	if !ok || decision.Owner != operability.OwnerOperator || len(report.Items) == 0 {
		t.Fatalf("ok=%v decision=%+v, want the operator-owned hold", ok, decision)
	}
}
