package runtime

import (
	"encoding/json"
	"strings"

	"sdo.dev/operator/sdk"
)

// SourceInventory evidence includes the sample time. Ignore only that clock
// field, preserving the accepted commit and every substantive evidence field.
func inventoryProgressEvidence(evidence string) string {
	prefix, payload, found := strings.Cut(evidence, "; ")
	var event map[string]json.RawMessage
	if !found || json.Unmarshal([]byte(payload), &event) != nil || event == nil {
		return evidence
	}
	delete(event, "observed_at")
	encoded, err := json.Marshal(event)
	if err != nil {
		return evidence
	}
	return prefix + "; " + string(encoded)
}

func failedInventoryAttempts(repairs []continuationRepair, remaining map[sdk.ObjectRef]bool) (map[string]string, bool) {
	attempts := map[string]string{}
	covered := map[sdk.ObjectRef]bool{}
	for _, repair := range repairs {
		receipt := repair.Receipt
		// An unknown target cannot prove failure coverage.
		if len(receipt.Resources) == 0 {
			return nil, false
		}
		relevant := false
		for _, ref := range receipt.Resources {
			if remaining[ref] {
				relevant = true
				covered[ref] = true
			}
		}
		if !relevant {
			continue
		}
		if repair.ActionID == "" || repair.RolledBack || receipt.Success || receipt.CompletedAt.IsZero() || receipt.CompletedAt.Before(receipt.StartedAt) {
			return nil, false
		}
		switch receipt.FailureReason {
		case "authorization_denied", "execution_failed":
		default:
			return nil, false
		}
		encoded, err := json.Marshal(repair)
		if err != nil {
			return nil, false
		}
		if _, duplicate := attempts[repair.ActionID]; duplicate {
			return nil, false
		}
		attempts[repair.ActionID] = string(encoded)
	}
	return attempts, len(covered) == len(remaining)
}
