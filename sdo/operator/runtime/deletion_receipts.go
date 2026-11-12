package runtime

import (
	"context"
	"encoding/json"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sdo.dev/operator/sdk"
)

// Worker command success is not proof that all declared resources were deleted.
// Check every claimed deletion without ever requesting a Secret object.
func (l *ResponderLoop) checkDeletionReceipts(ctx context.Context, receipts []RepairActionReceipt) []RepairActionReceipt {
	result := append([]RepairActionReceipt(nil), receipts...)
	for i := range result {
		receipt := &result[i]
		if receipt.Kind != "kubectl-delete" || !receipt.Success {
			continue
		}
		reason := ""
		if len(receipt.Resources) == 0 {
			reason = "delete_not_confirmed"
		}
		for j, ref := range receipt.Resources {
			for _, finding := range l.Request.Findings {
				if strings.EqualFold(ref.Kind, finding.PrimaryResource.Kind) && ref.Name == finding.PrimaryResource.Name && ref.Namespace == finding.PrimaryResource.Namespace {
					ref = finding.PrimaryResource
					break
				}
			}
			receipt.Resources[j] = ref
			if strings.EqualFold(ref.Kind, "Secret") || ref.APIVersion == "" || l.Executor.Client == nil {
				reason = "delete_not_confirmed"
				break
			}
			live, err := l.Executor.Client.Get(ctx, ref)
			if apierrors.IsNotFound(err) || (err == nil && live != nil && terminatingPod(ref, live.Object)) {
				continue
			}
			reason = "delete_not_confirmed"
			if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
				reason = "authorization_denied"
			}
			break
		}
		if reason != "" {
			receipt.Success = false
			receipt.FailureReason = reason
		}
	}
	return result
}

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
