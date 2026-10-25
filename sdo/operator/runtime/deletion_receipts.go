package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"time"

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
			_, err := l.Executor.Client.Get(ctx, ref)
			if apierrors.IsNotFound(err) {
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

// A continuation gets one opportunity to use a different mitigation. Stop only
// when the exact broken set persists and every remaining resource has a typed
// authorization denial. Successful peer repairs and other failures do not stop it.
func authorizationDeniedNoProgress(previous string, verification ToolResponse, repairs []continuationRepair) bool {
	var prior responderFeedback
	if previous == "" || json.Unmarshal([]byte(previous), &prior) != nil || len(prior.StillBroken) == 0 || len(prior.StillBroken) != len(verification.Findings) {
		return false
	}
	before := map[string]bool{}
	for _, finding := range prior.StillBroken {
		before[finding.Fingerprint] = true
	}
	denied := map[sdk.ObjectRef]bool{}
	latest := map[sdk.ObjectRef]time.Time{}
	for _, repair := range repairs {
		for _, ref := range repair.Receipt.Resources {
			if stamp, found := latest[ref]; found && repair.Receipt.CompletedAt.Before(stamp) {
				continue
			}
			latest[ref] = repair.Receipt.CompletedAt
			denied[ref] = !repair.Receipt.Success && repair.Receipt.FailureReason == "authorization_denied"
		}
	}
	for _, finding := range verification.Findings {
		if !before[FindingFingerprint(finding)] || !denied[finding.PrimaryResource] {
			return false
		}
	}
	return len(verification.Findings) > 0
}
