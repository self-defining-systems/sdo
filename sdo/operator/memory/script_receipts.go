package memory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// CaptureRepairReceipts copies only controller-owned measured receipt identity
// into protected recordings before reflection receives the capture directory.
// It adds no command bytes, benchmark verdicts, or resolved credential values.
func CaptureRepairReceipts(root, incidentID string, outcome *contracts.OutcomeRecord) error {
	if outcome == nil || outcome.GetIncidentId() != incidentID {
		return fmt.Errorf("script origin incident mismatch")
	}
	dir, err := IncidentCorpusDir(root, incidentID)
	if err != nil {
		return err
	}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		if _, err := os.Stat(filepath.Join(dir, phase+"-recordings.json")); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		for _, action := range outcome.GetRepairActions() {
			if !action.GetSuccess() || action.GetActionId() == "" || action.GetStartedAt() == nil || action.GetCompletedAt() == nil {
				continue
			}
			receipt := program.ScriptOriginReceipt{ScriptDigest: action.GetScriptDigest(), ID: action.GetActionId(), IncidentID: incidentID, Success: true, StartedAt: action.GetStartedAt().AsTime(), CompletedAt: action.GetCompletedAt().AsTime()}
			if receipt.CompletedAt.Before(receipt.StartedAt) {
				continue
			}
			for _, ref := range action.GetResources() {
				receipt.Resources = append(receipt.Resources, sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()})
			}
			raw, err := json.Marshal(receipt)
			if err != nil {
				return err
			}
			if err := AppendCapturedRecording(root, incidentID, phase, sdktest.PrimitiveCall{Method: "RepairReceipt", Arguments: json.RawMessage("[]"), Result: raw}); err != nil {
				return err
			}
		}
	}
	return nil
}
