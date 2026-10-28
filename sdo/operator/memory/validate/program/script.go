package program

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	sdkprogram "sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
)

// The generated replay reports opaque scripts honestly: recorded diagnosis and
// origin linkage are accepted without claiming simulated external effects.
const scriptReplayNoteHelper = `
func ReplayNote(programID,caseID string,r replay.Result) sdktest.MitigationReplayResult {
 status:=sdktest.MitigationReplaySkippedUnreplayableEffect
 if r.Status=="replayed" {status=sdktest.MitigationReplayPassed;if r.ReplayBasis=="observed-transition" {status=sdktest.MitigationReplayObservedTransition}}
 effects:=append([]string(nil),r.Effects...)
 effects=append(effects,"replay status: "+r.Status,"open diagnosis: "+string(r.Faulted.Result),"post diagnosis: "+string(r.Post.Result))
 effects=append(effects,r.Errors...)
 return sdktest.MitigationReplayResult{ProgramID:programID,CaseID:caseID,Status:status,Effects:effects}
}
`

// ValidateScriptOrigins binds newly authored origin labels to the accepted
// controller outcome: any successful timed receipt, with equal script bytes
// when the receipt recorded them. Static inspection separately checks the
// authored script digest.
func ValidateScriptOrigins(root string, outcome *contracts.OutcomeRecord, paths []string) error {
	cases := map[string]bool{}
	for _, path := range paths {
		if relative, ok := strings.CutPrefix(path, ".sdo/corpus/"); ok {
			id, _, _ := strings.Cut(relative, "/")
			cases[id] = true
		}
	}
	receipts := map[string]*contracts.RepairActionReceipt{}
	for _, receipt := range outcome.GetRepairActions() {
		receipts[receipt.GetActionId()] = receipt
	}
	for id := range cases {
		dir := filepath.Join(root, ".sdo", "corpus", id)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		corpus, err := sdktest.LoadCorpusMetadata(dir)
		if err != nil {
			return err
		}
		for digest, receiptID := range corpus.ScriptOrigins {
			receipt := receipts[receiptID]
			if receipt == nil || (receipt.GetScriptDigest() != "" && receipt.GetScriptDigest() != digest) || !receipt.GetSuccess() || receipt.GetStartedAt() == nil || receipt.GetCompletedAt() == nil || corpus.Provenance.IncidentID != outcome.GetIncidentId() {
				return fmt.Errorf("RunScript origin %q must reference a successful accepted incident receipt", receiptID)
			}
			matched := false
			for _, call := range corpus.Recordings {
				if call.Method != "RepairReceipt" || call.Error != "" {
					continue
				}
				var recorded sdkprogram.ScriptOriginReceipt
				if json.Unmarshal(call.Result, &recorded) == nil && recorded.ID == receiptID && recorded.ScriptDigest == receipt.GetScriptDigest() && recorded.IncidentID == outcome.GetIncidentId() && recorded.Success && recorded.StartedAt.Equal(receipt.GetStartedAt().AsTime()) && recorded.CompletedAt.Equal(receipt.GetCompletedAt().AsTime()) {
					matched = true
				}
			}
			if !matched {
				return fmt.Errorf("RunScript origin %q differs from the original accepted receipt", receiptID)
			}
		}
	}
	return nil
}
