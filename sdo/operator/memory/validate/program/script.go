package program

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
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
 effects=append(effects,"replay status: "+r.Status,"open diagnosis: "+string(r.Faulted.Result),"healthy diagnosis: "+string(r.Healthy.Result),"post diagnosis: "+string(r.Post.Result))
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

// ValidateScriptOriginsWithoutCorpus preserves exact script provenance without
// requiring replay snapshots or labels. Recorded digests prove the bytes directly;
// legacy receipts without bytes need an explicit README/metadata receipt link.
func ValidateScriptOriginsWithoutCorpus(ctx context.Context, root, baseCommit string, outcome *contracts.OutcomeRecord, paths []string) error {
	directories := map[string]bool{}
	for _, path := range paths {
		kind, _, ok := layout.Classify(path)
		if ok && (kind == layout.KindPlaybookProgram || kind == layout.KindPlaybook || kind == layout.KindLibrary) {
			directories[filepath.Dir(path)] = true
		}
	}
	for dir := range directories {
		base := filepath.Join(root, dir)
		scripts, err := scriptTexts(base)
		if err != nil {
			return err
		}
		if len(scripts) == 0 {
			continue
		}
		var origins ScriptOriginMetadata
		if strings.HasPrefix(dir, layout.PlaybooksDir+"/") {
			data, err := os.ReadFile(filepath.Join(base, "README.md"))
			if err != nil {
				return err
			}
			if err := parseProgramFrontmatter(data, &origins); err != nil {
				return err
			}
		} else if data, err := os.ReadFile(filepath.Join(base, "metadata.yaml")); err == nil {
			if err := yaml.Unmarshal(data, &origins); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		for _, script := range scripts {
			digest := sdkprogram.ScriptDigest(script)
			origin := origins.ForScript(script)
			linked := origin.IncidentID == outcome.GetIncidentId() && ScriptReceiptMatches(script, origin.LegacyOrigins, origin.IncidentID, outcome, origin.ReceiptID)
			if !linked && baseCommit != "" && origin.IncidentID != "" {
				resolved, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--verify", baseCommit+"^{commit}").Output()
				if err != nil {
					return fmt.Errorf("resolve script receipt base: %w", err)
				}
				accepted, err := memory.NewJSONStore(root).GetAt(ctx, strings.TrimSpace(string(resolved)), origin.IncidentID)
				linked = err == nil && ScriptReceiptMatches(script, origin.LegacyOrigins, origin.IncidentID, accepted.Record, origin.ReceiptID)
			}

			if !linked {
				return fmt.Errorf("RunScript digest %s requires its original successful accepted receipt; link script_receipts[<digest>] = {incident_id, receipt_id} in README front matter (or library metadata.yaml) to the accepted original receipt; retain originating_incident and prior script origins", digest)
			}
		}
	}
	return nil
}

// ScriptReceiptMatches binds exact executed bytes, or an explicit legacy receipt
// identity, to the independently accepted incident outcome.
func ScriptReceiptMatches(script string, origins map[string]string, incident string, outcome *contracts.OutcomeRecord, receiptIDs ...string) bool {
	digest := sdkprogram.ScriptDigest(script)
	selectedID := origins[digest]
	if len(receiptIDs) > 0 && receiptIDs[0] != "" {
		selectedID = receiptIDs[0]
	}
	for _, receipt := range outcome.GetRepairActions() {
		if selectedID != "" && receipt.GetActionId() != selectedID {
			continue
		}
		if !receipt.GetSuccess() || receipt.GetStartedAt() == nil || receipt.GetCompletedAt() == nil || receipt.GetCompletedAt().AsTime().Before(receipt.GetStartedAt().AsTime()) {
			continue
		}
		if receipt.GetScriptDigest() != "" {
			if receipt.GetScriptDigest() == digest {
				return true
			}
		} else if incident == outcome.GetIncidentId() && origins[digest] != "" && origins[digest] == receipt.GetActionId() {
			return true
		}
	}
	return false
}

// ScriptReceiptOrigin identifies one original incident receipt. Several script
// revisions can retain different origins without rewriting the playbook's birth.
type ScriptReceiptOrigin struct {
	IncidentID    string            `yaml:"incident_id"`
	ReceiptID     string            `yaml:"receipt_id"`
	LegacyOrigins map[string]string `yaml:"-"`
}

// ScriptOriginMetadata is the shared README/library provenance shape. Legacy
// digest-to-ID maps remain readable, while script_receipts preserves per-script
// incident identity across later playbook and helper revisions.
type ScriptOriginMetadata struct {
	Incident       string                         `yaml:"originating_incident"`
	ScriptOrigins  map[string]string              `yaml:"script_origins"`
	ScriptReceipts map[string]ScriptReceiptOrigin `yaml:"script_receipts"`
}

func (m ScriptOriginMetadata) ForScript(script string) ScriptReceiptOrigin {
	digest := sdkprogram.ScriptDigest(script)
	origin, ok := m.ScriptReceipts[digest]
	if ok && (origin.IncidentID == "" || origin.ReceiptID == "") {
		return ScriptReceiptOrigin{}
	}
	if !ok {
		origin = ScriptReceiptOrigin{IncidentID: m.Incident, ReceiptID: m.ScriptOrigins[digest]}
	}
	origin.LegacyOrigins = map[string]string{digest: origin.ReceiptID}
	return origin
}
