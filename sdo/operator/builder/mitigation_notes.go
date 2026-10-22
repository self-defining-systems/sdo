package builder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/sdktest"
)

// MitigationReplayResults reads the generated corpus gate's informational
// receipts. They are not proof of an external effect or inputs to live trust.
func (workspace *Workspace) MitigationReplayResults() ([]sdktest.MitigationReplayResult, error) {
	path := filepath.Join(workspace.Directory, "generated", "mitigation-replay.json")
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Size() > 1<<20 {
		return nil, fmt.Errorf("mitigation replay notes exceed 1 MiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results []sdktest.MitigationReplayResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("decode mitigation replay notes: %w", err)
	}
	for _, note := range results {
		if note.ProgramID == "" || note.CaseID == "" || note.Status != sdktest.MitigationReplayPassed && note.Status != sdktest.MitigationReplaySkippedUnreplayableEffect {
			return nil, fmt.Errorf("invalid mitigation replay note: %+v", note)
		}
	}
	return results, nil
}
