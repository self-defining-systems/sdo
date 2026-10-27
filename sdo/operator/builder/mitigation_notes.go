package builder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/sdktest"
)

// MitigationReplayResults reads the generated corpus replay's informational
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
		if note.ProgramID == "" || note.CaseID == "" || note.Status != sdktest.MitigationReplayPassed && note.Status != sdktest.MitigationReplaySkippedUnreplayableEffect && note.Status != sdktest.MitigationReplayObservedTransition {
			return nil, fmt.Errorf("invalid mitigation replay note: %+v", note)
		}
	}
	return results, nil
}

// CoverageGaps reads the generated corpus gate's non-blocking notes, such as a
// recorded incident the learned detectors do not reach.
func (workspace *Workspace) CoverageGaps() ([]string, error) {
	data, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", "coverage-gaps.json"))
	if os.IsNotExist(err) {
		return append([]string(nil), workspace.advisoryTestErrors...), nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("coverage gap notes exceed 1 MiB")
	}
	var gaps []string
	if err := json.Unmarshal(data, &gaps); err != nil {
		return nil, fmt.Errorf("decode coverage gap notes: %w", err)
	}
	return append(gaps, workspace.advisoryTestErrors...), nil
}
