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

// trafficLatencyNotes is the generated traffic test's file of health-probe
// scenarios that keep the default latency bound.
const trafficLatencyNotes = "traffic-latency-gaps.json"

// CoverageGaps reads the generated replay's non-blocking notes, such as a
// recorded incident the learned detectors do not reach.
func (workspace *Workspace) CoverageGaps() ([]string, error) {
	gaps := append([]string(nil), workspace.advisorySetupErrors...)
	recorded, err := workspace.generatedNotes("coverage-gaps.json")
	if err != nil {
		return gaps, err
	}
	gaps = append(gaps, recorded...)
	unverified, err := workspace.generatedNotes("incident-plan-gaps.json")
	if err != nil {
		return gaps, err
	}
	gaps = append(gaps, unverified...)
	return append(gaps, workspace.advisoryTestErrors...), nil
}

// TrafficLatencyGaps reads the generated traffic test's non-blocking notes:
// the health-probe scenarios that keep the default latency bound.
func (workspace *Workspace) TrafficLatencyGaps() ([]string, error) {
	return workspace.generatedNotes(trafficLatencyNotes)
}

func (workspace *Workspace) generatedNotes(name string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%s notes exceed 1 MiB", name)
	}
	var recorded []string
	if err := json.Unmarshal(data, &recorded); err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return recorded, nil
}

// OwnIncidentResults reads the generated own-incident replay's verdicts: one
// per playbook new in the change, written whether or not the replay failed.
func (workspace *Workspace) OwnIncidentResults() ([]sdktest.OwnIncidentResult, error) {
	data, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", "own-incident-replay.json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("own-incident replay verdicts exceed 1 MiB")
	}
	var results []sdktest.OwnIncidentResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("decode own-incident replay verdicts: %w", err)
	}
	return results, nil
}
