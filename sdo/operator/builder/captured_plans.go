package builder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

type CapturedPlanResult struct {
	ProgramID string
	CaseID    string
	Problem   string
}

const capturedPlanReport = "captured-plans.json"

func (workspace *Workspace) CapturedPlanResults() ([]CapturedPlanResult, error) {
	file, err := os.Open(filepath.Join(workspace.Directory, "generated", capturedPlanReport))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	const limit = 1 << 20
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("captured plan results exceed 1 MiB")
	}
	var results []CapturedPlanResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// Capture checks never block an accepted host's launch. A replay failure or
// timeout leaves the host with no unattended eligibility from that run.
func (workspace *Workspace) verifiedPlanIDs(ctx context.Context) []string {
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated", "captured_plans_test.go")); err != nil {
		return nil
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "corpus")); err != nil {
		return nil
	}
	if err := workspace.runGo(ctx, "test", "-count=1", "-timeout=30s", "-run", "^"+capturedPlansTest+"$", "./generated"); err != nil {
		slog.Warn("captured_plan_validation_unavailable", "error", err)
		return nil
	}
	results, err := workspace.CapturedPlanResults()
	if err != nil {
		slog.Warn("captured_plan_validation_unavailable", "error", err)
		return nil
	}
	var ids []string
	for _, result := range results {
		if result.CaseID != "" && result.Problem == "" {
			ids = append(ids, result.ProgramID)
		}
	}
	return ids
}
