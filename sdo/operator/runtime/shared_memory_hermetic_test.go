package runtime

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"time"

)

type sharedMemoryResponder struct {
	health *chaosDetector
	lane   string
	start  time.Time
}

func (d sharedMemoryResponder) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	if err := os.WriteFile(filepath.Join(r.RepositoryWorktree, d.lane+".txt"), []byte("synthetic accepted source repair\n"), 0644); err != nil {
		return IncidentResult{}, err
	}
	d.health.active.set(false)
	result := completedResult(r.IncidentID)
	result.ResponderSessionID = "session-" + d.lane
	result.Timing = TimingMetrics{StartedAt: d.start.Add(time.Second), CompletedAt: d.start.Add(2 * time.Second)}
	// The repair is the source file in the incident workspace, published by the
	// broker. No cluster receipt is recorded, so README edits stay judgement.
	return result, nil
}

// The cluster edge is synthetic; controller detection, workspaces, registered
// reflection tasks, independent compilation, native ledgers and Git are real.
var conflictBlock = regexp.MustCompile(`(?s)<<<<<<< [^\n]*\n.*?\n>>>>>>> [^\n]*\n`)

