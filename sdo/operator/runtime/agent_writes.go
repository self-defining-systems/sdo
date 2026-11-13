package runtime

import (
	"log/slog"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// agentWriteReceipts is the responder loop's view of what the agent changed: the writes derived from the
// recording of its Kubernetes calls (memory.AgentWrites), never the command journal the agent can edit.
// Settle detection and the closure guard read it.
func agentWriteReceipts(repositoryMount, incidentID string, through time.Time) ([]RepairActionReceipt, bool, string, error) {
	rows, pending, revision, err := memory.AgentWrites(repositoryMount, incidentID, through)
	if err != nil {
		return nil, pending, revision, err
	}
	if len(rows) > 0 {
		// A raw write has no plan application to capture the first mutation boundary, so the recording's
		// before-states stand in for open@pre-apply (a no-op once one exists).
		if captureErr := memory.CapturePreApplyFromWrites(repositoryMount, incidentID, through); captureErr != nil {
			slog.Warn("pre-apply capture from the recording unavailable", "incident_id", incidentID, "error", captureErr)
		}
	}
	result := incidentResultState(&contractsv1alpha1.IncidentResult{RepairActions: rows})
	return result.RepairActions, pending, revision, nil
}

// externalReceiptsSource is the responder loop's write recording reader, or nil when agent.recording is off. It
// must be nil, never a function that returns no rows: readers treat a non-nil source as one that would have
// seen a write, so verify's and detect's notes claim "no write" only from a recording that exists.
func externalReceiptsSource(recording bool, repositoryMount, incidentID string, clock func() time.Time) func() ([]RepairActionReceipt, bool, string, error) {
	if !recording {
		return nil
	}
	return func() ([]RepairActionReceipt, bool, string, error) {
		return agentWriteReceipts(repositoryMount, incidentID, clock())
	}
}
