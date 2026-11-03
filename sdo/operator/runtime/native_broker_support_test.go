package runtime

import (
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"time"
)

func nativeClosure(workspace IncidentWorkspace) IncidentClosure {
	// Reflection deadlines are compared with the wall clock, so closure times are current.
	now := lifecycleio.Now().UTC()
	return IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: workspace.IncidentID,
			Findings:        []sdk.Finding{stateFinding("health")},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: workspace.IncidentID, Status: IncidentCompleted,
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
		// The controller freezes every closure before its first broker attempt.
		Frozen: true,
	}
}
