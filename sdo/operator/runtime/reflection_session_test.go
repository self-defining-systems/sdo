package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// reflectionSessionRun reflects on a synthetic closure with the given responder
// session and prior stages, and reports which executor ran, the frozen request
// and the worker's error.
type reflectionSessionRun struct {
	resume, fresh, shared *pathsExecutor
	resumedIncidents      []string
	request               *contracts.ReflectionRequest
	decision              ReflectionDecision
	err                   error
}

func runReflectionSession(t *testing.T, mutate func(*IncidentClosure), configure func(*RegisteredReflectionWorker)) reflectionSessionRun {
	t.Helper()
	run := reflectionSessionRun{resume: &pathsExecutor{}, fresh: &pathsExecutor{}, shared: &pathsExecutor{}}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident-a", Classification: "success"}
	worker := RegisteredReflectionWorker{
		Executor: run.shared, AgentID: "reflector", TaskType: "reflection", Outcomes: pathsOutcomes{record: outcome},
		Catalog: pathsCatalog{snapshot: &contracts.OperationalMemorySnapshot{AcceptedCommit: syntheticPathsBase}}, Timeout: time.Hour,
		SessionExecutor: func(id string) (ReflectionTaskExecutor, error) {
			run.resumedIncidents = append(run.resumedIncidents, id)
			return run.resume, nil
		},
	}
	if configure != nil {
		configure(&worker)
	}
	now := lifecycleio.Now().UTC()
	finding := sdk.Finding{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: "incident-a",
			Findings:        []sdk.Finding{finding},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: "base", DeployedCommit: "base",
			RepositoryWorktree: "/proposal", RepositoryBaseCommit: "base",
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: "incident-a", Status: IncidentCompleted, ResponderSessionID: "responder-session-1",
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
	if mutate != nil {
		mutate(&closure)
	}
	workspace := IncidentWorkspace{IncidentID: "incident-a", Worktree: t.TempDir(), BaseCommit: syntheticPathsBase}
	run.decision, run.err = worker.Reflect(context.Background(), closure, workspace, outcome, "proposal", "outcome", ReflectionFeedback{Changes: []string{"1. exec Pod/db-1"}})
	for _, executor := range []*pathsExecutor{run.resume, run.fresh, run.shared} {
		if executor.task != nil {
			run.request = &contracts.ReflectionRequest{}
			if err := executor.task.GetRequest().UnmarshalTo(run.request); err != nil {
				t.Fatal(err)
			}
		}
	}
	return run
}

// The closing turn is the responder's own session: the resume transport runs and the
// frozen request carries the session ID and the change list Go rendered for the worker.
func TestReflectionResumesTheResponderSession(t *testing.T) {
	run := runReflectionSession(t, nil, nil)
	if !errors.Is(run.err, errPathsCaptured) || run.resume.task == nil {
		t.Fatalf("reflection did not run on the resume transport: err=%v", run.err)
	}
	if run.fresh.task != nil || run.shared.task != nil {
		t.Fatal("reflection ran outside the responder session transport")
	}
	if run.request.GetClosure().GetResult().GetResponderSessionId() != "responder-session-1" {
		t.Fatalf("request session=%q, want responder-session-1", run.request.GetClosure().GetResult().GetResponderSessionId())
	}
	if got := run.request.GetChanges(); len(got) != 1 || got[0] != "1. exec Pod/db-1" {
		t.Fatalf("request changes = %v, want the rendered change list", got)
	}
	if len(run.resumedIncidents) != 1 || run.resumedIncidents[0] != "incident-a" {
		t.Fatalf("session transport prepared for %v, want incident-a", run.resumedIncidents)
	}
}

// Resume that cannot be prepared is an error the closure retries; it never
// quietly runs the reflection in a new session.
func TestReflectionNeverFallsBackToFreshWhenResumeIsUnavailable(t *testing.T) {
	cases := map[string]func(*RegisteredReflectionWorker){
		"no transport": func(w *RegisteredReflectionWorker) { w.SessionExecutor = nil },
		"transport error": func(w *RegisteredReflectionWorker) {
			w.SessionExecutor = func(string) (ReflectionTaskExecutor, error) { return nil, errors.New("session store unavailable") }
		},
		"nil transport": func(w *RegisteredReflectionWorker) {
			w.SessionExecutor = func(string) (ReflectionTaskExecutor, error) { return nil, nil }
		},
	}
	for name, configure := range cases {
		t.Run(name, func(t *testing.T) {
			run := runReflectionSession(t, nil, configure)
			if run.err == nil || errors.Is(run.err, errPathsCaptured) {
				t.Fatalf("Reflect error = %v, want the unavailable session reported", run.err)
			}
			if run.fresh.task != nil || run.shared.task != nil || run.resume.task != nil {
				t.Fatal("reflection ran although the responder session could not be resumed")
			}
		})
	}
}

// A worker that cannot resume the session reports a typed failure; the
// decision keeps its reason instead of being retried in a new session.
func TestReflectionRecordsAResumeFailureFromTheWorker(t *testing.T) {
	for _, reason := range []string{"session_unavailable", "session_handoff_invalid", "session_handoff_timeout"} {
		t.Run(reason, func(t *testing.T) {
			run := runReflectionSession(t, nil, func(w *RegisteredReflectionWorker) {
				w.SessionExecutor = func(string) (ReflectionTaskExecutor, error) {
					return failingReflection{reason: reason}, nil
				}
			})
			if run.err != nil || run.decision.LearningDecision != "failed" || run.decision.FailureReason != reason {
				t.Fatalf("decision = %+v err = %v, want a failed decision with reason %s", run.decision, run.err, reason)
			}
			if run.fresh.task != nil || run.shared.task != nil {
				t.Fatal("a failed resume was retried in a new session")
			}
		})
	}
}

type failingReflection struct{ reason string }

func (f failingReflection) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	return &contracts.AgentResult{
		SchemaVersion: task.GetSchemaVersion(), TaskId: task.GetTaskId(), ProposalId: task.GetWorkspace().GetProposalId(),
		Status: "failed", FailureReason: f.reason, ErrorSummary: "cannot resume responder session",
	}, nil
}

// A closure without a responder session that the automatic stage did not
// resolve has nothing to resume: reflection is skipped with a recorded reason.
func TestReflectionWithoutAResponderSessionIsSkippedNotFresh(t *testing.T) {
	run := runReflectionSession(t, func(c *IncidentClosure) { c.Result.ResponderSessionID = "" }, nil)
	if run.err != nil || run.decision.LearningDecision != "no_change" || run.decision.NoChangeReason != "no_session" {
		t.Fatalf("decision = %+v err = %v, want no_change/no_session", run.decision, run.err)
	}
	if run.resume.task != nil || run.fresh.task != nil || run.shared.task != nil {
		t.Fatal("reflection ran without a responder session")
	}
}

// A closure the automatic stage resolved has no agent and so no session: the closing turn is skipped, never a fresh session.
func TestStageOnlyClosureGetsNoClosingTurn(t *testing.T) {
	run := runReflectionSession(t, func(c *IncidentClosure) {
		c.Result.ResponderSessionID = ""
		c.Result.ResolutionPath = "deterministic"
		c.Request.PriorStages = []*contracts.StageReport{{Stage: "deterministic"}}
	}, nil)
	if run.err != nil || run.decision.NoChangeReason != "no_session" || run.resume.task != nil || run.fresh.task != nil || run.shared.task != nil {
		t.Fatalf("decision = %+v err = %v, want no_change/no_session and no dispatch", run.decision, run.err)
	}
}
