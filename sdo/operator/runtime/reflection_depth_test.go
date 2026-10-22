package runtime

import (
	"context"
	"testing"
	"time"

	"encoding/json"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/program"
)

const (
	repeatProgram     = "service-targetport-mismatch"
	repeatApplication = "app-1"
	repeatRevision    = "rev1"
)

var repeatService = &contracts.ObjectRef{Kind: "Service", Namespace: "default", Name: "reservation"}

func receipt(kind, id string) *contracts.RepairActionReceipt {
	at := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &contracts.RepairActionReceipt{ActionId: id, Kind: kind, Target: "Service/reservation", Summary: kind, Details: kind, Success: true, StartedAt: at, CompletedAt: at}
}

func toolCall(t *testing.T, at time.Time, request ToolRequest, response ToolResponse) *contracts.ProgramToolCall {
	t.Helper()
	response.Command = request.Command
	req, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ProgramToolCall{Command: request.Command, At: timestamppb.New(at), RequestJson: string(req), ResponseJson: string(resp)}
}

func verifiedRepeat(t *testing.T) *contracts.OutcomeRecord {
	t.Helper()
	applied := time.Unix(1_700_000_100, 0)
	bindings := program.Bindings{"service": {Kind: "Service", Namespace: "default", Name: "reservation"}}
	return &contracts.OutcomeRecord{
		Classification: "success",
		RepairActions: []*contracts.RepairActionReceipt{
			receipt("sdo-apply", repeatApplication),
			receipt("program-plan", "sdo-program-"+repeatApplication),
		},
		PlanApplications: []*contracts.PlanApplication{{
			Plan: &contracts.Plan{}, ProgramIds: []string{repeatProgram}, Applied: true, ApplicationId: repeatApplication,
			CompletedAt:           timestamppb.New(applied),
			VerificationCompleted: true, VerificationPassed: true,
			Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH},
		}},
		ProgramRuns: []*contracts.ProgramRun{
			{ProgramId: repeatProgram, ProgramRevision: repeatRevision, Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}, Bindings: []*contracts.Binding{{Name: "service", Object: repeatService}}},
			{ProgramId: "image-pull-source-drift", ProgramRevision: repeatRevision, Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}},
		},
		ProgramToolCalls: []*contracts.ProgramToolCall{
			toolCall(t, applied.Add(-time.Minute), ToolRequest{Command: "plan", ProgramID: repeatProgram, Bindings: bindings}, ToolResponse{PlanID: "plan-1"}),
			toolCall(t, applied.Add(time.Second), ToolRequest{Command: "apply", PlanIDs: []string{"plan-1"}}, ToolResponse{
				ApplicationID: repeatApplication,
				ProgramRuns:   []ProgramRun{{ProgramID: repeatProgram, ProgramRevision: repeatRevision, Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch}}},
			}),
		},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "wrong targetPort", Resources: []*contracts.ObjectRef{repeatService}}},
	}
}

func repeatInput(outcome *contracts.OutcomeRecord) ReflectionDepthInput {
	return ReflectionDepthInput{Outcome: outcome, InstalledProgramIDs: []string{"image-pull-source-drift", repeatProgram}, SessionMode: "resume", Attempt: 1}
}

func TestReflectionDepthIsDecidedFromTypedOutcomeFacts(t *testing.T) {
	otherService := &contracts.ObjectRef{Kind: "Service", Namespace: "default", Name: "search"}
	cases := []struct {
		name   string
		mutate func(*contracts.OutcomeRecord, *ReflectionDepthInput)
		want   string
	}{
		{"verified repeat of an installed program refines", func(*contracts.OutcomeRecord, *ReflectionDepthInput) {}, ReflectionDepthRefine},
		{"program unknown to accepted memory is full", func(_ *contracts.OutcomeRecord, in *ReflectionDepthInput) {
			in.InstalledProgramIDs = []string{"image-pull-source-drift"}
		}, ReflectionDepthFull},
		{"fresh session cannot refine", func(_ *contracts.OutcomeRecord, in *ReflectionDepthInput) { in.SessionMode = "fresh" }, ReflectionDepthFull},
		{"validation retry is full", func(_ *contracts.OutcomeRecord, in *ReflectionDepthInput) { in.Attempt = 2 }, ReflectionDepthFull},
		{"diagnostics are full", func(_ *contracts.OutcomeRecord, in *ReflectionDepthInput) { in.HasDiagnostics = true }, ReflectionDepthFull},
		{"ad-hoc repair action is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{Kind: "kubectl-patch", Success: true})
		}, ReflectionDepthFull},
		{"manual action relabelled as sdo-apply is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.RepairActions = append(o.RepairActions, receipt("sdo-apply", "manual-kubectl-edit"))
		}, ReflectionDepthFull},
		{"program-plan receipt without an application is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.RepairActions = append(o.RepairActions, receipt("program-plan", "sdo-program-other"))
		}, ReflectionDepthFull},
		{"suffixed program-plan receipt refines", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.RepairActions[1].ActionId = "sdo-program-" + repeatApplication + "-2"
		}, ReflectionDepthRefine},
		{"failed repair action is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.RepairActions[1].Success = false }, ReflectionDepthFull},
		{"no repair action is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.RepairActions = nil }, ReflectionDepthFull},
		{"no plan application is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications = nil }, ReflectionDepthFull},
		{"unapplied plan is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications[0].Applied = false }, ReflectionDepthFull},
		{"dry run is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications[0].DryRun = true }, ReflectionDepthFull},
		{"rolled back application is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications[0].RolledBack = true }, ReflectionDepthFull},
		{"application error is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications[0].Error = "boom" }, ReflectionDepthFull},
		{"rollback error is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.PlanApplications[0].RollbackError = "stuck"
		}, ReflectionDepthFull},
		{"verification not completed is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.PlanApplications[0].VerificationCompleted = false
		}, ReflectionDepthFull},
		{"verification_passed=false is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.PlanApplications[0].VerificationPassed = false
		}, ReflectionDepthFull},
		{"failed health verification is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_MATCH
		}, ReflectionDepthFull},
		{"unrecorded verification is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.PlanApplications[0].Verification = nil }, ReflectionDepthFull},
		{"post-apply diagnosis missing from tool evidence is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.ProgramToolCalls = nil }, ReflectionDepthFull},
		{"program run without a revision earns no credit", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.ProgramRuns[0].ProgramRevision = "" }, ReflectionDepthFull},
		{"program failure signal is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.ProgramFailureSignals = []string{"adhoc_coverage_gap"}
		}, ReflectionDepthFull},
		{"source repair commit is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { commit := "abc"; o.RepairCommit = &commit }, ReflectionDepthFull},
		{"drift event is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.DriftEvents = []*contracts.DriftEvent{{}} }, ReflectionDepthFull},
		{"exhausted response loop is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.ResponseLoopExhausted = true }, ReflectionDepthFull},
		{"matched program that was not applied is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.ProgramRuns[1].Verdict.Result = contracts.Verdict_RESULT_MATCH
		}, ReflectionDepthFull},
		{"second binding of the applied program still matching is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.ProgramRuns = append(o.ProgramRuns, &contracts.ProgramRun{
				ProgramId: repeatProgram, ProgramRevision: repeatRevision, Duration: durationpb.New(time.Second),
				Verdict:  &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH},
				Bindings: []*contracts.Binding{{Name: "service", Object: otherService}},
			})
		}, ReflectionDepthFull},
		{"root cause no applied binding covers is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) {
			o.ConfirmedRootCauses = append(o.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "other", Resources: []*contracts.ObjectRef{otherService}})
		}, ReflectionDepthFull},
		{"unsuccessful outcome is full", func(o *contracts.OutcomeRecord, _ *ReflectionDepthInput) { o.Classification = "failed" }, ReflectionDepthFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := verifiedRepeat(t)
			input := repeatInput(outcome)
			tc.mutate(outcome, &input)
			if got := ReflectionDepth(input); got != tc.want {
				t.Fatalf("depth = %q, want %q", got, tc.want)
			}
		})
	}
	if got := ReflectionDepth(ReflectionDepthInput{SessionMode: "resume"}); got != ReflectionDepthFull {
		t.Fatalf("nil outcome depth = %q", got)
	}
}

type snapshotCatalog struct {
	snapshot *contracts.OperationalMemorySnapshot
}

func (catalog snapshotCatalog) Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error) {
	return catalog.snapshot, nil
}

// reflectDepth dispatches one reflection and returns the depth its task carried.
func reflectDepth(t *testing.T, worker RegisteredReflectionWorker, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, feedback ReflectionFeedback) string {
	t.Helper()
	var depth string
	executor := reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		depth = request.GetDepth()
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	worker.Executor = executor
	worker.SessionExecutor = func(string) (ReflectionTaskExecutor, error) { return executor, nil }
	if _, err := worker.Reflect(context.Background(), closure, workspace, outcome, "repair", workspace.BaseCommit, feedback); err != nil {
		t.Fatal(err)
	}
	return depth
}

func TestRegisteredReflectionWorkerDecidesDepthFromSessionAndAttempt(t *testing.T) {
	retry := ReflectionFeedback{Attempt: 2, Diagnostics: []*contracts.MemoryDiagnostic{{Rule: "reflection.decision", Message: "rejected"}}}
	for _, tc := range []struct {
		name      string
		mode      string
		sessionID string
		feedback  ReflectionFeedback
		installed []string
		want      string
	}{
		{"verified repeat in the resumed session", "resume", "session-123", ReflectionFeedback{}, []string{repeatProgram}, ReflectionDepthRefine},
		{"program missing from memory", "resume", "session-123", ReflectionFeedback{}, nil, ReflectionDepthFull},
		{"fresh fallback for a missing session", "resume", "", ReflectionFeedback{}, []string{repeatProgram}, ReflectionDepthFull},
		{"configured fresh session", "fresh", "session-123", ReflectionFeedback{}, []string{repeatProgram}, ReflectionDepthFull},
		{"attempt two after a rejected edit", "resume", "session-123", retry, []string{repeatProgram}, ReflectionDepthFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeRepository(t)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: base}
			for _, id := range tc.installed {
				snapshot.Playbooks = append(snapshot.Playbooks, &contracts.MemoryPlaybook{Id: id})
			}
			outcome := verifiedRepeat(t)
			outcome.SchemaVersion, outcome.IncidentId = 1, "current"
			workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
			closure := nativeClosure(workspace)
			closure.Result.ResponderSessionID = tc.sessionID
			worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Catalog: snapshotCatalog{snapshot}, Timeout: time.Minute, SessionMode: tc.mode}
			if got := reflectDepth(t, worker, closure, workspace, outcome, tc.feedback); got != tc.want {
				t.Fatalf("request depth = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRegisteredReflectionWorkerFrozenTaskKeepsItsDepth(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: base, Playbooks: []*contracts.MemoryPlaybook{{Id: repeatProgram}}}
	outcome := verifiedRepeat(t)
	outcome.SchemaVersion, outcome.IncidentId = 1, "current"
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	closure.Result.ResponderSessionID = "session-123"
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Catalog: snapshotCatalog{snapshot}, Timeout: time.Minute, SessionMode: "resume", TaskStore: &lifecycleio.TaskStore{Root: t.TempDir()}}
	if got := reflectDepth(t, worker, closure, workspace, outcome, ReflectionFeedback{}); got != ReflectionDepthRefine {
		t.Fatalf("first dispatch depth = %q", got)
	}
	// A restart replays the frozen task even if the outcome now looks unfit.
	outcome.PlanApplications[0].RolledBack = true
	if got := reflectDepth(t, worker, closure, workspace, outcome, ReflectionFeedback{}); got != ReflectionDepthRefine {
		t.Fatalf("restarted dispatch depth = %q, want the frozen refine", got)
	}
}
