package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

func receipt(kind string) *contracts.RepairActionReceipt {
	at := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &contracts.RepairActionReceipt{ActionId: kind + "-1", Kind: kind, Target: "Service/reservation", Summary: kind, Details: kind, Success: true, StartedAt: at, CompletedAt: at}
}

func verifiedRepeat() *contracts.OutcomeRecord {
	return &contracts.OutcomeRecord{
		Classification: "success",
		RepairActions: []*contracts.RepairActionReceipt{
			receipt("sdo-apply"),
			receipt("program-plan"),
		},
		PlanApplications: []*contracts.PlanApplication{{
			Plan: &contracts.Plan{}, ProgramIds: []string{"service-targetport-mismatch"}, Applied: true,
			Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH},
		}},
		ProgramRuns: []*contracts.ProgramRun{
			{ProgramId: "service-targetport-mismatch", Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}},
			{ProgramId: "image-pull-source-drift", Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}},
		},
	}
}

func TestReflectionDepthIsDecidedFromTypedOutcomeFacts(t *testing.T) {
	installed := []string{"image-pull-source-drift", "service-targetport-mismatch"}
	cases := []struct {
		name      string
		mutate    func(*contracts.OutcomeRecord)
		installed []string
		want      string
	}{
		{"verified repeat of an installed program refines", func(*contracts.OutcomeRecord) {}, installed, ReflectionDepthRefine},
		{"program unknown to accepted memory is full", func(*contracts.OutcomeRecord) {}, []string{"image-pull-source-drift"}, ReflectionDepthFull},
		{"ad-hoc repair action is full", func(o *contracts.OutcomeRecord) {
			o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{Kind: "kubectl-patch", Success: true})
		}, installed, ReflectionDepthFull},
		{"failed repair action is full", func(o *contracts.OutcomeRecord) { o.RepairActions[1].Success = false }, installed, ReflectionDepthFull},
		{"no repair action is full", func(o *contracts.OutcomeRecord) { o.RepairActions = nil }, installed, ReflectionDepthFull},
		{"no plan application is full", func(o *contracts.OutcomeRecord) { o.PlanApplications = nil }, installed, ReflectionDepthFull},
		{"unapplied plan is full", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].Applied = false }, installed, ReflectionDepthFull},
		{"failed verification is full", func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_MATCH
		}, installed, ReflectionDepthFull},
		{"unrecorded verification is full", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].Verification = nil }, installed, ReflectionDepthFull},
		{"program failure signal is full", func(o *contracts.OutcomeRecord) { o.ProgramFailureSignals = []string{"adhoc_coverage_gap"} }, installed, ReflectionDepthFull},
		{"source repair commit is full", func(o *contracts.OutcomeRecord) { commit := "abc"; o.RepairCommit = &commit }, installed, ReflectionDepthFull},
		{"matched program that was not applied is full", func(o *contracts.OutcomeRecord) {
			o.ProgramRuns[1].Verdict.Result = contracts.Verdict_RESULT_MATCH
		}, installed, ReflectionDepthFull},
		{"unsuccessful outcome is full", func(o *contracts.OutcomeRecord) { o.Classification = "failure" }, installed, ReflectionDepthFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := verifiedRepeat()
			tc.mutate(outcome)
			if got := ReflectionDepth(outcome, tc.installed); got != tc.want {
				t.Fatalf("depth = %q, want %q", got, tc.want)
			}
		})
	}
	if got := ReflectionDepth(nil, installed); got != ReflectionDepthFull {
		t.Fatalf("nil outcome depth = %q", got)
	}
}

type snapshotCatalog struct {
	snapshot *contracts.OperationalMemorySnapshot
}

func (catalog snapshotCatalog) Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error) {
	return catalog.snapshot, nil
}

func TestRegisteredReflectionWorkerFreezesDepthInTheTask(t *testing.T) {
	for _, tc := range []struct {
		name      string
		outcome   *contracts.OutcomeRecord
		installed []string
		want      string
	}{
		{"verified repeat", verifiedRepeat(), []string{"service-targetport-mismatch", "image-pull-source-drift"}, ReflectionDepthRefine},
		{"program missing from memory", verifiedRepeat(), []string{"image-pull-source-drift"}, ReflectionDepthFull},
		{"first incident without programs", &contracts.OutcomeRecord{Classification: "success"}, nil, ReflectionDepthFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeRepository(t)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: base}
			for _, id := range tc.installed {
				snapshot.Playbooks = append(snapshot.Playbooks, &contracts.MemoryPlaybook{Id: id})
			}
			outcome := tc.outcome
			outcome.SchemaVersion, outcome.IncidentId = 1, "current"
			worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Catalog: snapshotCatalog{snapshot}, Timeout: time.Minute}
			var depth string
			worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				request := &contracts.ReflectionRequest{}
				if err := task.Request.UnmarshalTo(request); err != nil {
					t.Fatal(err)
				}
				depth = request.GetDepth()
				response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			})
			workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
			if _, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, outcome, "repair", base, ReflectionFeedback{}); err != nil {
				t.Fatal(err)
			}
			if depth != tc.want {
				t.Fatalf("request depth = %q, want %q", depth, tc.want)
			}
		})
	}
}
