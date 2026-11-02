//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"strings"
	"testing"
	"time"
)

func TestRegisteredReflectionWorkerExportsTurnUsage(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	logPath := filepath.Join(t.TempDir(), "reflection-usage.jsonl")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh", UsageLogPath: func(string) string { return logPath }}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if err := os.WriteFile(logPath, []byte(`{"usage":{"input_tokens":42,"output_tokens":7,"model_requests":2}}`+"\n"), 0600); err != nil {
			return nil, err
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Usage == nil || decision.Usage.InputTokens != 42 || decision.Usage.OutputTokens != 7 || decision.Usage.GetModelRequests() != 2 {
		t.Fatalf("reflection usage lost: %+v", decision.Usage)
	}
}

func TestRegisteredReflectionWorkerSumsEveryTurnUsage(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "reflection-usage.jsonl")
	lines := `{"usage":{"input_tokens":100,"cache_read_input_tokens":60,"cached_input_tokens":60,"output_tokens":10,"llm_calls":1,"model_requests":4}}` + "\n" +
		"\n" +
		`{"usage":{"input_tokens":200,"cache_read_input_tokens":150,"cached_input_tokens":150,"output_tokens":20,"llm_calls":1,"model_requests":5}}` + "\n" +
		`not json` + "\n" +
		`{"usage":{"input_tokens":50,"cache_read_input_tokens":10,"cached_input_tokens":10,"output_tokens":5,"llm_calls":1,"model_requests":3}}` + "\n"
	if err := os.WriteFile(logPath, []byte(lines), 0600); err != nil {
		t.Fatal(err)
	}
	worker := RegisteredReflectionWorker{UsageLogPath: func(string) string { return logPath }}
	decision, err := worker.withUsage(ReflectionDecision{}, "current")
	if err != nil {
		t.Fatal(err)
	}
	usage := decision.Usage
	if usage == nil || usage.InputTokens != 350 || usage.OutputTokens != 35 || usage.GetCacheReadInputTokens() != 220 || usage.GetModelRequests() != 12 || usage.LlmCalls != 3 {
		t.Fatalf("reflection usage must sum every turn (3 turns): %+v", usage)
	}
}

func TestRegisteredReflectionWorkerReturnsTypedFailure(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "failed", ErrorSummary: "watch namespace is not a proto field"}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	decision, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil || decision.LearningDecision != "failed" || decision.FailureReason != "agent_failed" || decision.Error != "watch namespace is not a proto field" {
		t.Fatalf("typed failure lost: %+v, %v", decision, err)
	}
}

func TestRegisteredReflectionWorkerKeepsWorkerFailureReason(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "failed", FailureReason: "agent_timeout", ErrorSummary: "codex reflection failed: agent turn timed out after 451s"}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	decision, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil || decision.LearningDecision != "failed" || decision.FailureReason != "agent_timeout" {
		t.Fatalf("worker failure reason lost: %+v, %v", decision, err)
	}
}

func TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: strings.Repeat("x", 700<<10)})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	_, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err == nil || !strings.Contains(err.Error(), "transport limit") {
		t.Fatalf("oversized worker result passed reflection validation: %v", err)
	}
}

func TestRegisteredReflectionWorkerReturnsTypedSDKGap(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		response, err := anypb.New(&contracts.ReflectionResult{
			LearningDecision: "sdk_gap", NoChangeReason: "The incident requires a UDP probe",
			SdkGaps: []*contracts.SdkGap{{FaultClass: "service-port-mismatch", Primitive: "ProbeUDP", Reason: "The incident required a UDP probe", ScratchScript: "kubectl run probe"}},
		})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "sdk_gap" || len(decision.SDKGaps) != 1 || decision.SDKGaps[0].Primitive != "ProbeUDP" {
		t.Fatalf("SDK gap lost: %+v", decision)
	}
}

func TestRegisteredReflectionWorkerUsesTypedTaskAndPriorMemory(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	if _, err := store.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "prior", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	w := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute, SessionMode: "fresh"}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if task.TaskType != reflectionTaskType(t) || task.Workspace.CheckoutPath != root {
			t.Fatalf("wrong task: %+v", task)
		}
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.GetClosure().GetRequest().GetIncidentId() != "current" || len(request.GetPriorOutcomes()) != 1 || request.ProposalCommit != "repair" || request.OutcomeCommit != "outcome" {
			t.Fatalf("wrong reflection request: %+v", request)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "prior case already covers it"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := w.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "no_change" {
		t.Fatalf("wrong decision: %+v", decision)
	}
}

func TestRegisteredReflectionWorkerMissingSessionNeverStartsFresh(t *testing.T) {
	root := nativeRepository(t)
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	closure := nativeClosure(workspace)
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "resume"}
	calls := 0
	worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		return reflectionNoChangeResult(ctx, task)
	})
	worker.SessionExecutor = func(string) (ReflectionTaskExecutor, error) { return worker.Executor, nil }
	for _, id := range []string{"", "../escape", "session-123"} {
		closure.Result.ResponderSessionID = id
		decision, err := worker.Reflect(context.Background(), closure, workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
		if err != nil {
			t.Fatal(err)
		}
		if id != "session-123" && (calls != 0 || decision.NoChangeReason != "no_session") {
			t.Fatalf("absent session executed: calls=%d decision=%v", calls, decision)
		}
	}
	if calls != 1 {
		t.Fatalf("saved session calls=%d", calls)
	}
}

func TestRegisteredReflectionRetryUsesANewTaskIdentity(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute, SessionMode: "fresh"}
	var ids []string
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		ids = append(ids, task.TaskId)
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	record := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}
	for _, attempt := range []int{0, 0, 1} {
		feedback := ReflectionFeedback{Attempt: uint32(attempt + 1)}
		if attempt > 0 {
			feedback.Diagnostics = []*contracts.MemoryDiagnostic{{Rule: "test", Message: "retry"}}
		}
		if _, err := worker.Reflect(context.Background(), closure, workspace, record, "repair", "outcome", feedback); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] != ids[1] {
		t.Fatalf("the same attempt must keep its identity so a restart resumes its Job: %v", ids)
	}
	if ids[2] == ids[0] {
		t.Fatalf("a retry after a rejected reflection must not reuse the rejected attempt's Job and result: %v", ids)
	}
}

func TestRegisteredReflectionWorkerDirectEdits(t *testing.T) {
	for _, tc := range []struct {
		name, decision, reason  string
		edit, legacy, wantError bool
	}{
		{name: "update", decision: "updated", edit: true},
		{name: "no change", decision: "no_change", reason: "already covered"},
		{name: "SDK gap", decision: "sdk_gap", reason: "SDK lacks the observation"},
		{name: "SDK gap without reason", decision: "sdk_gap", wantError: true},
		{name: "hidden edit", decision: "no_change", reason: "already covered", edit: true},
		{name: "empty update", decision: "updated"},
		{name: "legacy proposal", decision: "updated", edit: true, legacy: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
			worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				if task.TaskType != reflectionTaskType(t) {
					t.Fatalf("legacy capability: %s", task.TaskType)
				}
				if tc.edit {
					if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/base/README.md"), []byte("updated"), 0o644); err != nil {
						return nil, err
					}
				}
				response := &contracts.ReflectionResult{LearningDecision: tc.decision, NoChangeReason: tc.reason}
				if tc.decision == "sdk_gap" {
					response.SdkGaps = []*contracts.SdkGap{{FaultClass: "fault", Primitive: "ProbeUDP", Reason: "UDP", ScratchScript: "probe"}}
				}
				if tc.edit {
					response.ProposedArtifactIds = []string{"base"}
				}
				if tc.legacy {
					response.ArtifactProposal = &contracts.OperationalArtifactProposal{}
				}
				packed, err := anypb.New(response)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
			})
			workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
			_, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", base, ReflectionFeedback{})
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
			if tc.wantError {
				var failure *validate.Failure
				if !errors.As(err, &failure) || len(failure.Diagnostics) == 0 {
					t.Fatalf("result rejection omitted shared diagnostics: %v", err)
				}
				for _, diagnostic := range failure.Diagnostics {
					if diagnostic.Rule != validate.RuleReflectionDecision {
						t.Fatalf("result rejection rule = %q", diagnostic.Rule)
					}
				}
			}
		})
	}
}

func TestRegisteredReflectionRequestCarriesProgramRunsOnlyOnceAndNotForPriorOutcomes(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	run := &contracts.ProgramRun{ProgramId: "program", Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}
	if _, err := store.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "prior", Classification: "success", ProgramRuns: []*contracts.ProgramRun{run, run}}); err != nil {
		t.Fatal(err)
	}
	w := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute, SessionMode: "fresh"}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if len(request.GetPriorOutcomes()) != 1 || len(request.GetPriorOutcomes()[0].GetProgramRuns()) != 0 || request.GetPriorOutcomes()[0].GetClassification() != "success" {
			t.Fatalf("a prior outcome carries program runs: %+v", request.GetPriorOutcomes())
		}
		if len(request.GetOutcome().GetProgramRuns()) != 1 {
			t.Fatalf("the current outcome lost its program runs: %+v", request.GetOutcome())
		}
		closure := request.GetClosure()
		if len(closure.GetResult().GetProgramRuns()) != 0 || closure.GetRequest().GetPreparedAgentTask() != "" || len(closure.GetRequest().GetInitialProgramRuns()) != 0 {
			t.Fatalf("the closure repeats evidence the request carries elsewhere: %+v", closure)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	closure.Request.PreparedAgentTask = strings.Repeat("t", 1024)
	closure.Request.InitialProgramRuns = []ProgramRun{{ProgramID: "program"}}
	closure.Result.ProgramRuns = []ProgramRun{{ProgramID: "program"}}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success", ProgramRuns: []*contracts.ProgramRun{run}}
	if _, err := w.Reflect(context.Background(), closure, workspace, outcome, "repair", "outcome", ReflectionFeedback{}); err != nil {
		t.Fatal(err)
	}
}

// A reflection request is dispatched through a ConfigMap, so its size is bounded by construction: raw tool
// transcripts are carried once (in the outcome) and earlier incidents only as bounded summaries.
func TestRegisteredReflectionRequestFitsTheTransportBudgetByConstruction(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	transcript := func(prefix string) []*contracts.ProgramToolCall {
		calls := make([]*contracts.ProgramToolCall, 40)
		for i := range calls {
			calls[i] = &contracts.ProgramToolCall{Command: prefix, ResponseJson: strings.Repeat("r", 4000)}
		}
		return calls
	}
	for _, id := range []string{"prior-1", "prior-2", "prior-3", "prior-4"} {
		if _, err := store.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success", ProgramToolCalls: transcript(id)}); err != nil {
			t.Fatal(err)
		}
	}
	w := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute, SessionMode: "fresh"}
	var dispatched *contracts.AgentTask
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		dispatched = task
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	for i := 0; i < 40; i++ {
		closure.Result.ProgramToolCalls = append(closure.Result.ProgramToolCalls, ToolCall{Command: "current", ResponseJSON: strings.Repeat("r", 4000)})
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success", ProgramToolCalls: transcript("current")}
	if _, err := w.Reflect(context.Background(), closure, workspace, outcome, "repair", "outcome", ReflectionFeedback{}); err != nil {
		t.Fatal(err)
	}
	request := &contracts.ReflectionRequest{}
	if err := dispatched.Request.UnmarshalTo(request); err != nil {
		t.Fatal(err)
	}
	if size := proto.Size(dispatched); size > worker_registry.MaxRequestBytes {
		t.Fatalf("reflection task is %d bytes, over the %d byte request budget", size, worker_registry.MaxRequestBytes)
	}
	if len(request.GetOutcome().GetProgramToolCalls()) != 40 {
		t.Fatalf("the current outcome lost its tool transcript: %d calls", len(request.GetOutcome().GetProgramToolCalls()))
	}
	if len(request.GetClosure().GetResult().GetProgramToolCalls()) != 0 {
		t.Fatal("the closure repeats the outcome's tool transcript")
	}
	for _, prior := range request.GetPriorOutcomes() {
		if len(prior.GetProgramToolCalls()) != 0 {
			t.Fatalf("prior outcome %s carries a raw tool transcript", prior.GetIncidentId())
		}
	}
	if len(request.GetPriorOutcomes()) > maxReflectionHistory {
		t.Fatalf("history carries %d incidents, want at most %d", len(request.GetPriorOutcomes()), maxReflectionHistory)
	}
}

func TestReflectionDirectFilesCLIAndBroker(t *testing.T) {
	ctx := context.Background()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := operatorCLIFixture(t)
	for _, scenario := range []string{"detector and README", "malformed then fixed", "no change", "SDK gap"} {
		t.Run(scenario, func(t *testing.T) {
			target := nativeRepository(t)
			if err := os.CopyFS(target, os.DirFS("../memory/validate/testdata/good")); err != nil {
				t.Fatal(err)
			}
			// Accepted executable and session observations bind the detector to
			// the independently verified Service cause used below.
			fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo"
			module, err := os.ReadFile(filepath.Join(fixture, "diagnostics/go.mod"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
				t.Fatal(err)
			}
			testutil.CopyPortProgram(t, target, fixture)
			copyFastloopTree(t, filepath.Join(fixture, "corpus"), filepath.Join(target, ".sdo/corpus"))
			for _, relative := range []string{".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", ".sdo/diagnostics/detectors/incidents/cache/detector.go"} {
				path := filepath.Join(target, relative)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data = []byte(strings.ReplaceAll(string(data), "base", "service-targetport-mismatch"))
				if err := os.WriteFile(path, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			index := filepath.Join(target, ".sdo/playbooks/README.md")
			data, err := os.ReadFile(index)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(index, append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, target, "add", "-A")
			nativeGit(t, target, "commit", "-qm", "memory")
			compile := func(ctx context.Context, root string, _ []string) error {
				ds, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
				if err != nil {
					return err
				}
				return validate.Err(ds)
			}
			repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
			if err != nil {
				t.Fatal(err)
			}
			sessions := t.TempDir()
			capture, err := memory.IncidentCorpusDir(sessions, "direct-files")
			if err != nil {
				t.Fatal(err)
			}
			reflectionFixtureHealthy(t, capture)
			t.Setenv("SDO_CAPTURED_CORPUS_DIR", capture)
			calls := 0
			var authoredREADME, authoredGo []byte
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute, SessionMode: "fresh"}
			worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				calls++
				checkout, base := task.Workspace.CheckoutPath, task.Workspace.BaseCommit
				readme := filepath.Join(checkout, ".sdo/playbooks/base/README.md")
				detector := filepath.Join(checkout, ".sdo/diagnostics/detectors/incidents/cache/detector.go")
				response := &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "already covered"}
				if scenario == "SDK gap" {
					response.SdkGaps = []*contracts.SdkGap{{FaultClass: "fault", Primitive: "ProbeUDP", Reason: "UDP", ScratchScript: "probe"}}
					response.LearningDecision = "sdk_gap"
					response.NoChangeReason = "No deterministic SDK observation for this cause"
				}
				if scenario != "no change" && scenario != "SDK gap" {
					original, err := os.ReadFile(readme)
					if err != nil {
						return nil, err
					}
					authoredREADME = append(original, []byte("\n## Verification\nThe controller confirmed recovery.\n")...)
					source, err := os.ReadFile(detector)
					if err != nil {
						return nil, err
					}
					authoredGo = append(source, []byte("\n// Confirmed signature retained after reflection.\n")...)
					if err := os.WriteFile(detector, authoredGo, 0o644); err != nil {
						return nil, err
					}
					if err := os.WriteFile(readme, authoredREADME, 0o644); err != nil {
						return nil, err
					}
					programReadme := filepath.Join(checkout, ".sdo/playbooks/service-targetport-mismatch/README.md")
					programText, err := os.ReadFile(programReadme)
					if err != nil {
						return nil, err
					}
					if err := os.WriteFile(programReadme, append(programText, []byte("\nThe current controller receipt confirms this repair step.\n")...), 0644); err != nil {
						return nil, err
					}
					caseDir := filepath.Join(checkout, ".sdo/corpus/current-repair")
					if err := os.MkdirAll(caseDir, 0755); err != nil {
						return nil, err
					}
					labels, err := os.ReadFile(filepath.Join(checkout, ".sdo/corpus/targetport-mismatch/labels.json"))
					if err != nil {
						return nil, err
					}
					if err := os.WriteFile(filepath.Join(caseDir, "labels.json"), labels, 0644); err != nil {
						return nil, err
					}
					appendControllerRepairReceipts(t, checkout, "current-repair", reflectionFixtureOutcome("direct-files"))
					response = &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base", "service-targetport-mismatch"}, SdkGaps: []*contracts.SdkGap{reflectionFixtureGap()}}
					if scenario == "malformed then fixed" {
						if err := os.WriteFile(readme, []byte("malformed README\n"), 0o644); err != nil {
							return nil, err
						}
						writeReflectionCheckDecision(t, checkout, response)
						report, code := runReflectionMemoryCheck(t, binary, checkout, base, operatorRoot)
						if code != 1 || len(report.Diagnostics) == 0 {
							t.Fatalf("malformed README passed CLI: %d %v", code, report)
						}
						_, rejected := repository.Publish(ctx, "direct-files", broker.Responder, broker.ReflectionPhase)
						var failure *validate.Failure
						if !errors.As(rejected, &failure) {
							t.Fatalf("broker did not return named diagnostics: %v", rejected)
						}
						ds := failure.Diagnostics
						if !proto.Equal(report, &contracts.MemoryCheckReport{Root: checkout, Actor: "responder", Phase: "reflection", Base: base, Diagnostics: validate.ProtoDiagnostics(ds)}) {
							t.Fatalf("CLI/broker diagnostics differ: CLI %v broker %v", report, ds)
						}
						if err := os.WriteFile(readme, authoredREADME, 0o644); err != nil {
							return nil, err
						}
					}
				}
				writeReflectionCheckDecision(t, checkout, response)
				report, code := runReflectionMemoryCheck(t, binary, checkout, base, operatorRoot)
				if code != 0 {
					t.Fatalf("final memory check failed: %d %v", code, report)
				}
				packed, err := anypb.New(response)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
			})
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), SessionRoot: sessions, Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
				derived := testDerivedOutcome(c.Request.IncidentID, "success", true)
				if scenario != "no change" && scenario != "SDK gap" {
					derived.Record = reflectionFixtureOutcome(c.Request.IncidentID)
				}
				return derived, nil
			})}
			workspace, err := native.PrepareIncident(ctx, "direct-files")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repaired\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			receipt, err := native.ProcessClosure(ctx, nativeClosure(workspace))
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("worker dispatched %d times", calls)
			}
			if scenario == "no change" || scenario == "SDK gap" {
				if receipt.ReflectionCommit != "" {
					t.Fatal("no-change created a commit")
				}
			} else {
				if receipt.ReflectionCommit == "" {
					t.Fatal("update was not committed")
				}
				ledger, err := native.readLedger("direct-files")
				if err != nil || len(ledger.GetReflectionDecision().GetPlaybookIds()) != 2 || ledger.GetReflectionDecision().GetPlaybookIds()[0] != "base" {
					t.Fatalf("accepted playbook IDs not durable: %v %v", ledger, err)
				}
				for rel, want := range map[string][]byte{".sdo/playbooks/base/README.md": authoredREADME, ".sdo/diagnostics/detectors/incidents/cache/detector.go": authoredGo} {
					got, err := os.ReadFile(filepath.Join(target, rel))
					if err != nil || string(got) != string(want) {
						t.Fatalf("authored bytes changed in %s: %v", rel, err)
					}
				}
			}
		})
	}
}

func runReflectionMemoryCheck(t *testing.T, binary, root, base, operatorRoot string) (*contracts.MemoryCheckReport, int) {
	t.Helper()
	command := exec.Command(binary, "memory", "check", "--json")
	command.Dir = root
	command.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+base, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=")
	out, err := command.Output()
	code := 0
	if err != nil {
		var failed *exec.ExitError
		if !errors.As(err, &failed) {
			t.Fatal(err)
		}
		code = failed.ExitCode()
		if code > 1 {
			t.Fatalf("CLI infrastructure failure: %s", failed.Stderr)
		}
	}
	report := &contracts.MemoryCheckReport{}
	if err := protojson.Unmarshal(out, report); err != nil {
		t.Fatalf("CLI report: %v: %s", err, out)
	}
	return report, code
}

func TestReflectionBrokerDiagnosticRetryIsDurableAndBounded(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if int(request.ValidationAttempt) != calls+1 {
			t.Fatalf("correction %d, want %d", request.ValidationAttempt, calls)
		}
		if calls > 0 && len(request.PreviousDiagnostics) == 0 {
			t.Fatal("durable diagnostic feedback missing")
		}
		calls++
		if err := os.WriteFile(filepath.Join(task.Workspace.CheckoutPath, ".sdo/playbooks/base/README.md"), []byte("bad README\n"), 0o644); err != nil {
			return nil, err
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
	})}
	workspace, err := native.PrepareIncident(ctx, "retry")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("retry")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 1+broker.MaxValidationRetries || !ledger.ReflectionRejected || !ledger.ReflectionProcessed || ledger.ReflectionCommit != "" {
		t.Fatalf("terminal rejection not durable: %v", ledger)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.Attempt != uint32(i+1) || len(rejection.Diagnostics) == 0 {
			t.Fatalf("missing diagnostic feedback: %v", rejection)
		}
	}
	if _, err := repository.ProposalInfo("retry", broker.ReflectionPhase); err == nil {
		t.Fatal("rejected checkout retained")
	}
	native = NativeIncidentBroker{Repository: repository, LedgerRoot: native.LedgerRoot, Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: native.Deriver}
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}

	if want := 1 + broker.MaxValidationRetries; calls != want {
		t.Fatalf("dispatched %d times, want %d", calls, want)
	}
}

func TestReflectionInfrastructureFailureDoesNotSpendLearningRetries(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	validations := 0
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, paths []string) error {
		for _, path := range paths {
			if strings.HasPrefix(path, ".sdo/playbooks/") {
				validations++
				return errors.New("validator infrastructure unavailable")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
	}), Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		path := filepath.Join(w.Worktree, ".sdo/playbooks/base/README.md")
		content, err := os.ReadFile(path)
		if err != nil {
			return ReflectionDecision{}, err
		}
		err = os.WriteFile(path, append(content, []byte("\nConfirmed recovery.\n")...), 0o644)
		return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, err
	})}
	workspace, err := native.PrepareIncident(ctx, "infrastructure")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	for range 2 {
		if _, err := native.ProcessClosure(ctx, closure); err == nil || !strings.Contains(err.Error(), "infrastructure unavailable") {
			t.Fatalf("infrastructure failure lost: %v", err)
		}
		ledger, err := native.readLedger(workspace.IncidentID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ledger.ReflectionRejections) != 0 || ledger.ReflectionRejected {
			t.Fatalf("infrastructure consumed learning budget: %v", ledger)
		}
	}
	if calls != 1 || validations != 2 {
		t.Fatalf("worker dispatched %d times for %d validation attempts", calls, validations)
	}
}

// Hand-written program and toy snapshots exercise direct-edit publication without run outputs.
func TestReflectionLearnedProgramCLIAndBroker(t *testing.T) {
	ctx := context.Background()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := operatorCLIFixture(t)
	for _, scenario := range []string{"valid", "physical_path", "failed_closure", "undetected_repair"} {
		physical := scenario == "physical_path"
		t.Run(scenario, func(t *testing.T) {
			target := nativeRepository(t)
			copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
			module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(target, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, target, "add", "-A")
			nativeGit(t, target, "commit", "-qm", "memory")
			const incidentID = "learned-program"
			sessions := t.TempDir()
			capture, err := memory.IncidentCorpusDir(sessions, incidentID)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(capture, 0755); err != nil {
				t.Fatal(err)
			}
			fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo/corpus/targetport-mismatch"
			for destination, source := range map[string]string{"open@detect.json": "open.json", "fixed.json": "fixed.json", "open@detect-recordings.json": "recordings.json"} {
				data, err := os.ReadFile(filepath.Join(fixture, source))
				if err != nil {
					t.Fatal(err)
				}
				if destination == "fixed.json" {
					healthy, err := os.ReadFile(filepath.Join(fixture, "healthy.json"))
					if err != nil {
						t.Fatal(err)
					}
					var fixedView, healthyView map[string]any
					if err := json.Unmarshal(data, &fixedView); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(healthy, &healthyView); err != nil {
						t.Fatal(err)
					}
					for key, value := range healthyView {
						if items, ok := value.([]any); ok {
							prior, _ := fixedView[key].([]any)
							fixedView[key] = append(prior, items...)
						}
					}
					data, err = json.Marshal(fixedView)
					if err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "undetected_repair" && destination != "open@detect-recordings.json" {
					observed := time.Unix(0, 0).UTC()
					if destination == "fixed.json" {
						observed = time.Unix(3, 0).UTC()
					}
					data = capturefixture.Recapture(t, data, func(snapshot *sdktest.Snapshot) {
						snapshot.ObservedAtTime = &observed
						for i := range snapshot.ServiceList {
							if snapshot.ServiceList[i].Name == "front" {
								snapshot.ServiceList[i].UID = "same-front-object"
							}
						}
					})
				}
				if err := os.WriteFile(filepath.Join(capture, destination), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			reflectionFixtureHealthy(t, capture)
			compile := func(ctx context.Context, root string, _ []string) error {
				diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
				if err != nil {
					return err
				}
				return validate.Err(diagnostics)
			}
			repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
			if err != nil {
				t.Fatal(err)
			}
			outcome := reflectionFixtureOutcome(incidentID)
			// A second cause has a real, resource-scoped SDK gap: an observation
			// the SDK does not offer, with no recorded hand repair of its own. It
			// must not excuse the learnable Service cause.
			peer := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "database"}
			gap := &contracts.SdkGap{FaultClass: "database", Primitive: "DatabaseReplicationStatus", Reason: "synthetic unavailable replication status read", ScratchScript: "scratch/replication", Resources: []*contracts.ObjectRef{peer}}
			outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "database replication recovered", Resources: []*contracts.ObjectRef{peer}})
			outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: "database-health", RuleId: "replication", Severity: "warn", Summary: "replica lagging", Evidence: "synthetic observed database fault", Status: "active", PrimaryResource: peer, Fingerprint: "database"})
			outcome.DetectorHistory = append(outcome.DetectorHistory, &contracts.DetectorEvaluation{DetectorId: "database-health", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(3, 0))})
			if scenario == "undetected_repair" {
				outcome.ConfirmedRootCauses = outcome.ConfirmedRootCauses[1:]
				outcome.Findings = outcome.Findings[1:]
				outcome.UndetectedCauses = validate.CaptureUndetectedCauses(outcome, capture)
				if len(outcome.UndetectedCauses) != 1 {
					t.Fatal("recorded Service repair was not offered as undetected learning")
				}
			}
			if scenario == "failed_closure" {
				outcome.Classification = "failed"
				outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: reflectionFixtureResources()}}}
				outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "unresolved blocker", Resources: []*contracts.ObjectRef{{Kind: "Role", Namespace: "demo", Name: "blocker"}}})
			}
			if _, err := repository.RecordOutcome(ctx, incidentID, outcome); err != nil {
				t.Fatal(err)
			}
			// The controller copies original repair receipts into the capture
			// before reflection, as the registered worker does in production.
			if err := memory.CaptureRepairReceipts(sessions, incidentID, outcome); err != nil {
				t.Fatal(err)
			}
			proposal, err := repository.CreateProposalForPhase(ctx, incidentID, broker.ReflectionPhase)
			if err != nil {
				t.Fatal(err)
			}
			var authoredTree string
			var checkErr error
			var report contracts.MemoryCheckReport
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
			worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				checkout := task.Workspace.CheckoutPath
				testutil.CopyPortProgram(t, checkout, "../../../tests/fixtures/sdo/playbook_program_app/.sdo")
				writeCapturedProgramCorpus(t, checkout, capture, incidentID, "service-targetport-mismatch", "service", "Service", "demo", "front", "peer", "detect")
				index := filepath.Join(checkout, ".sdo/playbooks/README.md")
				data, err := os.ReadFile(index)
				if err != nil {
					return nil, err
				}
				data = append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...)
				if err := os.WriteFile(index, data, 0644); err != nil {
					return nil, err
				}
				if physical {
					path := filepath.Join(checkout, ".sdo/playbooks/service-targetport-mismatch/README.md")
					data, err := os.ReadFile(path)
					if err != nil {
						return nil, err
					}
					if err := os.WriteFile(path, append(data, []byte("\nRead .sdo/goal.md for context.\n")...), 0644); err != nil {
						return nil, err
					}
				}
				decision := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"service-targetport-mismatch"}, SdkGaps: []*contracts.SdkGap{gap}}
				writeReflectionCheckDecision(t, checkout, decision)
				command := exec.Command(binary, "memory", "check", "--json")
				command.Dir = checkout
				command.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR="+capture, "SDO_INCIDENT_ID="+incidentID)
				output, err := command.Output()
				checkErr = err
				if err := protojson.Unmarshal(output, &report); err != nil {
					t.Fatalf("CLI report: %v: %s", err, output)
				}
				nativeGit(t, checkout, "add", "-f", ".sdo")
				authoredTree = nativeGit(t, checkout, "write-tree")
				response, err := anypb.New(decision)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			})
			workspace := IncidentWorkspace{IncidentID: incidentID, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}
			reflected, err := worker.Reflect(ctx, nativeClosure(workspace), workspace, outcome, "repair", proposal.BaseCommit, ReflectionFeedback{})
			if err != nil {
				t.Fatal(err)
			}
			if reflected.LearningDecision != "updated" || len(reflected.SDKGaps) != 1 || len(reflected.PlaybookIDs) != 1 {
				t.Fatalf("mixed publication lost repaired playbook or peer gap: %+v", reflected)
			}
			// Broker validation is controller work after the worker returns; it must
			// not consume the worker execution deadline. Both still compile for real.
			receipt, publishErr := repository.PublishReflection(validate.WithCaptureSource(ctx, capture, incidentID), incidentID, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}, SdkGaps: []*contracts.SdkGap{gap}})
			if physical {
				var failure *validate.Failure
				if checkErr == nil || !errors.As(publishErr, &failure) {
					t.Fatalf("physical path accepted: CLI %v broker %v", checkErr, publishErr)
				}
				if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
					t.Fatalf("diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
				}
				if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != "text.physical-path" {
					t.Fatalf("unexpected physical-path diagnostics: %v", report.Diagnostics)
				}
			} else {
				if checkErr != nil || publishErr != nil {
					t.Fatalf("learned program rejected: CLI %v %v broker %v", checkErr, report.Diagnostics, publishErr)
				}
				// The proposal checkout and accepted commit contain exactly the same tree.
				acceptedTree := nativeGit(t, target, "rev-parse", receipt.Commit+"^{tree}")
				if authoredTree != acceptedTree {
					t.Fatalf("accepted tree %s differs from authored %s", acceptedTree, authoredTree)
				}
			}
			if !physical && authoredTree == "" {
				t.Fatal("program was not committed")
			}
		})
	}
}

func reflectionFixtureResources() []*contracts.ObjectRef {
	return []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}
}
func reflectionFixtureGap() *contracts.SdkGap {
	return &contracts.SdkGap{FaultClass: "synthetic-routing", Primitive: "FixtureUnavailableObservation", Reason: "transport fixture records an explicit SDK coverage gap", ScratchScript: "scratch/fixture", Resources: reflectionFixtureResources()}
}

func reflectionFixtureOutcome(incidentID string) *contracts.OutcomeRecord {
	resources := reflectionFixtureResources()
	return &contracts.OutcomeRecord{
		SchemaVersion: 1, IncidentId: incidentID, Classification: "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "verified synthetic routing repair", Resources: resources}},
		Findings:            []*contracts.Finding{{DetectorId: "routing-relations", RuleId: "targetport", Status: "active", Severity: "warn", Summary: "port mismatch", Evidence: "selected pod exposes another port", Fingerprint: "front-port", PrimaryResource: resources[0]}},
		RepairActions:       []*contracts.RepairActionReceipt{{ActionId: "repair-front-port", Kind: "kubectl", Target: "Service/demo/front", Summary: "restore port", Details: "port restored and read back", Success: true, Resources: resources, StartedAt: timestamppb.New(time.Unix(1, 0)), CompletedAt: timestamppb.New(time.Unix(2, 0))}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "routing-relations", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(3, 0))}},
	}
}

func reflectionFixtureHealthy(t *testing.T, capture string) {
	t.Helper()
	healthy := filepath.Join(capture, "healthy")
	if err := os.MkdirAll(healthy, 0755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/corpus/targetport-mismatch/healthy.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(healthy, "healthy.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
}
