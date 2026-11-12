package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry/registrytest"
)

const syntheticPathsBase = "0000000000000000000000000000000000000001"

type pathsOutcomes struct{ record *contracts.OutcomeRecord }

func (o pathsOutcomes) Get(context.Context, string) (memory.Outcome, error) {
	return memory.Outcome{Record: o.record, IncidentID: o.record.GetIncidentId(), SchemaVersion: 1, Classification: o.record.GetClassification()}, nil
}

func (o pathsOutcomes) List(ctx context.Context) ([]memory.Outcome, error) {
	outcome, err := o.Get(ctx, o.record.GetIncidentId())
	return []memory.Outcome{outcome}, err
}

type pathsCatalog struct {
	snapshot *contracts.OperationalMemorySnapshot
}

func (c pathsCatalog) Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error) {
	return c.snapshot, nil
}

var errPathsCaptured = errors.New("task captured")

type pathsExecutor struct{ task *contracts.AgentTask }

func (e *pathsExecutor) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	e.task = task
	return nil, errPathsCaptured
}

func pathsReflection(t *testing.T, snapshot *contracts.OperationalMemorySnapshot, result *IncidentResult) (*pathsExecutor, error) {
	t.Helper()
	executor := &pathsExecutor{}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident-a", Classification: "success"}
	worker := RegisteredReflectionWorker{Executor: executor, AgentID: "reflector", TaskType: "reflection", Outcomes: pathsOutcomes{record: outcome}, Catalog: pathsCatalog{snapshot: snapshot}, Timeout: time.Hour, SessionMode: "fresh"}
	now := lifecycleio.Now().UTC()
	finding := sdk.Finding{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}
	result.SchemaVersion, result.IncidentID, result.Status = ProtocolSchemaVersion, "incident-a", IncidentCompleted
	result.Timing = TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)}
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: "incident-a",
			Findings:        []sdk.Finding{finding},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: "base", DeployedCommit: "base",
			RepositoryWorktree: "/proposal", RepositoryBaseCommit: "base",
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result:              result,
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
	workspace := IncidentWorkspace{IncidentID: "incident-a", Worktree: t.TempDir(), BaseCommit: syntheticPathsBase}
	_, err := worker.Reflect(context.Background(), closure, workspace, outcome, "proposal", "outcome", ReflectionFeedback{})
	return executor, err
}

// A responder result may name the memory files it wrote, such as the source
// patch the patching skill tells it to capture. That agent prose reaches the
// reflection worker in logical form; it never aborts reflection.
func TestReflectionAcceptsAgentProseNamingMemoryPaths(t *testing.T) {
	result := &IncidentResult{
		RepairChanges: []string{"source change captured in .sdo/deployment/patches/0001-raise-limit.patch"},
		Error:         "read .sdo/outcomes/abc.json for prior evidence",
		ConfirmedRootCauses: []ConfirmedRootCause{{
			Summary:   "limit lowered; see .sdo/playbooks/raise-limit/README.md",
			Resources: []sdk.ObjectRef{{Kind: "Deployment", Name: "health"}},
			Evidence:  []RootCauseEvidence{{Kind: "live-observation", Source: "cat .sdo/deployment/plan.yaml", Observation: "ok"}},
		}},
	}
	executor, err := pathsReflection(t, &contracts.OperationalMemorySnapshot{AcceptedCommit: syntheticPathsBase}, result)
	if !errors.Is(err, errPathsCaptured) {
		t.Fatalf("Reflect error = %v, want the reflection task dispatched", err)
	}
	request := &contracts.ReflectionRequest{}
	if err := executor.task.GetRequest().UnmarshalTo(request); err != nil {
		t.Fatal(err)
	}
	text := protojson.Format(request)
	if layout.ContainsPhysicalPath(text) {
		t.Fatalf("dispatched reflection request names a physical memory path: %s", text)
	}
	for _, want := range []string{"memory:deployment/patches/0001-raise-limit.patch", "memory:outcomes/abc.json", "playbook:raise-limit"} {
		if !strings.Contains(text, want) {
			t.Errorf("dispatched request lacks logical reference %q", want)
		}
	}
}

// SDO-authored logical memory that names a physical path is still a leak.
func TestReflectionRefusesPhysicalPathInMemorySnapshot(t *testing.T) {
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: syntheticPathsBase, ArchitectureMarkdown: "see .sdo/architecture/map.yaml"}
	executor, err := pathsReflection(t, snapshot, &IncidentResult{})
	if err == nil || !strings.Contains(err.Error(), "physical operational memory path") {
		t.Fatalf("Reflect error = %v, want the physical-path leak refused", err)
	}
	if executor.task != nil {
		t.Fatal("reflection task dispatched with a leaking memory snapshot")
	}
}

// The same holds for a native incident task: prose from an earlier episode
// that names a memory file reaches the responder in logical form.
func TestIncidentTaskAcceptsPriorProseNamingMemoryPaths(t *testing.T) {
	now := lifecycleio.Now().UTC()
	request := IncidentRequest{
		SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: "incident-b",
		Findings: []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "fault", Evidence: "evidence",
			PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}},
		DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: "base", DeployedCommit: "base",
		RepositoryWorktree: "/proposal", RepositoryBaseCommit: syntheticPathsBase,
		ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		PriorResponseProgress: []*contracts.ResponseProgress{{IncidentId: "incident-a", Error: "wrote .sdo/deployment/patches/0001-raise-limit.patch"}},
	}
	registry := registrytest.BuiltIn(t)
	typed, err := incidentRequestToProto(request)
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := registry.AgentFor(typed)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: syntheticPathsBase}
	task, _, err := encodeIncidentAgentTask(request, registry, agentID, snapshot)
	if err != nil {
		t.Fatalf("encode incident task: %v", err)
	}
	text := protojson.Format(task)
	if layout.ContainsPhysicalPath(text) || !strings.Contains(text, "memory:deployment/patches/0001-raise-limit.patch") {
		t.Fatalf("incident task does not carry the logical reference: %s", text)
	}
	leaking := &contracts.OperationalMemorySnapshot{AcceptedCommit: syntheticPathsBase, ArchitectureMarkdown: "see .sdo/architecture/map.yaml"}
	if _, _, err := encodeIncidentAgentTask(request, registry, agentID, leaking); err == nil {
		t.Fatal("incident task accepted a memory snapshot that names a physical path")
	}
}
