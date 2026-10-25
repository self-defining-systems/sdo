package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// This exercises accepted Git and outcome memory across the native lifecycle
// and controller. Worker turns and cluster side effects are deterministic fakes.
func TestNativeLifecycleToIncidentOutcomeUsesAcceptedState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	registrations, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	workers := &nativePathWorkers{t: t, registry: registrations}
	installer := &nativePathInstaller{t: t, accepted: accepted}
	backend := &nativePathBackend{}
	strategies, err := deployment.NewRegistry(deployment.RawRegistration("raw-manifests-v1", "demo", backend))
	if err != nil {
		t.Fatal(err)
	}
	strategies.AcceptedRoot = accepted
	coordinator := lifecycle.Coordinator{
		Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registrations,
		Workers: workers, Readiness: strategies,
		Verifier: deployment.IndependentHealthVerifier{AcceptedRoot: accepted, OperatorRoot: "..", Readiness: strategies, Probe: nativePathProbe{}}, Installer: installer,
	}
	result, err := coordinator.Run(ctx, lifecycle.Config{OperationID: "native-bootstrap", Application: "demo", Namespace: "demo", HealthObjective: "The application stays healthy.", DeployerAgentID: "sdo-deployer", JudgeAgentID: "sdo-health-judge", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(workers.tasks, ",") != "source-deployment,source-architecture,health-judgment-direct-v1" {
		t.Fatalf("registered workers did not run in role order: %v", workers.tasks)
	}
	if backend.applies != 1 || backend.observes < 2 {
		t.Fatalf("trusted deployment was not applied and re-observed: applies=%d observes=%d", backend.applies, backend.observes)
	}
	if result.GoalCommit == "" || result.SchemaCommit == "" || result.SourceCommit == "" || result.ArchitectureCommit == "" || result.JudgeCommit == "" || result.SourceCommit == result.ArchitectureCommit {
		t.Fatalf("bootstrap did not make distinct accepted commits: %+v", result)
	}
	if head := nativeGit(t, accepted, "rev-parse", "HEAD"); result.JudgeCommit != head || installer.commit != head {
		t.Fatalf("installer did not receive accepted judge commit: HEAD=%s installer=%s", head, installer.commit)
	}
	if arch := nativeGit(t, accepted, "show", result.ArchitectureCommit+":.sdo/arch.md"); !strings.Contains(arch, "generated_at_commit: "+result.SourceCommit) {
		t.Fatalf("architecture does not cite accepted source: %s", arch)
	}
	if _, err := memory.OpenReader(accepted).Get(ctx, "native-incident"); err != memory.ErrNotFound {
		t.Fatalf("outcome existed before closure: %v", err)
	}

	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{},
		Reflector:        RegisteredReflectionWorker{Executor: workers, AgentID: "sdo-reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(accepted), Timeout: time.Minute, SessionMode: "fresh"},
		ResponderBackend: "test", ResponderModel: "test",
	}
	workspace, err := native.PrepareIncident(ctx, "native-incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repaired source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.SourceCommit, closure.Request.DeployedCommit = result.SourceCommit, result.SourceCommit
	closure.Request.DetectorHistory = []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: closure.DetectedAt, Status: DetectorEvaluationFiring}}
	closure.Result.ConfirmedRootCauses = []ConfirmedRootCause{{
		Summary: "bad replica", Resources: []sdk.ObjectRef{{Kind: "Deployment", Name: "app"}},
		Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: "health", Observation: "health detector fired"}}, ExplainedDetectors: []string{"health"},
	}}
	closure.Result.RepairActions = []RepairActionReceipt{{
		ActionID: "repair-1", Kind: "kubectl", Target: "Deployment/app", Summary: "restored replica", Details: "repaired source was applied", Success: true,
		StartedAt: closure.DetectedAt.Add(100 * time.Millisecond), CompletedAt: closure.DetectedAt.Add(200 * time.Millisecond),
		Resources: []sdk.ObjectRef{{Kind: "Deployment", Name: "app"}},
	}}
	clearedAt := closure.DetectedAt.Add(500 * time.Millisecond)
	closure.HealthClearedAt = &clearedAt
	closure.FinalDetectorStates = []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: closure.VerifiedAt, Status: DetectorEvaluationClear}}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalCommit == "" || receipt.OutcomeCommit == "" || receipt.OutcomeCommit != nativeGit(t, accepted, "rev-parse", "HEAD") {
		t.Fatalf("closure did not accept repair and outcome: %+v", receipt)
	}
	stored, err := memory.OpenReader(accepted).Get(ctx, "native-incident")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Record.GetClassification() != "success" || stored.Record.GetSourceCommit() != result.SourceCommit {
		t.Fatalf("logical outcome lost verified source: %+v", stored.Record)
	}
	prior := relevantOutcomeEvidence(accepted, []sdk.Finding{{DetectorID: "health", Fingerprint: "health"}}, result.SourceCommit, nil)
	if len(prior) != 1 || prior[0].IncidentID != "native-incident" {
		t.Fatalf("runtime could not read broker outcome: %+v", prior)
	}
	if len(workers.tasks) != 4 || workers.tasks[3] != reflectionTaskType(t) {
		t.Fatalf("verified success did not dispatch registered typed reflection: %v", workers.tasks)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	repeated, err := native.ProcessClosure(ctx, closure)
	if err != nil || repeated != receipt {
		t.Fatalf("retry changed accepted closure: %+v %v", repeated, err)
	}
}

type nativePathWorkers struct {
	t        *testing.T
	registry *worker_registry.Registry
	tasks    []string
}

func (w *nativePathWorkers) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if err := w.registry.ValidateTask(task); err != nil {
		return nil, err
	}
	w.tasks = append(w.tasks, task.GetTaskType())
	checkout := task.GetWorkspace().GetCheckoutPath()
	var response *anypb.Any
	var err error
	switch task.GetTaskType() {
	case nativeSourceTask():
		if err = os.WriteFile(filepath.Join(checkout, "app.txt"), []byte("deployed source\n"), 0o644); err != nil {
			return nil, err
		}
		if err = os.WriteFile(filepath.Join(checkout, "Dockerfile"), []byte("FROM scratch\nCOPY app.txt /app.txt\n"), 0o644); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Join(checkout, "deploy"), 0o755); err != nil {
			return nil, err
		}
		manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: demo\n  namespace: demo\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: demo\n  template:\n    metadata:\n      labels:\n        app: demo\n    spec:\n      containers:\n        - name: demo\n          image: registry.example/demo:latest\n"
		if err = os.WriteFile(filepath.Join(checkout, "deploy", "app.yaml"), []byte(manifest), 0o644); err != nil {
			return nil, err
		}
		config, packErr := anypb.New(&contracts.RawManifestStrategy{ManifestPaths: []string{"deploy/app.yaml"}, ExpectedWorkloadIds: []string{"Deployment/demo/demo"}, ImageBuilds: []*contracts.ImageBuild{{Image: "registry.example/demo:latest", ContextPath: ".", DockerfilePath: "Dockerfile"}}})
		if packErr != nil {
			return nil, packErr
		}
		response, err = anypb.New(&contracts.SourceDeploymentResult{Proposed: true, Summary: "source prepared", AgentSessionId: "source-session", Plan: &contracts.DeploymentPlan{StrategyId: "raw-manifests-v1", StrategyConfig: config}})
	case nativeArchitectureTask():
		request := &contracts.ArchitectureRequest{}
		if err = task.GetRequest().UnmarshalTo(request); err != nil {
			return nil, err
		}
		response, err = anypb.New(&contracts.ArchitectureResult{SummaryMarkdown: "# Architecture\nSource-grounded demo.", TopologyFingerprint: request.GetSourceTopologyFingerprint(), SourceCommit: request.GetAcceptedSourceCommit(), AgentSessionId: "architecture-session"})
	case nativeHealthJudgeTask():
		request := &contracts.HealthJudgeRequest{}
		if err = task.GetRequest().UnmarshalTo(request); err != nil {
			return nil, err
		}
		if request.GetArchitectureSummaryMarkdown() == "" {
			return nil, fmt.Errorf("judge did not receive accepted architecture")
		}
		manifest := layout.DetectorManifest{
			APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
			ID: "health-objective", Constructor: "New", Class: "health", Owner: "health_judge",
			Watches:  []layout.Watch{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Endpoints"}, {APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"}},
			Interval: "30s", Persistence: layout.Persistence{Firing: 2, Clearing: 2},
			Batching:          layout.Batching{Severity: "critical", Debounce: "500ms"},
			PossiblePlaybooks: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
		}
		if err := layout.WriteDetector(checkout, layout.Detector{Class: layout.ClassHealth, Manifest: manifest, Files: []layout.File{{Name: "detector.go", Content: formattedNativePathSource(w.t, `package objective

import (
 "time"
 "sdo.dev/operator/sdk"
 "sdo.dev/operator/sdk/servicehealth"
)

func New() sdk.Detector { return servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{ID:"health-objective", Class:sdk.DetectorClassHealth, Owner:sdk.DetectorOwnerHealthJudge, Watches:servicehealth.EndpointWatches(), Interval:30*time.Second, Persistence:sdk.PersistencePolicy{Firing:2,Clearing:2}, Batching:sdk.BatchingPolicy{Severity:sdk.SeverityCritical,Debounce:500*time.Millisecond}, Playbooks:[]string{"health-objective"}, OriginatingCommit:"lifecycle-bootstrap"}) }
`)}}}); err != nil {
			return nil, err
		}
		playbooks := filepath.Join(checkout, layout.PlaybooksDir)
		if err := os.MkdirAll(filepath.Join(playbooks, "health-objective"), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(playbooks, "health-objective", "README.md"), []byte("---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\n# Repair <AFFECTED_RESOURCE>\n"), 0o644); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(playbooks, "README.md"), []byte("# Playbooks\n\n- [health-objective](health-objective/README.md)\n"), 0o644); err != nil {
			return nil, err
		}
		response, err = anypb.New(&contracts.HealthJudgeResult{Authored: true, Summary: "health checks authored", AgentSessionId: "judge-session", ObjectiveDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(request.GetHealthObjective()))), DetectorIds: []string{"health-objective"}})
	case reflectionTaskType(w.t):
		request := &contracts.ReflectionRequest{}
		if err = task.GetRequest().UnmarshalTo(request); err != nil {
			return nil, err
		}
		if request.GetOutcomeCommit() != task.GetWorkspace().GetBaseCommit() || request.GetOutcome() == nil {
			return nil, fmt.Errorf("reflection did not start from committed outcome")
		}
		var gaps []*contracts.SdkGap
		for _, cause := range request.Outcome.ConfirmedRootCauses {
			gaps = append(gaps, &contracts.SdkGap{FaultClass: "synthetic-fixture", Primitive: "fixture-capability", Reason: "lifecycle transport test has no executable observation fixture", ScratchScript: "scratch/test", Resources: cause.Resources})
		}
		response, err = anypb.New(&contracts.ReflectionResult{LearningDecision: "sdk_gap", NoChangeReason: "synthetic lifecycle fixture lacks executable observations", SdkGaps: gaps})
	default:
		return nil, fmt.Errorf("unexpected task %q", task.GetTaskType())
	}
	if err != nil {
		return nil, err
	}
	result := &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.GetTaskId(), Status: "completed", ProposalId: task.GetWorkspace().GetProposalId(), Response: response}
	if err := w.registry.ValidateResult(task, result); err != nil {
		return nil, err
	}
	return result, nil
}

type nativePathBackend struct {
	applies  int
	observes int
	revision string
	image    string
}

func (b *nativePathBackend) BuildImage(_ context.Context, _ string, _ *contracts.ImageBuild) (string, error) {
	return "registry.example/demo@sha256:" + strings.Repeat("b", 64), nil
}

func (b *nativePathBackend) Apply(_ context.Context, manifest []byte) error {
	revision := regexp.MustCompile(`sdo.dev/source-tree: ([a-f0-9]{40})`).FindSubmatch(manifest)
	if len(revision) != 2 {
		return fmt.Errorf("trusted adapter did not stamp candidate tree digest")
	}
	b.image = "registry.example/demo@sha256:" + strings.Repeat("b", 64)
	if !strings.Contains(string(manifest), b.image) {
		return fmt.Errorf("trusted adapter did not use immutable image")
	}
	b.revision = string(revision[1])
	b.applies++
	return nil
}

func (b *nativePathBackend) Observe(_ context.Context, id string) (deployment.WorkloadObservation, error) {
	b.observes++
	if id != "Deployment/demo/demo" || b.revision == "" {
		return deployment.WorkloadObservation{}, fmt.Errorf("unobserved workload %q", id)
	}
	return deployment.WorkloadObservation{ResourceID: id, Revision: b.revision, Images: []string{b.image}, Ready: true}, nil
}

type nativePathProbe struct{}

func (nativePathProbe) Evaluate(context.Context, string, string, string) (string, error) {
	return "clear", nil
}

type nativePathInstaller struct {
	t        *testing.T
	accepted string
	commit   string
}

func (i *nativePathInstaller) Install(_ context.Context, commit, namespace string) error {
	if namespace != "demo" || commit != nativeGit(i.t, i.accepted, "rev-parse", "HEAD") {
		return fmt.Errorf("installer did not receive accepted revision")
	}
	i.commit = commit
	return nil
}

// The task types are whatever the built-in manifests declare for each request
// message; the test restates none of them.
func nativeSourceTask() string       { return manifestTaskType(&contracts.SourceDeploymentRequest{}) }
func nativeArchitectureTask() string { return manifestTaskType(&contracts.ArchitectureRequest{}) }
func nativeHealthJudgeTask() string  { return manifestTaskType(&contracts.HealthJudgeRequest{}) }

func manifestTaskType(request proto.Message) string {
	registry, err := worker_registry.LoadDirectory(registrytest.Directory())
	if err != nil {
		panic(err)
	}
	agent, err := registry.AgentFor(request)
	if err != nil {
		panic(err)
	}
	task, err := registry.TaskType(agent, request)
	if err != nil {
		panic(err)
	}
	return task
}

func formattedNativePathSource(t *testing.T, source string) []byte {
	t.Helper()
	formatted, err := format.Source([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return formatted
}
