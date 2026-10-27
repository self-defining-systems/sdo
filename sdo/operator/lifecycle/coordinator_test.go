package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

type fakeBroker struct {
	events              []string
	accepted            map[string]broker.Receipt
	evidence            map[string]broker.SourceEvidence
	missingEvidence     bool
	pendingFinalization bool
	lastCommit          string
	mutatedSource       bool
	noChange            map[string]bool
	// rejectPublish makes the next N Publish calls for a proposal fail with
	// typed validator diagnostics, as the real broker does.
	rejectPublish map[string]int
	// proposalRoot overrides the checkout directory for tests that run a real
	// executor against a temporary repository mount.
	proposalRoot string
	rejections    map[string]*contracts.ProposalRejectionLog
}

func (f *fakeBroker) Rejections(id string) (*contracts.ProposalRejectionLog, error) {
	if log, ok := f.rejections[id]; ok {
		return log, nil
	}
	return &contracts.ProposalRejectionLog{ProposalId: id}, nil
}
func (f *fakeBroker) RecordRejection(id string, diagnostics []*contracts.MemoryDiagnostic) (*contracts.ProposalRejectionLog, error) {
	log, _ := f.Rejections(id)
	if log.Terminal {
		return log, nil
	}
	log.Rejections = append(log.Rejections, broker.NewProposalRejection(uint32(len(log.Rejections)+1), diagnostics))
	log.Terminal = broker.RetriesExhausted(log.Rejections)
	if f.rejections == nil {
		f.rejections = map[string]*contracts.ProposalRejectionLog{}
	}
	f.rejections[id] = log
	f.events = append(f.events, "record-rejection:"+id)
	return log, nil
}

func (f *fakeBroker) CreateProposalForPhase(_ context.Context, id string, phase broker.Phase) (broker.Proposal, error) {
	f.events = append(f.events, "create:"+id)
	root := f.proposalRoot
	if root == "" {
		root = "/workspace/proposals/"
	}
	return broker.Proposal{IncidentID: id, Path: root + id, BaseCommit: f.lastCommit}, nil
}
func (f *fakeBroker) Publish(_ context.Context, id string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	f.events = append(f.events, "publish:"+id+":"+string(actor))
	if f.rejectPublish[id] > 0 {
		f.rejectPublish[id]--
		return broker.Receipt{}, &validate.Failure{Diagnostics: []validate.Diagnostic{{Rule: "detector.manifest", Path: ".sdo/diagnostics/detectors/health/x/manifest.yaml", Line: 3, Message: "bad manifest", Hint: "fix it"}}}
	}
	r := broker.Receipt{IncidentID: id, Phase: phase, Commit: "commit-" + id, ChangedPaths: []string{"changed"}}
	f.accepted[id] = r
	f.lastCommit = r.Commit
	return r, nil
}
func (f *fakeBroker) PublishVerifiedSource(ctx context.Context, id string, evidence broker.SourceEvidence) (broker.Receipt, error) {
	if f.mutatedSource || evidence.ProposalTreeDigest != strings.Repeat("a", 40) {
		return broker.Receipt{}, errors.New("source proposal changed after verification")
	}
	receipt, err := f.Publish(ctx, id, broker.Deployer, broker.InitialPhase)
	if err == nil {
		if !f.pendingFinalization {
			evidence.AcceptedCommit = receipt.Commit
		}
		if f.evidence == nil {
			f.evidence = map[string]broker.SourceEvidence{}
		}
		f.evidence[id] = evidence
	}
	return receipt, err
}
func (f *fakeBroker) SourceEvidence(_ context.Context, id string) (broker.SourceEvidence, bool, error) {
	if f.missingEvidence {
		return broker.SourceEvidence{}, false, nil
	}
	e, ok := f.evidence[id]
	if ok && e.AcceptedCommit == "" && f.pendingFinalization {
		// The broker ledger has staged the evidence and the accepted commit
		// exists, but the process crashed before ledger finalization.
		e.AcceptedCommit = f.accepted[id].Commit
		f.evidence[id] = e
	}
	return e, ok, nil
}
func (f *fakeBroker) AcceptedCommit(_ context.Context, id string, phase broker.Phase) (broker.Receipt, bool, error) {
	r, ok := f.accepted[id]
	return r, ok, nil
}
func (f *fakeBroker) ChangedPaths(_ context.Context, id string, phase broker.Phase) ([]string, error) {
	if f.noChange[id] {
		return nil, nil
	}
	return []string{"changed"}, nil
}
func (f *fakeBroker) ReleaseProposal(id string, phase broker.Phase) error {
	f.events = append(f.events, "release:"+id)
	return nil
}

type fakeBootstrap struct{ events *[]string }

func (f fakeBootstrap) WriteGoal(_ string, application, objective string) error {
	*f.events = append(*f.events, "write-goal:"+application)
	return nil
}
func (f fakeBootstrap) WriteSchema(_ string) error {
	*f.events = append(*f.events, "write-schema")
	return nil
}
func (f fakeBootstrap) WriteArchitecture(_ string, application, sourceCommit, fingerprint, summary string) error {
	*f.events = append(*f.events, "write-architecture:"+sourceCommit)
	return nil
}
func (f fakeBootstrap) ReadArchitecture(_ string) (string, error) {
	return "# Architecture\nSource-grounded layout.\n", nil
}

type fakeExecutor struct {
	tasks               []*contracts.AgentTask
	deployFailures      int
	archFailures        int
	retiredJudgePayload bool
	judgeJobFailures    int
	judgeJobsVanished   int
	discarded           []string
}

func (f *fakeExecutor) DiscardVanished(_ context.Context, task *contracts.AgentTask) error {
	f.discarded = append(f.discarded, task.GetTaskId())
	return nil
}

func (f *fakeExecutor) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	f.tasks = append(f.tasks, task)
	var response *anypb.Any
	var err error
	switch task.GetTaskType() {
	case sourceDeploymentTask:
		proposed := len(f.tasks) > f.deployFailures
		raw, packErr := anypb.New(&contracts.RawManifestStrategy{ManifestPaths: []string{"deploy/app.yaml"}, ExpectedWorkloadIds: []string{"Deployment/demo"}})
		if packErr != nil {
			return nil, packErr
		}
		response, err = anypb.New(&contracts.SourceDeploymentResult{Proposed: proposed, Summary: "prepared", AgentSessionId: "session", Plan: &contracts.DeploymentPlan{StrategyId: "raw-manifests-v1", StrategyConfig: raw}})
	case architectureTask:
		if f.archFailures > 0 {
			f.archFailures--
			return nil, errors.New("architecture worker interrupted")
		}
		request := &contracts.ArchitectureRequest{}
		if err := task.GetRequest().UnmarshalTo(request); err != nil {
			return nil, err
		}
		response, err = anypb.New(&contracts.ArchitectureResult{SummaryMarkdown: "# Architecture\nSource-grounded layout.", TopologyFingerprint: request.GetSourceTopologyFingerprint(), SourceCommit: request.GetAcceptedSourceCommit(), AgentSessionId: "architecture-session"})
	case healthJudgeTask:
		if f.judgeJobsVanished > 0 {
			f.judgeJobsVanished--
			return nil, &worker_registry.JobVanishedError{TaskID: task.GetTaskId(), JobName: worker_registry.JobName(task.GetTaskId())}
		}
		if f.judgeJobFailures > 0 {
			f.judgeJobFailures--
			return nil, &worker_registry.JobFailedError{JobName: worker_registry.JobName(task.GetTaskId())}
		}
		request := &contracts.HealthJudgeRequest{}
		if err := task.GetRequest().UnmarshalTo(request); err != nil {
			return nil, err
		}
		response, err = anypb.New(&contracts.HealthJudgeResult{Authored: true, Summary: "authored", AgentSessionId: "judge-session", ObjectiveDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(request.GetHealthObjective()))), DetectorIds: []string{"health-objective"}})
		if f.retiredJudgePayload {
			result := &contracts.HealthJudgeResult{}
			if err := response.UnmarshalTo(result); err != nil {
				return nil, err
			}
			result.ArtifactProposal = &contracts.OperationalArtifactProposal{}
			response, err = anypb.New(result)
		}
	default:
		return nil, fmt.Errorf("unexpected task type %s", task.GetTaskType())
	}
	if err != nil {
		return nil, err
	}
	return &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: response}, nil
}

type fakeVerifier struct {
	events  *[]string
	healthy bool
}

func (f fakeVerifier) Verify(_ context.Context, commit, namespace, objective string, source DeploymentReceipt) (bool, string, error) {
	if !completeDeploymentReceipt(source) {
		return false, "", errors.New("missing source evidence")
	}
	*f.events = append(*f.events, "verify:"+commit)
	return f.healthy, "unhealthy", nil
}

type fakeInstaller struct{ events *[]string }

func (f fakeInstaller) Install(_ context.Context, commit, namespace string) error {
	*f.events = append(*f.events, "install:"+commit)
	return nil
}

type fakeReadiness struct {
	healthy bool
	events  *[]string
	drift   bool
}

func (f fakeReadiness) ApplyAndVerify(_ context.Context, checkout string, plan *contracts.DeploymentPlan) (DeploymentReceipt, error) {
	*f.events = append(*f.events, "readiness:"+checkout)
	if plan.GetStrategyId() != "raw-manifests-v1" {
		return DeploymentReceipt{}, errors.New("wrong strategy")
	}
	if !f.healthy {
		return DeploymentReceipt{}, errors.New("unavailable deployment")
	}
	digest := strings.Repeat("a", 40)
	return DeploymentReceipt{ProposalTreeDigest: digest, ObservedRevision: digest, StrategyID: plan.GetStrategyId(), Images: []string{"image@sha256:" + strings.Repeat("b", 64)}, ResourceIDs: []string{"Deployment/demo"}, Ready: true}, nil
}

func (f fakeReadiness) VerifyAccepted(_ context.Context, commit, namespace string, previous DeploymentReceipt) (DeploymentReceipt, error) {
	*f.events = append(*f.events, "reobserve:"+commit)
	if !f.healthy {
		return DeploymentReceipt{}, errors.New("accepted deployment no longer ready")
	}
	if f.drift {
		previous.Images = []string{"other@sha256:" + strings.Repeat("c", 64)}
	}
	return previous, nil
}

func testCoordinator(t *testing.T) (Coordinator, *fakeBroker, *fakeExecutor, *[]string) {
	t.Helper()
	capabilities := map[string][]*contracts.AgentCapability{}
	for _, item := range []struct{ id, task, request, result string }{
		{"source-agent", sourceDeploymentTask, "SourceDeploymentRequest", "SourceDeploymentResult"},
		{"source-agent", architectureTask, "ArchitectureRequest", "ArchitectureResult"},
		{"judge-agent", healthJudgeTask, "HealthJudgeRequest", "HealthJudgeResult"},
	} {
		capabilities[item.id] = append(capabilities[item.id], &contracts.AgentCapability{TaskType: item.task, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1." + item.request, ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1." + item.result})
	}
	registrations := []*contracts.AgentRegistration{}
	for id, caps := range capabilities {
		registrations = append(registrations, &contracts.AgentRegistration{SchemaVersion: worker_registry.SchemaVersion, AgentId: id, Capabilities: caps, Launch: &contracts.AgentRegistration_LocalProcess{LocalProcess: &contracts.LocalProcessLaunch{Command: []string{"test"}}}})
	}
	registry, err := worker_registry.New(registrations...)
	if err != nil {
		t.Fatal(err)
	}
	events := []string{}
	b := &fakeBroker{accepted: map[string]broker.Receipt{}}
	e := &fakeExecutor{}
	c := Coordinator{Broker: b, Bootstrap: fakeBootstrap{&events}, Registry: registry, Workers: e, Readiness: fakeReadiness{healthy: true, events: &events}, Verifier: fakeVerifier{&events, true}, Installer: fakeInstaller{&events}, SourceFacts: func(_ context.Context, checkout, accepted string) (memory.SourceFacts, error) {
		proposalID := strings.TrimPrefix(checkout, "/workspace/proposals/")
		proposalID = strings.Replace(proposalID, "-architecture-", "-source-", 1)
		proposalID = strings.Replace(proposalID, "-judge-", "-source-", 1)
		if !strings.HasPrefix(checkout, "/workspace/proposals/") || accepted != "commit-"+proposalID {
			return memory.SourceFacts{}, errors.New("inventory was not bound to accepted source")
		}
		return memory.SourceFacts{Fingerprint: strings.Repeat("f", 64), Resources: []*contracts.SourceResource{{Kind: "Deployment", Name: "demo", Namespace: "demo", Source: "deploy.yaml"}}}, nil
	}}
	return c, b, e, &events
}

func TestCoordinatorPublishesEachRoleBeforeIndependentVerification(t *testing.T) {
	c, b, e, events := testCoordinator(t)
	result, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "stay healthy", RegistryHost: "registry.demo.svc:5000", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceCommit != "commit-init-1-source-1" || result.ArchitectureCommit != "commit-init-1-architecture-1" || result.JudgeCommit != "commit-init-1-judge-1" {
		t.Fatalf("unexpected accepted commits: %#v", result)
	}
	want := []string{"publish:init-1-goal:human", "publish:init-1-schema:upkeep", "publish:init-1-source-1:deployer", "publish:init-1-architecture-1:deployer", "publish:init-1-judge-1:health_judge"}
	var got []string
	for _, event := range b.events {
		if len(event) >= 8 && event[:8] == "publish:" {
			got = append(got, event)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("wrong broker sequence: got %v, want %v", got, want)
	}
	if len(e.tasks) != 3 || e.tasks[0].GetTaskType() != sourceDeploymentTask || e.tasks[1].GetTaskType() != architectureTask || e.tasks[2].GetTaskType() != healthJudgeTask {
		t.Fatalf("wrong worker sequence: %#v", e.tasks)
	}
	if e.tasks[0].GetWorkspace().GetCheckoutPath() != "/workspace/proposals/init-1-source-1" {
		t.Fatalf("worker did not receive proposal checkout: %#v", e.tasks[0].Workspace)
	}
	deployerRequest := &contracts.SourceDeploymentRequest{}
	if err := e.tasks[0].GetRequest().UnmarshalTo(deployerRequest); err != nil || deployerRequest.GetRegistryHost() != "registry.demo.svc:5000" {
		t.Fatalf("worker did not receive trusted registry host: %+v %v", deployerRequest, err)
	}
	architectureRequest := &contracts.ArchitectureRequest{}
	if err := e.tasks[1].GetRequest().UnmarshalTo(architectureRequest); err != nil {
		t.Fatal(err)
	}
	if architectureRequest.GetAcceptedSourceCommit() != result.SourceCommit || architectureRequest.GetSourceTopologyFingerprint() != strings.Repeat("f", 64) || len(architectureRequest.GetSourceResources()) != 1 || architectureRequest.GetSourceResources()[0].GetSource() != "deploy.yaml" {
		t.Fatalf("architecture worker did not receive accepted source facts: %+v", architectureRequest)
	}
	judgeRequest := &contracts.HealthJudgeRequest{}
	if err := e.tasks[2].GetRequest().UnmarshalTo(judgeRequest); err != nil {
		t.Fatal(err)
	}
	if judgeRequest.GetAcceptedSourceCommit() != result.SourceCommit || judgeRequest.GetSourceTopologyFingerprint() != strings.Repeat("f", 64) || len(judgeRequest.GetSourceResources()) != 1 || judgeRequest.GetSourceResources()[0].GetSource() != "deploy.yaml" {
		t.Fatalf("judge did not receive accepted source facts: %+v", judgeRequest)
	}
	if fmt.Sprint(*events) != fmt.Sprint([]string{"write-goal:demo", "write-schema", "readiness:/workspace/proposals/init-1-source-1", "write-architecture:commit-init-1-source-1", "verify:commit-init-1-judge-1", "install:commit-init-1-judge-1"}) {
		t.Fatalf("wrong operational sequence: %v", *events)
	}
}

func TestCoordinatorDoesNotPublishUnverifiedSourceClaim(t *testing.T) {
	c, b, _, events := testCoordinator(t)
	c.Readiness = fakeReadiness{healthy: false, events: events}
	_, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil {
		t.Fatal("expected independent readiness rejection")
	}
	for _, event := range b.events {
		if event == "publish:init-1-source-1:deployer" {
			t.Fatal("published source on agent self-report alone")
		}
	}
}

func TestCoordinatorRejectsSourceMutationAfterReadinessVerification(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	b.mutatedSource = true
	_, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil {
		t.Fatal("expected source mutation rejection")
	}
	for _, event := range b.events {
		if event == "publish:init-1-source-1:deployer" {
			t.Fatal("published changed source after verification")
		}
	}
}

func TestCoordinatorReusesUnchangedBootstrapArtifacts(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	b.noChange = map[string]bool{"init-1-goal": true, "init-1-schema": true}
	_, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range b.events {
		if event == "publish:init-1-goal:human" || event == "publish:init-1-schema:upkeep" {
			t.Fatal("republished unchanged bootstrap memory")
		}
	}
}

func TestCoordinatorMigratesMemoryAsUpkeepOnly(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	receipt, err := c.MigrateMemory(context.Background(), "migrate-1")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit != "commit-migrate-1-schema" {
		t.Fatalf("receipt = %+v", receipt)
	}
	want := []string{"create:migrate-1-schema", "publish:migrate-1-schema:upkeep"}
	if strings.Join(b.events, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want only the Upkeep schema step", b.events)
	}
	if _, err := (Coordinator{}).MigrateMemory(context.Background(), "migrate-1"); err == nil {
		t.Fatal("migration ran without a broker")
	}
}

func TestCoordinatorResumesAfterAcceptedSourceWithoutRepublishingIt(t *testing.T) {
	c, b, executor, _ := testCoordinator(t)
	executor.archFailures = 1
	cfg := Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("expected first architecture worker interruption")
	}
	if _, err := c.Run(context.Background(), cfg); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sourcePublishes := 0
	for _, event := range b.events {
		if event == "publish:init-1-source-1:deployer" {
			sourcePublishes++
		}
	}
	if sourcePublishes != 1 {
		t.Fatalf("accepted source was republished %d times", sourcePublishes)
	}
}

func TestCoordinatorRecoversStagedSourceEvidenceAfterPublishCrash(t *testing.T) {
	c, b, executor, events := testCoordinator(t)
	b.pendingFinalization = true
	executor.archFailures = 1
	cfg := Config{OperationID: "init-crash", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("expected interruption after accepted source")
	}
	if got := b.evidence["init-crash-source-1"].AcceptedCommit; got != "" {
		t.Fatalf("test did not model interrupted ledger finalization: %q", got)
	}
	if _, err := c.Run(context.Background(), cfg); err != nil {
		t.Fatalf("resume staged source evidence: %v", err)
	}
	if got := b.evidence["init-crash-source-1"].AcceptedCommit; got != "commit-init-crash-source-1" {
		t.Fatalf("evidence was not reconciled to accepted source commit: %q", got)
	}
	if !containsEvent(*events, "reobserve:commit-init-crash-source-1") {
		t.Fatal("accepted deployment was not independently re-observed")
	}
}

func TestCoordinatorRejectsChangedDeploymentOnResume(t *testing.T) {
	c, _, executor, events := testCoordinator(t)
	executor.archFailures = 1
	cfg := Config{OperationID: "init-drift", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("expected interruption")
	}
	c.Readiness = fakeReadiness{healthy: true, events: events, drift: true}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("changed image was accepted on resume")
	}
}

func containsEvent(events []string, expected string) bool {
	for _, event := range events {
		if event == expected {
			return true
		}
	}
	return false
}

func TestCoordinatorRejectsUnknownWorkerBeforeCreatingProposals(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	_, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "unknown", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil || !errors.Is(err, ErrAgentRegistration) {
		t.Fatalf("expected registration failure, got %v", err)
	}
	if len(b.events) != 0 {
		t.Fatalf("created proposal before registration validation: %v", b.events)
	}
}

func TestCoordinatorRejectsMismatchedRoleTypesBeforeCreatingProposals(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	wrong, err := worker_registry.New(
		&contracts.AgentRegistration{SchemaVersion: worker_registry.SchemaVersion, AgentId: "source-agent", Capabilities: []*contracts.AgentCapability{{TaskType: sourceDeploymentTask, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionResult"}, {TaskType: architectureTask, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ArchitectureRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ArchitectureResult"}}, Launch: &contracts.AgentRegistration_LocalProcess{LocalProcess: &contracts.LocalProcessLaunch{Command: []string{"worker"}}}},
		&contracts.AgentRegistration{SchemaVersion: worker_registry.SchemaVersion, AgentId: "judge-agent", Capabilities: []*contracts.AgentCapability{{TaskType: healthJudgeTask, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.HealthJudgeRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.HealthJudgeResult"}}, Launch: &contracts.AgentRegistration_LocalProcess{LocalProcess: &contracts.LocalProcessLaunch{Command: []string{"worker"}}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Registry = wrong
	_, err = c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if !errors.Is(err, ErrAgentRegistration) {
		t.Fatalf("expected role type failure, got %v", err)
	}
	if len(b.events) != 0 {
		t.Fatalf("created proposal before type validation: %v", b.events)
	}
}

type wrongResultExecutor struct{}

func (wrongResultExecutor) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	response, err := anypb.New(&contracts.ReflectionResult{Summary: "wrong payload"})
	if err != nil {
		return nil, err
	}
	return &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: response}, nil
}

func TestCoordinatorRejectsWrongWorkerResultBeforeBrokerPublish(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	c.Workers = wrongResultExecutor{}
	_, err := c.Run(context.Background(), Config{OperationID: "init-1", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil {
		t.Fatal("expected typed result rejection")
	}
	for _, event := range b.events {
		if event == "publish:init-1-source-1:deployer" {
			t.Fatal("published mismatched worker result")
		}
	}
}

// The task types are whatever the built-in manifests declare for each request
// message; the tests restate none of them.
var (
	sourceDeploymentTask = manifestTaskType(&contracts.SourceDeploymentRequest{})
	architectureTask     = manifestTaskType(&contracts.ArchitectureRequest{})
	healthJudgeTask      = manifestTaskType(&contracts.HealthJudgeRequest{})
)

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

func judgeRequests(t *testing.T, e *fakeExecutor) []*contracts.HealthJudgeRequest {
	t.Helper()
	var requests []*contracts.HealthJudgeRequest
	for _, task := range e.tasks {
		if task.GetTaskType() != healthJudgeTask {
			continue
		}
		request := &contracts.HealthJudgeRequest{}
		if err := task.GetRequest().UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
	}
	return requests
}

func TestCoordinatorRedispatchesRejectedHealthJudgeWithTypedDiagnostics(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	b.rejectPublish = map[string]int{"op-judge-1": 1}
	result, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 1 {
		t.Fatalf("a retried judge must not consume a deployment attempt: %+v", result)
	}
	requests := judgeRequests(t, e)
	if len(requests) != 2 {
		t.Fatalf("judge dispatches = %d", len(requests))
	}
	if requests[0].GetValidationAttempt() != 1 || len(requests[0].GetPreviousDiagnostics()) != 0 {
		t.Fatalf("first dispatch carries feedback: %+v", requests[0])
	}
	retry := requests[1]
	if retry.GetValidationAttempt() != 2 || len(retry.GetPreviousDiagnostics()) != 1 || retry.GetPreviousDiagnostics()[0].GetRule() != "detector.manifest" || retry.GetPreviousDiagnostics()[0].GetLine() != 3 {
		t.Fatalf("retry lacks typed diagnostics: %+v", retry)
	}
	if retry.GetCorrectionFeedback() != "" {
		t.Fatalf("broker diagnostics must not travel as a string: %q", retry.GetCorrectionFeedback())
	}
	if e.tasks[len(e.tasks)-2].GetTaskId() == e.tasks[len(e.tasks)-1].GetTaskId() {
		t.Fatal("retry must be a distinct task")
	}
}

func TestCoordinatorStopsRetryingHealthJudgeAfterBound(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	b.rejectPublish = map[string]int{"op-judge-1": 99}
	_, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil {
		t.Fatal("expected exhaustion")
	}
	if got := len(judgeRequests(t, e)); got != 1+broker.MaxValidationRetries {
		t.Fatalf("judge dispatches = %d", got)
	}
	log, _ := b.Rejections("op-judge-1")
	if !log.GetTerminal() || len(log.GetRejections()) != 1+broker.MaxValidationRetries {
		t.Fatalf("terminal rejection not recorded: %+v", log)
	}
}

func TestCoordinatorResumesHealthJudgeRetryCountAfterRestart(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	diagnostic := &contracts.MemoryDiagnostic{Rule: "detector.manifest", Message: "earlier rejection"}
	b.rejections = map[string]*contracts.ProposalRejectionLog{"op-judge-1": {ProposalId: "op-judge-1", Rejections: []*contracts.ProposalRejection{broker.NewProposalRejection(1, []*contracts.MemoryDiagnostic{diagnostic}), broker.NewProposalRejection(2, []*contracts.MemoryDiagnostic{diagnostic})}}}
	if _, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	requests := judgeRequests(t, e)
	if len(requests) != 1 || requests[0].GetValidationAttempt() != 3 || requests[0].GetPreviousDiagnostics()[0].GetMessage() != "earlier rejection" {
		t.Fatalf("restart reset or duplicated attempts: %+v", requests)
	}
}

func TestCoordinatorRejectsRetiredJudgePayloadBeforePublishing(t *testing.T) {
	c, b, worker, _ := testCoordinator(t)
	worker.retiredJudgePayload = true
	_, err := c.Run(context.Background(), Config{OperationID: "retired", Application: "demo", Namespace: "demo", HealthObjective: "healthy", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil || !strings.Contains(err.Error(), "rejects deprecated artifact_proposal") {
		t.Fatalf("retired payload accepted: %v", err)
	}
	for _, event := range b.events {
		if event == "publish:retired-judge-1:health_judge" {
			t.Fatal("published retired judge payload")
		}
	}
}
