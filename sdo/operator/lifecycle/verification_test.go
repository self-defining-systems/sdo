package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// fakeBroker accepts every publication and numbers the commits. It is the
// in-memory stand-in for the commit broker's accepted-state contract.
type fakeBroker struct {
	mu       sync.Mutex
	accepted map[string]broker.Receipt
	evidence map[string]broker.SourceEvidence
	// reports are the operability reports PublishVerifiedSource received.
	reports map[string]operability.Report
	head    int
	// root holds the proposal checkouts: the lifecycle writes plan.yaml there.
	root string
	// scratch holds the patch.uncaptured feedback returned once per entry.
	scratch []string
	// rejections is the durable rejection log of each proposal.
	rejections map[string]*contracts.ProposalRejectionLog
	// stages, accepts and discards record the candidate protocol; staleAccepts
	// makes that many Accept calls fail with ErrCandidateStale (the head moved),
	// stageErrors is consumed one per StageCandidate call.
	stages, accepts, discards int
	staleAccepts              int
	stageErrors               []error
	candidates                []broker.Candidate
	evidence2                 []broker.CandidateEvidence
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	return &fakeBroker{accepted: map[string]broker.Receipt{}, evidence: map[string]broker.SourceEvidence{}, rejections: map[string]*contracts.ProposalRejectionLog{}, root: t.TempDir()}
}

// plan reads back the plan.yaml the lifecycle wrote into proposal id.
func (b *fakeBroker) plan(t *testing.T, id string) *contracts.DeploymentPlan {
	t.Helper()
	plan, err := planfile.Read(filepath.Join(b.root, id), planfile.Catalogs{})
	if err != nil {
		t.Fatalf("plan.yaml of proposal %s: %v", id, err)
	}
	return plan
}

func (b *fakeBroker) commit() string { return fmt.Sprintf("commit-%d", b.head) }

func (b *fakeBroker) CreateProposalForPhase(_ context.Context, id string, _ broker.Phase) (broker.Proposal, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return broker.Proposal{IncidentID: id, Path: filepath.Join(b.root, id), BaseCommit: b.commit()}, nil
}

func (b *fakeBroker) accept(id string) broker.Receipt {
	b.head++
	receipt := broker.Receipt{IncidentID: id, Phase: broker.InitialPhase, Commit: b.commit()}
	b.accepted[id] = receipt
	return receipt
}

func (b *fakeBroker) Publish(_ context.Context, id string, _ broker.Actor, _ broker.Phase) (broker.Receipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.accept(id), nil
}

func (b *fakeBroker) PublishVerifiedSource(_ context.Context, id string, evidence broker.SourceEvidence, report operability.Report) (broker.Receipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	receipt := b.accept(id)
	evidence.AcceptedCommit = receipt.Commit
	b.evidence[id] = evidence
	if b.reports == nil {
		b.reports = map[string]operability.Report{}
	}
	b.reports[id] = report
	return receipt, nil
}

// OperabilityReport returns the report PublishVerifiedSource was given.
func (b *fakeBroker) OperabilityReport(_ context.Context, id string, _ operability.Stage) (operability.Report, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	report, ok := b.reports[id]
	return report, ok, nil
}

func (b *fakeBroker) SourceEvidence(_ context.Context, id string) (broker.SourceEvidence, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	evidence, ok := b.evidence[id]
	return evidence, ok, nil
}

func (b *fakeBroker) AcceptedCommit(_ context.Context, id string, _ broker.Phase) (broker.Receipt, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	receipt, ok := b.accepted[id]
	return receipt, ok, nil
}

func (b *fakeBroker) ChangedPaths(context.Context, string, broker.Phase) ([]string, error) {
	return []string{"changed"}, nil
}
func (b *fakeBroker) ScratchFeedback(context.Context, string, broker.Phase) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.scratch) == 0 {
		return "", nil
	}
	feedback := b.scratch[0]
	b.scratch = b.scratch[1:]
	return feedback, nil
}
func (b *fakeBroker) ReleaseProposal(string, broker.Phase) error { return nil }
func (b *fakeBroker) Rejections(id string) (*contracts.ProposalRejectionLog, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if log, ok := b.rejections[id]; ok {
		return log, nil
	}
	return &contracts.ProposalRejectionLog{ProposalId: id}, nil
}
func (b *fakeBroker) RecordRejection(id string, diagnostics []*contracts.MemoryDiagnostic) (*contracts.ProposalRejectionLog, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	log, ok := b.rejections[id]
	if !ok {
		log = &contracts.ProposalRejectionLog{ProposalId: id}
		b.rejections[id] = log
	}
	log.Rejections = append(log.Rejections, broker.NewProposalRejection(uint32(len(log.Rejections)+1), diagnostics))
	log.Terminal = broker.RetriesExhausted(log.Rejections)
	return log, nil
}

func (b *fakeBroker) StageCandidate(_ context.Context, id string, _ broker.Actor, phase broker.Phase) (broker.Candidate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stages++
	if len(b.stageErrors) > 0 {
		err := b.stageErrors[0]
		b.stageErrors = b.stageErrors[1:]
		if err != nil {
			return broker.Candidate{}, err
		}
	}
	candidate := broker.Candidate{IncidentID: id, Actor: broker.HealthJudge, Phase: phase, Path: filepath.Join(b.root, id), Parent: b.commit(), Commit: fmt.Sprintf("candidate-%d", b.stages), Tree: "candidate-tree"}
	b.candidates = append(b.candidates, candidate)
	return candidate, nil
}

func (b *fakeBroker) Accept(_ context.Context, candidate broker.Candidate, evidence broker.CandidateEvidence) (broker.Receipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accepts++
	b.evidence2 = append(b.evidence2, evidence)
	if b.staleAccepts > 0 {
		b.staleAccepts--
		return broker.Receipt{}, fmt.Errorf("%w: head moved", broker.ErrCandidateStale)
	}
	return b.accept(candidate.IncidentID), nil
}

func (b *fakeBroker) DiscardCandidate(string, broker.Phase) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.discards++
	return nil
}

type fakeArtifacts struct{}

// countingArtifacts counts the architectures Go writes.
type countingArtifacts struct {
	fakeArtifacts
	architectures int
}

func (a *countingArtifacts) WriteArchitecture(string, string, string, string) error {
	a.architectures++
	return nil
}

func (fakeArtifacts) WriteGoal(string, string, string, []layout.Source) error { return nil }
func (fakeArtifacts) WriteSchema(string) error                                { return nil }
func (fakeArtifacts) WriteArchitecture(string, string, string, string) error  { return nil }
func (fakeArtifacts) ReadArchitecture(string) (string, error)                 { return "architecture", nil }

// fakeWorkers answers the deployer and the judge. It records each deployer
// request, which is how a test observes attempts and correction feedback.
type fakeWorkers struct {
	mu            sync.Mutex
	deployer      []*contracts.SourceDeploymentRequest
	judgeRuns     int
	plan          *contracts.DeploymentPlan // proposed instead of the default raw plan when set
	judgeRequests []*contracts.HealthJudgeRequest
	// judgeFailures is how many judge dispatches report no authored detectors
	// before one succeeds.
	judgeFailures int
}

func (w *fakeWorkers) Execute(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var response proto.Message
	deployerRequest := &contracts.SourceDeploymentRequest{}
	if err := task.GetRequest().UnmarshalTo(deployerRequest); err == nil {
		w.deployer = append(w.deployer, deployerRequest)
		config, _ := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
		plan := &contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "raw", Config: config}, ExpectedWorkloadIds: []string{"Deployment/ns/app"}}
		if w.plan != nil {
			plan = w.plan
		}
		response = &contracts.SourceDeploymentResult{Proposed: true, AgentSessionId: "session", Plan: plan}
	} else {
		w.judgeRuns++
		judgeRequest := &contracts.HealthJudgeRequest{}
		if err := task.GetRequest().UnmarshalTo(judgeRequest); err == nil {
			w.judgeRequests = append(w.judgeRequests, judgeRequest)
		}
		response = &contracts.HealthJudgeResult{Authored: true, DetectorIds: []string{"detector"}, AgentSessionId: "session"}
		if w.judgeRuns <= w.judgeFailures {
			response = &contracts.HealthJudgeResult{Summary: "no detectors this time"}
		}
	}
	packed, err := anypb.New(response)
	if err != nil {
		return nil, err
	}
	return &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.GetTaskId(), Status: "completed", ProposalId: task.GetWorkspace().GetProposalId(), Response: packed}, nil
}

func (w *fakeWorkers) deployerAttempts() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.deployer)
}

type fakeReadiness struct {
	errors []error // consumed one per ApplyAndVerify call
	calls  int
	plans  []*contracts.DeploymentPlan
	// receipt adjusts each receipt ApplyAndVerify returns; reverify each one
	// VerifyAccepted returns.
	receipt  func(*lifecycle.DeploymentReceipt)
	reverify func(*lifecycle.DeploymentReceipt)
	// live replaces the passing live checks CheckLive returns, one result per
	// call (the last repeats); liveCalls records the checkout of each call.
	live      [][]operability.Check
	liveCalls []string
}

// CheckLive reports the live deployment checks of the staged checkout.
func (r *fakeReadiness) CheckLive(_ context.Context, checkout string, _ *contracts.DeploymentPlan, receipt lifecycle.DeploymentReceipt, _ error) ([]operability.Check, error) {
	r.liveCalls = append(r.liveCalls, checkout)
	if len(r.live) > 0 {
		checks := r.live[min(len(r.liveCalls)-1, len(r.live)-1)]
		return checks, nil
	}
	pass := func(id string) operability.Check {
		return operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass}
	}
	return []operability.Check{pass("O1.deploy"), pass("O1.ready"), pass("O2.stamps"), pass("O6.budget")}, nil
}

// ApplyAndVerify binds the plan's source to the receipt, as the deployer does.
func (r *fakeReadiness) ApplyAndVerify(_ context.Context, _ string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	r.calls++
	r.plans = append(r.plans, plan)
	if len(r.errors) > 0 {
		err := r.errors[0]
		r.errors = r.errors[1:]
		if err != nil {
			return lifecycle.DeploymentReceipt{}, err
		}
	}
	receipt := lifecycle.DeploymentReceipt{Source: proto.Clone(plan.GetSource()).(*contracts.DeploymentSource), BaseTree: "tree", EffectiveTree: "effective", PatchesDigest: "patches", ObservedRevision: "effective", RenderDigest: "render", EnvDigest: "env", Images: []string{"img"}, ResourceIDs: []string{"deployment/app"}, Ready: true}
	if r.receipt != nil {
		r.receipt(&receipt)
	}
	return receipt, nil
}

func (r *fakeReadiness) VerifyAccepted(_ context.Context, _, _ string, receipt lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	if r.reverify != nil {
		r.reverify(&receipt)
	}
	return receipt, nil
}

type fakeVerifier struct {
	verdicts []lifecycle.HealthVerdict // the last one repeats
	errors   []error
	calls    int
	// candidateRoots records the root of each VerifyCandidate call.
	candidateRoots []string
}

func (v *fakeVerifier) VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	v.candidateRoots = append(v.candidateRoots, root+"@"+commit)
	return v.Verify(ctx, commit, namespace, objective, source)
}

func (v *fakeVerifier) Verify(context.Context, string, string, string, lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	v.calls++
	if len(v.errors) > 0 {
		err := v.errors[0]
		v.errors = v.errors[1:]
		if err != nil {
			return lifecycle.HealthVerdict{}, err
		}
	}
	index := min(v.calls-1, len(v.verdicts)-1)
	return v.verdicts[index], nil
}

type fakeInstaller struct{ installs int }

func (i *fakeInstaller) Install(context.Context, string, string) error { i.installs++; return nil }

type infraError struct{ message string }

func (e infraError) Error() string               { return e.message }
func (e infraError) InfrastructureOutcome() bool { return true }

type harness struct {
	coordinator lifecycle.Coordinator
	workers     *fakeWorkers
	readiness   *fakeReadiness
	verifier    *fakeVerifier
	installer   *fakeInstaller
	waits       []time.Duration
}

func newHarness(t *testing.T, verifier *fakeVerifier) *harness {
	t.Helper()
	h := &harness{workers: &fakeWorkers{}, readiness: &fakeReadiness{}, verifier: verifier, installer: &fakeInstaller{}}
	h.coordinator = lifecycle.Coordinator{
		Broker: newFakeBroker(t), Bootstrap: fakeArtifacts{}, Registry: registrytest.BuiltIn(t),
		Workers: h.workers, Readiness: h.readiness, Verifier: verifier, Installer: h.installer,
		SourceFacts: func(context.Context, string, string) (memory.SourceFacts, error) { return memory.SourceFacts{}, nil },
		Wait:        func(_ context.Context, d time.Duration) error { h.waits = append(h.waits, d); return nil },
	}
	return h
}

func config() lifecycle.Config {
	return lifecycle.Config{OperationID: "op", Application: "app", Namespace: "ns", HealthObjective: "stay up", MaxAttempts: 3, PendingEvaluations: 4, PendingInterval: time.Second, InfrastructureTries: 3, InfrastructureBackoff: time.Second}
}

var (
	clear   = lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear, Summary: "accepted health detectors report clear"}
	pending = lifecycle.HealthVerdict{Outcome: lifecycle.VerificationPending, Summary: "accepted health detectors report pending", Detectors: []lifecycle.DetectorState{{DetectorID: "traffic-window", Status: "pending", Reason: "observation window incomplete"}}}
	firing  = lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "accepted health detectors report firing", Detectors: []lifecycle.DetectorState{{DetectorID: "workload-health", Status: "firing", Reason: "frontend has 0 ready replicas"}}}
	// judgeFiring is a detector the health judge authored tripping: the
	// designer owns the correction first.
	judgeFiring = lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "accepted health detectors report firing", Detectors: []lifecycle.DetectorState{{DetectorID: "frontend-available", Status: "firing", Reason: "frontend has 0 ready replicas"}}}
)

func TestPendingThenClearSucceedsWithoutAnotherDeployerAttempt(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{pending, pending, clear}})
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 1 || result.Attempts != 1 || h.installer.installs != 1 {
		t.Fatalf("deployer attempts=%d result attempts=%d installs=%d", h.workers.deployerAttempts(), result.Attempts, h.installer.installs)
	}
	if h.verifier.calls != 3 || len(h.waits) != 2 {
		t.Fatalf("verify calls=%d waits=%v", h.verifier.calls, h.waits)
	}
}

func TestPersistentPendingFailsNamingTheDetectorWithoutSpendingAttempts(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{pending}})
	_, err := h.coordinator.Run(context.Background(), config())
	var terminal *lifecycle.PendingVerificationError
	if !errors.As(err, &terminal) {
		t.Fatalf("error = %v, want PendingVerificationError", err)
	}
	if !strings.Contains(err.Error(), "traffic-window") || !strings.Contains(err.Error(), "observation window incomplete") {
		t.Fatalf("terminal error does not name the pending detector and reason: %v", err)
	}
	if h.workers.deployerAttempts() != 1 || h.verifier.calls != 4 || h.installer.installs != 0 {
		t.Fatalf("deployer attempts=%d verify calls=%d installs=%d", h.workers.deployerAttempts(), h.verifier.calls, h.installer.installs)
	}
}

func TestFiringVerdictReachesTheDeployerWithDetectorDetail(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{firing, clear}})
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 2 {
		t.Fatalf("deployer attempts = %d, want 2", h.workers.deployerAttempts())
	}
	feedback := h.workers.deployer[1].GetCorrectionFeedback()
	if !strings.Contains(feedback, "workload-health") || !strings.Contains(feedback, "frontend has 0 ready replicas") {
		t.Fatalf("correction feedback lacks detector detail: %q", feedback)
	}
}

// App-file edits no patch reproduces never reach a deployment: the agent gets
// the patch.uncaptured text as a correction before anything is applied.
func TestUncapturedScratchCorrectsTheDeployerBeforeDeploying(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.coordinator.Broker.(*fakeBroker).scratch = []string{"patch.uncaptured: 1 app file(s) differ: app.go"}
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 2 || !strings.Contains(h.workers.deployer[1].GetCorrectionFeedback(), "patch.uncaptured") {
		t.Fatalf("deployer requests = %v", h.workers.deployer)
	}
	if h.readiness.calls != 1 {
		t.Fatalf("apply calls = %d, want the deployment to happen only after the correction", h.readiness.calls)
	}
}

func TestTransientDeployFailureRetriesWithoutConsumingAnAttempt(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.readiness.errors = []error{fmt.Errorf("build image: %w", infraError{"could not connect to buildkitd.sock after 10 trials"}), nil}
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if h.readiness.calls != 2 || h.workers.deployerAttempts() != 1 || result.Attempts != 1 {
		t.Fatalf("apply calls=%d deployer attempts=%d result attempts=%d", h.readiness.calls, h.workers.deployerAttempts(), result.Attempts)
	}
}

func TestPersistentTransientDeployFailureIsADistinctTerminalError(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	broken := fmt.Errorf("build: %w", infraError{"buildkitd unavailable"})
	h.readiness.errors = []error{broken, broken, broken, broken}
	_, err := h.coordinator.Run(context.Background(), config())
	var terminal *lifecycle.InfrastructureError
	if !errors.As(err, &terminal) || !strings.Contains(err.Error(), "buildkitd unavailable") {
		t.Fatalf("error = %v, want InfrastructureError", err)
	}
	if h.workers.deployerAttempts() != 1 || h.readiness.calls != 3 {
		t.Fatalf("deployer attempts=%d apply calls=%d", h.workers.deployerAttempts(), h.readiness.calls)
	}
}

func TestPlanFailureStillCorrectsTheDeployer(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.readiness.errors = []error{errors.New("manifest frontend.yaml: no such namespace"), nil}
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 2 || !strings.Contains(h.workers.deployer[1].GetCorrectionFeedback(), "no such namespace") {
		t.Fatalf("deployer requests = %v", h.workers.deployer)
	}
}

func TestTransientVerificationErrorRetriesTheEvaluation(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}, errors: []error{infraError{"rollout still progressing"}}})
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.verifier.calls != 2 || h.workers.deployerAttempts() != 1 {
		t.Fatalf("verify calls=%d deployer attempts=%d", h.verifier.calls, h.workers.deployerAttempts())
	}
}

func TestPlanReachesTheDeployerAsProposed(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	kustomize, _ := anypb.New(&contracts.KustomizeRenderConfig{Path: "k"})
	h.workers.plan = &contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "kustomize", Config: kustomize}, ExpectedWorkloadIds: []string{"Deployment/ns/app"}}
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if len(h.readiness.plans) != 1 || !proto.Equal(h.readiness.plans[0], h.workers.plan) {
		t.Fatalf("deployer saw %v, want the proposed plan unchanged", h.readiness.plans)
	}
}

func TestPlanWithoutSourceFeedsBackToTheDeployer(t *testing.T) {
	legacyConfig, _ := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	for name, plan := range map[string]*contracts.DeploymentPlan{
		"strategy only": {StrategyId: "raw-manifests-v1", StrategyConfig: legacyConfig, ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		"empty":         {ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		"source and strategy": {
			Source:     &contracts.DeploymentSource{RendererId: "raw", Config: legacyConfig},
			StrategyId: "raw-manifests-v1", ExpectedWorkloadIds: []string{"Deployment/ns/app"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
			h.workers.plan = plan
			if _, err := h.coordinator.Run(context.Background(), config()); err == nil {
				t.Fatal("run succeeded with a plan that names no deployment source")
			}
			if h.readiness.calls != 0 {
				t.Fatalf("deployer called %d times for a rejected plan", h.readiness.calls)
			}
			if h.workers.deployerAttempts() < 2 {
				t.Fatalf("deployer attempts=%d, want a correction round", h.workers.deployerAttempts())
			}
			if feedback := h.workers.deployer[1].GetCorrectionFeedback(); !strings.Contains(feedback, "source") {
				t.Fatalf("feedback %q does not ask for a deployment source", feedback)
			}
		})
	}
}
