// Package lifecycle owns the accepted operation sequence. Agent workers only
// propose role-scoped changes; the broker decides which edits become durable.
package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/worker_registry"
)

var ErrAgentRegistration = errors.New("lifecycle agent registration")

// ProposalBroker is the only authority that can publish an agent proposal.
type ProposalBroker interface {
	CreateProposalForPhase(context.Context, string, broker.Phase) (broker.Proposal, error)
	Publish(context.Context, string, broker.Actor, broker.Phase) (broker.Receipt, error)
	PublishVerifiedSource(context.Context, string, broker.SourceEvidence) (broker.Receipt, error)
	SourceEvidence(context.Context, string) (broker.SourceEvidence, bool, error)
	AcceptedCommit(context.Context, string, broker.Phase) (broker.Receipt, bool, error)
	ChangedPaths(context.Context, string, broker.Phase) ([]string, error)
	ReleaseProposal(string, broker.Phase) error
	// Rejections and RecordRejection are the durable retry state for a
	// proposal rejected by validator diagnostics.
	Rejections(string) (*contracts.ProposalRejectionLog, error)
	RecordRejection(string, []*contracts.MemoryDiagnostic) (*contracts.ProposalRejectionLog, error)
}

// ArtifactWriter is the storage edge for role-owned bootstrap artifacts.
// The coordinator does not know their physical paths or serialization.
type ArtifactWriter interface {
	WriteGoal(checkout, application, objective string) error
	WriteSchema(checkout string) error
	WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary string) error
	ReadArchitecture(checkout string) (string, error)
}

type WorkerExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

// DeploymentReceipt binds an independently observed rollout to the exact
// candidate source tree. Empty or incomplete evidence cannot be accepted.
type DeploymentReceipt struct {
	ContainerImages    []rendered.DeployedImage
	ProposalTreeDigest string
	ObservedRevision   string
	StrategyID         string
	Images             []string
	ResourceIDs        []string
	ManifestPaths      []string
	Ready              bool
}

// SourceReadinessVerifier is a trusted Go build/apply/observe adapter. The
// agent's plan is only input; it cannot attest its own deployment.
type SourceReadinessVerifier interface {
	ApplyAndVerify(context.Context, string, *contracts.DeploymentPlan) (DeploymentReceipt, error)
	VerifyAccepted(context.Context, string, string, DeploymentReceipt) (DeploymentReceipt, error)
}

// HealthVerifier judges the accepted detector against the live application.
// Its verdict is independent of the agent's HealthJudgeResult assertion.
type HealthVerifier interface {
	Verify(ctx context.Context, acceptedCommit, namespace, objective string, source DeploymentReceipt) (healthy bool, feedback string, err error)
}

type RuntimeInstaller interface {
	Install(ctx context.Context, acceptedCommit, namespace string) error
}

type deploymentPlanInstaller interface {
	InstallDeployment(context.Context, string, string, DeploymentReceipt) error
}

type Coordinator struct {
	Broker    ProposalBroker
	Bootstrap ArtifactWriter
	Registry  *worker_registry.Registry
	Workers   WorkerExecutor
	Readiness SourceReadinessVerifier
	Verifier  HealthVerifier
	Installer RuntimeInstaller
	// SourceFacts reads a trusted projection from the accepted source commit.
	// The default implementation is the operational-memory storage edge.
	SourceFacts func(context.Context, string, string) (memory.SourceFacts, error)
}

type Config struct {
	OperationID, Application, Namespace, HealthObjective string
	RegistryHost                                         string
	DeployerAgentID, ArchitectureAgentID, JudgeAgentID   string
	MaxAttempts                                          int
}

type Result struct {
	GoalCommit, SchemaCommit, SourceCommit, ArchitectureCommit, JudgeCommit string
	Attempts                                                                int
}

func (c Coordinator) Run(ctx context.Context, cfg Config) (Result, error) {
	if err := c.validate(cfg); err != nil {
		return Result{}, err
	}
	var result Result
	goal, err := c.bootstrap(ctx, cfg.OperationID+"-goal", broker.Human, func(checkout string) error {
		return c.Bootstrap.WriteGoal(checkout, cfg.Application, cfg.HealthObjective)
	})
	if err != nil {
		return result, fmt.Errorf("publish human goal: %w", err)
	}
	result.GoalCommit = goal.Commit
	schema, err := c.bootstrap(ctx, cfg.OperationID+"-schema", broker.Upkeep, c.Bootstrap.WriteSchema)
	if err != nil {
		return result, fmt.Errorf("publish operator schema: %w", err)
	}
	result.SchemaCommit = schema.Commit
	feedback := ""
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		result.Attempts = attempt
		deployerID := fmt.Sprintf("%s-source-%d", cfg.OperationID, attempt)
		sourceReceipt, deployment, sourceReady, nextFeedback, err := c.deploy(ctx, deployerID, cfg, attempt, feedback)
		if err != nil {
			return result, fmt.Errorf("deployment attempt %d: %w", attempt, err)
		}
		if !sourceReady {
			feedback = nextFeedback
			continue
		}
		result.SourceCommit = sourceReceipt.Commit
		architectureID := fmt.Sprintf("%s-architecture-%d", cfg.OperationID, attempt)
		architectureReceipt, architectureReady, nextFeedback, err := c.architecture(ctx, architectureID, cfg, attempt, sourceReceipt.Commit, feedback)
		if err != nil {
			return result, fmt.Errorf("architecture attempt %d: %w", attempt, err)
		}
		if !architectureReady {
			feedback = nextFeedback
			continue
		}
		result.ArchitectureCommit = architectureReceipt.Commit
		judgeID := fmt.Sprintf("%s-judge-%d", cfg.OperationID, attempt)
		judgeReceipt, judgeReady, nextFeedback, err := c.judge(ctx, judgeID, cfg, attempt, sourceReceipt.Commit, architectureReceipt.Commit, feedback)
		if err != nil {
			return result, fmt.Errorf("health judge attempt %d: %w", attempt, err)
		}
		if !judgeReady {
			feedback = nextFeedback
			continue
		}
		result.JudgeCommit = judgeReceipt.Commit
		healthy, finding, err := c.Verifier.Verify(ctx, judgeReceipt.Commit, cfg.Namespace, cfg.HealthObjective, deployment)
		if err != nil {
			return result, fmt.Errorf("independent verification: %w", err)
		}
		if !healthy {
			feedback = finding
			continue
		}
		install := func() error {
			if typed, ok := c.Installer.(deploymentPlanInstaller); ok {
				return typed.InstallDeployment(ctx, judgeReceipt.Commit, cfg.Namespace, deployment)
			}
			return c.Installer.Install(ctx, judgeReceipt.Commit, cfg.Namespace)
		}
		if err := install(); err != nil {
			return result, fmt.Errorf("install controller: %w", err)
		}
		return result, nil
	}
	return result, fmt.Errorf("deployment exhausted %d attempts; last rejection: %s", cfg.MaxAttempts, feedback)
}

// MigrateMemory has the Upkeep actor bring an already-bootstrapped repository's
// operational memory to the current schema. It is the same step Run performs
// first, so an interrupted migration resumes by operation ID and an up-to-date
// repository is left untouched.
func (c Coordinator) MigrateMemory(ctx context.Context, operationID string) (broker.Receipt, error) {
	if c.Broker == nil || c.Bootstrap == nil || strings.TrimSpace(operationID) == "" {
		return broker.Receipt{}, errors.New("broker, artifact store, and operation ID are required")
	}
	receipt, err := c.bootstrap(ctx, operationID+"-schema", broker.Upkeep, c.Bootstrap.WriteSchema)
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("migrate operational memory: %w", err)
	}
	return receipt, nil
}

func (c Coordinator) validate(cfg Config) error {
	if c.Broker == nil || c.Bootstrap == nil || c.Registry == nil || c.Workers == nil || c.Readiness == nil || c.Verifier == nil || c.Installer == nil {
		return errors.New("broker, artifact store, registry, workers, source readiness, health verifier, and installer are required")
	}
	if cfg.OperationID == "" || cfg.Application == "" || cfg.Namespace == "" || strings.TrimSpace(cfg.HealthObjective) == "" || cfg.MaxAttempts < 1 {
		return errors.New("operation ID, application, namespace, objective, and positive max attempts are required")
	}
	for _, choice := range []struct {
		id      string
		request proto.Message
	}{
		{cfg.DeployerAgentID, &contracts.SourceDeploymentRequest{}},
		{cfg.ArchitectureAgentID, &contracts.ArchitectureRequest{}},
		{cfg.JudgeAgentID, &contracts.HealthJudgeRequest{}},
	} {
		if _, _, err := c.target(choice.id, choice.request); err != nil {
			return err
		}
	}
	return nil
}

// target resolves the agent and task type that serve request. An empty
// configured ID selects the one registered agent declaring the request's
// message type, so neither task names nor built-in IDs live in Go.
func (c Coordinator) target(configured string, request proto.Message) (agentID, taskType string, err error) {
	agentID = configured
	if agentID == "" {
		if agentID, err = c.Registry.AgentFor(request); err != nil {
			return "", "", fmt.Errorf("%w: %v", ErrAgentRegistration, err)
		}
	}
	if taskType, err = c.Registry.TaskType(agentID, request); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrAgentRegistration, err)
	}
	return agentID, taskType, nil
}

func (c Coordinator) bootstrap(ctx context.Context, id string, actor broker.Actor, write func(string) error) (broker.Receipt, error) {
	if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
		return broker.Receipt{}, err
	} else if ok {
		return prior, nil
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, err
	}
	if err := write(proposal.Path); err != nil {
		return broker.Receipt{}, err
	}
	paths, err := c.Broker.ChangedPaths(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, err
	}
	if len(paths) == 0 {
		if err := c.Broker.ReleaseProposal(id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, err
		}
		return broker.Receipt{IncidentID: id, Phase: broker.InitialPhase, Commit: proposal.BaseCommit}, nil
	}
	return c.Broker.Publish(ctx, id, actor, broker.InitialPhase)
}

func (c Coordinator) deploy(ctx context.Context, id string, cfg Config, attempt int, feedback string) (broker.Receipt, DeploymentReceipt, bool, string, error) {
	if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, false, "", err
	} else if ok {
		evidence, found, err := c.Broker.SourceEvidence(ctx, id)
		if err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", fmt.Errorf("load broker source evidence: %w", err)
		}
		if !found {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", errors.New("accepted source lacks broker-validated deployment evidence")
		}
		if evidence.AcceptedCommit != prior.Commit {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", errors.New("source evidence belongs to another accepted commit")
		}
		recorded := deploymentFromBroker(evidence)
		readiness, err := c.Readiness.VerifyAccepted(ctx, prior.Commit, cfg.Namespace, recorded)
		if err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", fmt.Errorf("verify accepted source: %w", err)
		}
		if !completeDeploymentReceipt(readiness) || !sameDeploymentIdentity(recorded, readiness) {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", errors.New("accepted source has incomplete or changed deployment evidence")
		}
		return prior, readiness, true, "", nil
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, false, "", err
	}
	request := &contracts.SourceDeploymentRequest{Application: cfg.Application, Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, CorrectionFeedback: feedback, Attempt: uint32(attempt), RegistryHost: cfg.RegistryHost}
	task, err := c.newTask(id, cfg.DeployerAgentID, proposal, request)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, false, "", err
	}
	response := &contracts.SourceDeploymentResult{}
	if err := c.execute(ctx, task, response); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		if ctx.Err() != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", ctx.Err()
		}
		return broker.Receipt{}, DeploymentReceipt{}, false, err.Error(), nil
	}
	if !response.GetProposed() || strings.TrimSpace(response.GetAgentSessionId()) == "" ||
		response.GetPlan() == nil || response.GetPlan().GetStrategyId() == "" || response.GetPlan().GetStrategyConfig() == nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "deployment worker did not propose a complete source plan: " + response.GetSummary(), nil
	}
	readiness, err := c.Readiness.ApplyAndVerify(ctx, proposal.Path, response.GetPlan())
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent source deployment failed: " + err.Error(), nil
	}
	if !completeDeploymentReceipt(readiness) {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent source deployment returned incomplete evidence", nil
	}
	if readiness.StrategyID != response.GetPlan().GetStrategyId() {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent deployment strategy differs from proposal", nil
	}
	receipt, err := c.Broker.PublishVerifiedSource(ctx, id, deploymentToBroker(readiness))
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "broker rejected deployment: " + err.Error(), nil
	}
	return receipt, readiness, true, "", nil
}

func completeDeploymentReceipt(receipt DeploymentReceipt) bool {
	return receipt.Ready && receipt.ProposalTreeDigest != "" && receipt.ObservedRevision != "" && receipt.StrategyID != "" && len(receipt.Images) > 0 && len(receipt.ResourceIDs) > 0
}

func sameDeploymentIdentity(left, right DeploymentReceipt) bool {
	if left.ProposalTreeDigest != right.ProposalTreeDigest || left.ObservedRevision != right.ObservedRevision || left.StrategyID != right.StrategyID {
		return false
	}
	leftImages, rightImages := slices.Clone(left.Images), slices.Clone(right.Images)
	leftResources, rightResources := slices.Clone(left.ResourceIDs), slices.Clone(right.ResourceIDs)
	leftPaths, rightPaths := slices.Clone(left.ManifestPaths), slices.Clone(right.ManifestPaths)
	slices.Sort(leftImages)
	slices.Sort(rightImages)
	slices.Sort(leftResources)
	slices.Sort(rightResources)
	slices.Sort(leftPaths)
	slices.Sort(rightPaths)
	return reflect.DeepEqual(left.ContainerImages, right.ContainerImages) && slices.Equal(leftImages, rightImages) && slices.Equal(leftResources, rightResources) && slices.Equal(leftPaths, rightPaths)
}

func deploymentToBroker(receipt DeploymentReceipt) broker.SourceEvidence {
	return broker.SourceEvidence{ContainerImages: append([]rendered.DeployedImage(nil), receipt.ContainerImages...), ProposalTreeDigest: receipt.ProposalTreeDigest, ObservedRevision: receipt.ObservedRevision, StrategyID: receipt.StrategyID, Images: append([]string(nil), receipt.Images...), ResourceIDs: append([]string(nil), receipt.ResourceIDs...), ManifestPaths: append([]string(nil), receipt.ManifestPaths...), Ready: receipt.Ready}
}

func deploymentFromBroker(evidence broker.SourceEvidence) DeploymentReceipt {
	return DeploymentReceipt{ContainerImages: append([]rendered.DeployedImage(nil), evidence.ContainerImages...), ProposalTreeDigest: evidence.ProposalTreeDigest, ObservedRevision: evidence.ObservedRevision, StrategyID: evidence.StrategyID, Images: append([]string(nil), evidence.Images...), ResourceIDs: append([]string(nil), evidence.ResourceIDs...), ManifestPaths: append([]string(nil), evidence.ManifestPaths...), Ready: evidence.Ready}
}

func (c Coordinator) architecture(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, feedback string) (broker.Receipt, bool, string, error) {
	if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
		return broker.Receipt{}, false, "", err
	} else if ok {
		return prior, true, "", nil
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, false, "", err
	}
	if proposal.BaseCommit != sourceCommit {
		return broker.Receipt{}, false, "", fmt.Errorf("architecture proposal base %q differs from accepted source %q", proposal.BaseCommit, sourceCommit)
	}
	readSourceFacts := c.SourceFacts
	if readSourceFacts == nil {
		readSourceFacts = memory.SourceInventory
	}
	facts, err := readSourceFacts(ctx, proposal.Path, sourceCommit)
	if err != nil {
		return broker.Receipt{}, false, "", fmt.Errorf("read accepted source inventory for architecture: %w", err)
	}
	if len(facts.Fingerprint) != 64 {
		return broker.Receipt{}, false, "", errors.New("accepted source inventory omitted topology fingerprint")
	}
	request := &contracts.ArchitectureRequest{Application: cfg.Application, AcceptedSourceCommit: sourceCommit, CorrectionFeedback: feedback, Attempt: uint32(attempt), SourceResources: facts.Resources, SourceTopologyFingerprint: facts.Fingerprint}
	task, err := c.newTask(id, cfg.ArchitectureAgentID, proposal, request)
	if err != nil {
		return broker.Receipt{}, false, "", err
	}
	response := &contracts.ArchitectureResult{}
	if err := c.execute(ctx, task, response); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		if ctx.Err() != nil {
			return broker.Receipt{}, false, "", ctx.Err()
		}
		return broker.Receipt{}, false, err.Error(), nil
	}
	if response.GetSourceCommit() != sourceCommit || strings.TrimSpace(response.GetSummaryMarkdown()) == "" || response.GetTopologyFingerprint() != facts.Fingerprint || response.GetAgentSessionId() == "" {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, false, "architecture worker returned incomplete or stale source evidence", nil
	}
	if err := c.Bootstrap.WriteArchitecture(proposal.Path, cfg.Application, sourceCommit, response.GetTopologyFingerprint(), response.GetSummaryMarkdown()); err != nil {
		return broker.Receipt{}, false, "", err
	}
	receipt, err := c.Broker.Publish(ctx, id, broker.Deployer, broker.InitialPhase)
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, false, "broker rejected architecture: " + err.Error(), nil
	}
	return receipt, true, "", nil
}

// judge dispatches the health judge until the broker accepts its diagnostics
// or the validation retry bound is spent. Every broker rejection is recorded
// durably, with its typed diagnostics, before the checkout is released, so the
// attempt number derived from that log survives a restart.
func (c Coordinator) judge(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string) (broker.Receipt, bool, string, error) {
	for {
		if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", err
		} else if ok {
			return prior, true, "", nil
		}
		log, err := c.Broker.Rejections(id)
		if err != nil {
			return broker.Receipt{}, false, "", err
		}
		if log.GetTerminal() {
			last := log.GetRejections()[len(log.GetRejections())-1]
			return broker.Receipt{}, false, fmt.Sprintf("broker rejected health diagnostics %d times; last rejection: %s", len(log.GetRejections()), describeDiagnostics(last.GetDiagnostics())), nil
		}
		receipt, ready, nextFeedback, rejected, err := c.judgeOnce(ctx, id, cfg, attempt, sourceCommit, architectureCommit, feedback, log)
		if err != nil || ready || rejected == nil {
			return receipt, ready, nextFeedback, err
		}
		if _, err := c.Broker.RecordRejection(id, rejected); err != nil {
			return broker.Receipt{}, false, "", err
		}
		if err := c.Broker.ReleaseProposal(id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", err
		}
	}
}

func describeDiagnostics(diagnostics []*contracts.MemoryDiagnostic) string {
	parts := make([]string, 0, len(diagnostics))
	for _, d := range diagnostics {
		parts = append(parts, fmt.Sprintf("[%s] %s: %s", d.GetRule(), d.GetPath(), d.GetMessage()))
	}
	return strings.Join(parts, "; ")
}

// judgeOnce runs one judge dispatch. A non-nil rejected slice means the broker
// rejected the proposal with typed diagnostics and the caller owns the retry.
func (c Coordinator) judgeOnce(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string, log *contracts.ProposalRejectionLog) (broker.Receipt, bool, string, []*contracts.MemoryDiagnostic, error) {
	validationAttempt := uint32(len(log.GetRejections()) + 1)
	var previous []*contracts.MemoryDiagnostic
	if n := len(log.GetRejections()); n > 0 {
		previous = log.GetRejections()[n-1].GetDiagnostics()
		// A crash may have left the rejected checkout behind.
		if err := c.Broker.ReleaseProposal(id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", nil, err
		}
	}
	fail := func(feedback string) (broker.Receipt, bool, string, []*contracts.MemoryDiagnostic, error) {
		return broker.Receipt{}, false, feedback, nil, nil
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, false, "", nil, err
	}
	if proposal.BaseCommit != architectureCommit {
		return broker.Receipt{}, false, "", nil, fmt.Errorf("judge proposal base %q differs from accepted architecture %q", proposal.BaseCommit, architectureCommit)
	}
	architectureSummary, err := c.Bootstrap.ReadArchitecture(proposal.Path)
	if err != nil {
		return broker.Receipt{}, false, "", nil, err
	}
	readSourceFacts := c.SourceFacts
	if readSourceFacts == nil {
		readSourceFacts = memory.SourceInventory
	}
	facts, err := readSourceFacts(ctx, proposal.Path, sourceCommit)
	if err != nil {
		return broker.Receipt{}, false, "", nil, fmt.Errorf("read accepted source inventory: %w", err)
	}
	if len(facts.Fingerprint) != 64 {
		return broker.Receipt{}, false, "", nil, errors.New("accepted source inventory omitted topology fingerprint")
	}
	request := &contracts.HealthJudgeRequest{Application: cfg.Application, Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, AcceptedSourceCommit: sourceCommit, CorrectionFeedback: feedback, Attempt: uint32(attempt), ArchitectureSummaryMarkdown: architectureSummary, SourceResources: facts.Resources, SourceTopologyFingerprint: facts.Fingerprint, ValidationAttempt: validationAttempt, PreviousDiagnostics: previous}
	taskID := id
	if validationAttempt > 1 {
		// A retry is a new task: its Job and request must not collide with the
		// rejected attempt's.
		taskID = fmt.Sprintf("%s-retry%d", id, validationAttempt-1)
	}
	task, err := c.newTask(taskID, cfg.JudgeAgentID, proposal, request)
	if err != nil {
		return broker.Receipt{}, false, "", nil, err
	}
	response := &contracts.HealthJudgeResult{}
	if err := c.execute(ctx, task, response); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		if ctx.Err() != nil {
			return broker.Receipt{}, false, "", nil, ctx.Err()
		}
		return fail(err.Error())
	}
	expectedDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSpace(cfg.HealthObjective))))
	if !response.GetAuthored() || len(response.GetDetectorIds()) == 0 ||
		strings.TrimSpace(response.GetAgentSessionId()) == "" || response.GetObjectiveDigest() != expectedDigest {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail("health judge did not author detectors: " + response.GetSummary())
	}
	if response.GetArtifactProposal() != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail("direct-edit health judge rejects deprecated artifact_proposal")
	}
	receipt, err := c.Broker.Publish(ctx, id, broker.HealthJudge, broker.InitialPhase)
	if err != nil {
		if diagnostics, typed := broker.RejectionDiagnostics(err); typed {
			return broker.Receipt{}, false, "", diagnostics, nil
		}
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail("broker rejected health diagnostics: " + err.Error())
	}
	return receipt, true, "", nil, nil
}

func (c Coordinator) newTask(id, configuredAgent string, proposal broker.Proposal, request proto.Message) (*contracts.AgentTask, error) {
	agentID, taskType, err := c.target(configuredAgent, request)
	if err != nil {
		return nil, err
	}
	packed, err := anypb.New(request)
	if err != nil {
		return nil, err
	}
	return &contracts.AgentTask{SchemaVersion: worker_registry.SchemaVersion, TaskId: id, AgentId: agentID, TaskType: taskType, Request: packed, Workspace: &contracts.ProposalWorkspace{ProposalId: proposal.IncidentID, CheckoutPath: proposal.Path, BaseCommit: proposal.BaseCommit}}, nil
}

func (c Coordinator) execute(ctx context.Context, task *contracts.AgentTask, response proto.Message) error {
	if err := c.Registry.ValidateTask(task); err != nil {
		return err
	}
	result, err := c.Workers.Execute(ctx, task)
	if err != nil {
		return err
	}
	if err := c.Registry.ValidateResult(task, result); err != nil {
		return err
	}
	if result.GetStatus() != "completed" {
		return fmt.Errorf("worker %s: %s", result.GetStatus(), result.GetErrorSummary())
	}
	return result.GetResponse().UnmarshalTo(response)
}
