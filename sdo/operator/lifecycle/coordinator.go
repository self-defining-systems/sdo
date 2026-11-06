// Package lifecycle owns the accepted operation sequence. Agent workers only
// propose role-scoped changes; the broker decides which edits become durable.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/plugindesc"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/worker_registry"
)

var ErrAgentRegistration = errors.New("lifecycle agent registration")

// ProposalBroker is the only authority that can publish an agent proposal.
type ProposalBroker interface {
	CreateProposalForPhase(context.Context, string, broker.Phase) (broker.Proposal, error)
	Publish(context.Context, string, broker.Actor, broker.Phase) (broker.Receipt, error)
	// PublishVerifiedSource accepts the deployed source with its stage-deployment
	// operability report; the broker recomputes the report's binding and decision.
	PublishVerifiedSource(context.Context, string, broker.SourceEvidence, operability.Report) (broker.Receipt, error)
	// OperabilityReport reads back the verified report of an accepted commit.
	OperabilityReport(context.Context, string, operability.Stage) (operability.Report, bool, error)
	SourceEvidence(context.Context, string) (broker.SourceEvidence, bool, error)
	AcceptedCommit(context.Context, string, broker.Phase) (broker.Receipt, bool, error)
	// AcceptedSourceEvidence is the deployment of the accepted HEAD.
	AcceptedSourceEvidence(context.Context) (broker.AcceptedSource, bool, error)
	ChangedPaths(context.Context, string, broker.Phase) ([]string, error)
	// ScratchFeedback is the patch.uncaptured correction text for a checkout
	// whose app-file edits no patch reproduces, or "" when consistent.
	ScratchFeedback(context.Context, string, broker.Phase) (string, error)
	ReleaseProposal(string, broker.Phase) error
	// StageCandidate, Accept and DiscardCandidate split publication of the
	// health judge's proposal: the broker validates and stages the commit
	// without moving the accepted head, the coordinator verifies the staged
	// checkout, and only an accepted decision is fast-forwarded.
	StageCandidate(context.Context, string, broker.Actor, broker.Phase) (broker.Candidate, error)
	Accept(context.Context, broker.Candidate, broker.CandidateEvidence) (broker.Receipt, error)
	DiscardCandidate(string, broker.Phase) error
	// CheckBase validates the untouched accepted base of a proposal with the
	// shared validator and returns the diagnostics an agent edit could not be
	// blamed for (the empty-diff diagnostic is excluded).
	CheckBase(context.Context, string, broker.Actor, broker.Phase) ([]*contracts.MemoryDiagnostic, error)
	// Rejections and RecordRejection are the durable retry state for a
	// proposal rejected by validator diagnostics.
	Rejections(string) (*contracts.ProposalRejectionLog, error)
	RecordRejection(string, []*contracts.MemoryDiagnostic) (*contracts.ProposalRejectionLog, error)
}

// ArtifactWriter is the storage edge for role-owned bootstrap artifacts.
// The coordinator does not know their physical paths or serialization.
type ArtifactWriter interface {
	WriteGoal(checkout, application, objective string, sources []layout.Source) error
	WriteSchema(checkout string) error
	WriteArchitecture(checkout, application, acceptedSourceCommit, summary string) error
	ReadArchitecture(checkout string) (string, error)
}

type WorkerExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

// VanishedDiscarder lets the owner of a worker's durable evidence forget a
// dispatch whose Job disappeared without a result.
const vanishedWorkerRule = "worker.job_vanished"

// jobFailedRule is the rejection recorded for an agent Job that failed.
const jobFailedRule = "worker.job_failed"

type VanishedDiscarder interface {
	DiscardVanished(context.Context, *contracts.AgentTask) error
}

// DeploymentReceipt binds an independently observed rollout to the exact
// candidate source tree, the DeploymentSource that rendered it, and the digests
// of what that source rendered to (canonical desired objects and install
// environment). Empty or incomplete evidence cannot be accepted.
type DeploymentReceipt struct {
	ContainerImages []rendered.DeployedImage
	// Source is the DeploymentSource the deployment rendered; opaque here.
	Source *contracts.DeploymentSource
	// BaseTree is the accepted commit's tree (broker.ProposalTreeDigest).
	BaseTree string
	// EffectiveTree is the ID of the tree that was rendered and built:
	// BaseTree with the deployment patch series applied and the .sdo/
	// memory projected away (rendered.Effective). PatchesDigest is the digest
	// of that series. The pod-template source stamp is EffectiveTree, so
	// ObservedRevision (read back from the cluster) equals it.
	EffectiveTree    string
	PatchesDigest    string
	ObservedRevision string
	// RenderDigest and EnvDigest are rendered.DesiredSet's digests at BaseTree.
	RenderDigest string
	EnvDigest    string
	// Environment is the install-time render environment the digests were
	// computed with (renderenv.Discover); nil is unknown (renderer defaults),
	// as in evidence recorded before discovery existed.
	Environment *contracts.RenderEnvironment
	// Policy is the install-time deployment policy the deployment was checked
	// against (recorded for audit; not part of restart identity). Nil is none.
	Policy      *contracts.DeploymentPolicy
	Images      []string
	ResourceIDs []string
	Ready       bool
}

// SourceReadinessVerifier is a trusted Go build/apply/observe adapter. The
// agent's plan is only input; it cannot attest its own deployment.
type SourceReadinessVerifier interface {
	ApplyAndVerify(context.Context, string, *contracts.DeploymentPlan) (DeploymentReceipt, error)
	VerifyAccepted(context.Context, string, string, DeploymentReceipt) (DeploymentReceipt, error)
}

// SourceReadiness is what the Coordinator needs of its deployer: apply and
// verify a deployment, and compute the live operability checks of its receipt.
// A single interface lets the compiler refuse a verifier that cannot check live.
type SourceReadiness interface {
	SourceReadinessVerifier
	LiveChecker
}

// HealthVerifier judges the accepted detector against the live application.
// Its verdict is independent of the agent's HealthJudgeResult assertion.
type HealthVerifier interface {
	CandidateVerifier
	Verify(ctx context.Context, acceptedCommit, namespace, objective string, source DeploymentReceipt) (HealthVerdict, error)
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
	Readiness SourceReadiness
	Verifier  HealthVerifier
	Installer RuntimeInstaller
	// TelemetryApply deploys the designer's telemetry backends between the
	// judge and the independent verification; nil skips the phase.
	TelemetryApply TelemetryApplier
	// SourceFacts reads a trusted projection from the accepted source commit.
	// The default renders the accepted source through Renderers (the same
	// desired-state computation deploy and drift use), so a Helm chart or a
	// Kustomize overlay has the workloads a raw manifest would. Tests inject
	// a fixed projection.
	SourceFacts func(context.Context, string, string) (memory.SourceFacts, error)
	// Renderers and ImageSources are the deployer's own catalogs. The deployer
	// request describes exactly these, so the agent is offered what the
	// deployer can deploy and nothing else. Nil offers none.
	Renderers    *render.Catalog
	ImageSources *imagesrc.Catalog
	// Wait pauses between bounded re-evaluations and infrastructure retries;
	// nil waits on a timer.
	Wait func(context.Context, time.Duration) error
}

type Config struct {
	// Telemetry is the environment's telemetry context for the judge.
	Telemetry                                            *contracts.TelemetryContext
	OperationID, Application, Namespace, HealthObjective string
	RegistryHost                                         string
	// Sources are the upstream sources to follow, published with the human
	// goal. They require a health objective, because the goal commit carries them.
	Sources                       []layout.Source
	DeployerAgentID, JudgeAgentID string
	// ProvidedPlan is a deployment the human or harness supplies (an
	// application that ships its own deployment). The deployer agent is not
	// dispatched: the plan goes through the same admission, trusted Deployer,
	// evidence and verification as an agent's plan, and any failure of it is
	// terminal (*ProvidedPlanError), never fed back to an agent.
	ProvidedPlan *contracts.DeploymentPlan
	// DeploymentSource and RenderEnvironment tell RefreshHealth what to
	// render: the accepted checkout's plan source and the recorded install
	// environment. Run takes both from the deployment receipt instead.
	DeploymentSource  *contracts.DeploymentSource
	RenderEnvironment *contracts.RenderEnvironment
	MaxAttempts       int
	// DesignerAttempts is how many operability corrections the health judge
	// gets for its own detectors before a detector set that still fires
	// unchanged is attributed to the application; zero selects 2.
	DesignerAttempts int
	// RefreshScope names who runs a health refresh. A refresh's publication
	// (proposal checkout, judge Job, rejection log) is identified by its
	// inputs and by this scope, so the controller child's start-up refresh and
	// the refresh an upstream advance runs never share one publication: a child
	// restarting while an advance's refresh is in flight cannot attach to, or
	// recreate the checkout of, the other's task. Empty is the controller's own
	// start-up refresh.
	RefreshScope string
	// MaxAttempts bounds deployer correction attempts only. The fields below
	// bound the waits that never consume one; zero selects the default.
	PendingEvaluations    int
	PendingInterval       time.Duration
	InfrastructureTries   int
	InfrastructureBackoff time.Duration
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
	if strings.TrimSpace(cfg.HealthObjective) != "" {
		goal, err := c.bootstrap(ctx, cfg.OperationID+"-goal", broker.Human, func(checkout string) error {
			return c.Bootstrap.WriteGoal(checkout, cfg.Application, cfg.HealthObjective, cfg.Sources)
		})
		if err != nil {
			return result, fmt.Errorf("publish human goal: %w", err)
		}
		result.GoalCommit = goal.Commit
	}
	schema, err := c.bootstrap(ctx, cfg.OperationID+"-schema", broker.Upkeep, c.Bootstrap.WriteSchema)
	if err != nil {
		return result, fmt.Errorf("publish operator schema: %w", err)
	}
	result.SchemaCommit = schema.Commit
	feedback := ""
	var tracker feedbackTracker
	lastRole := operability.OwnerDeployer
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		result.Attempts = attempt
		deployerID := fmt.Sprintf("%s-source-%d", cfg.OperationID, attempt)
		if cfg.ProvidedPlan != nil {
			// One deployment for the whole operation: later rounds (a failed
			// judge, say) resume the accepted source instead of redeploying.
			deployerID = fmt.Sprintf("%s-source-1", cfg.OperationID)
		}
		sourceReceipt, deployment, sourceReady, nextFeedback, err := c.deploy(ctx, deployerID, cfg, attempt, feedback)
		if err != nil {
			if opErr, ok := asOperabilityError(err); ok && cfg.ProvidedPlan != nil {
				return result, &ProvidedPlanError{Stage: "deploy", Reason: opErr.Decision.Feedback(), Report: &opErr.Report}
			}
			return result, fmt.Errorf("deployment attempt %d: %w", attempt, err)
		}
		if !sourceReady {
			if cfg.ProvidedPlan != nil {
				return result, &ProvidedPlanError{Stage: "deploy", Reason: nextFeedback}
			}
			feedback, lastRole = nextFeedback, operability.OwnerDeployer
			if err := tracker.repeated(lastRole, result.SchemaCommit, feedback); err != nil {
				return result, err
			}
			continue
		}
		result.SourceCommit = sourceReceipt.Commit
		// The architecture belongs to the accepted source, not to the attempt:
		// a later attempt over the same source (a failed judge, a provided
		// plan) resumes the accepted architecture instead of proposing
		// another one against a head that has already moved past the source.
		architectureID := fmt.Sprintf("%s-architecture-%s", cfg.OperationID, shortCommit(sourceReceipt.Commit))
		architectureReceipt, architectureReady, nextFeedback, err := c.architecture(ctx, architectureID, cfg, sourceReceipt.Commit, deployment)
		if err != nil {
			return result, fmt.Errorf("architecture attempt %d: %w", attempt, err)
		}
		if !architectureReady {
			feedback = nextFeedback
			continue
		}
		result.ArchitectureCommit = architectureReceipt.Commit
		judgeID := fmt.Sprintf("%s-judge-%d", cfg.OperationID, attempt)
		// A judge commit accepted by an earlier run was verified before it was
		// accepted; only a crash between acceptance and install gets here.
		_, resumed, err := c.Broker.AcceptedCommit(ctx, judgeID, broker.InitialPhase)
		if err != nil {
			return result, err
		}
		judgeReceipt, judgeReady, nextFeedback, err := c.judge(ctx, judgeID, cfg, attempt, sourceReceipt.Commit, architectureReceipt.Commit, feedback, deployment)
		if err != nil {
			if isTerminalOperability(err) {
				return result, err
			}
			return result, fmt.Errorf("health judge attempt %d: %w", attempt, err)
		}
		if !judgeReady {
			feedback, lastRole = nextFeedback, operability.OwnerDesigner
			if err := tracker.repeated(lastRole, result.ArchitectureCommit, feedback); err != nil {
				return result, err
			}
			continue
		}
		result.JudgeCommit = judgeReceipt.Commit
		if resumed {
			if err := c.verifyResumed(ctx, cfg, judgeReceipt.Commit, deployment); err != nil {
				return result, err
			}
		}
		if err := c.exposeOperability(ctx, judgeID, cfg); err != nil {
			return result, err
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
	return result, NewAgentNotConvergingError(lastRole, result.SchemaCommit, []string{fmt.Sprintf("exhausted %d attempts; last rejection: %s", cfg.MaxAttempts, operability.Clip(feedback))}, true)
}

func isTerminalOperability(err error) bool {
	var provided *ProvidedPlanError
	_, offlineGate := asOperabilityError(err)
	return offlineGate || errors.As(err, &provided)
}

// verifyResumed re-verifies a judge commit that was accepted before this run
// started. Nothing can correct an accepted commit, so anything but a clear
// verdict ends the run.
func (c Coordinator) verifyResumed(ctx context.Context, cfg Config, commit string, deployment DeploymentReceipt) error {
	verdict, err := c.verifyHealth(ctx, cfg, commit, deployment)
	if err != nil {
		return fmt.Errorf("independent verification: %w", err)
	}
	if verdict.Outcome == VerificationClear {
		return nil
	}
	if cfg.ProvidedPlan != nil {
		return &ProvidedPlanError{Stage: "verify", Reason: verdict.Feedback()}
	}
	return fmt.Errorf("accepted health diagnostics %s fail independent verification and can no longer be corrected: %s", shortCommit(commit), verdict.Feedback())
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
	if cfg.OperationID == "" || cfg.Application == "" || cfg.Namespace == "" || cfg.MaxAttempts < 1 {
		return errors.New("operation ID, application, namespace and positive max attempts are required")
	}
	if len(cfg.Sources) > 0 && strings.TrimSpace(cfg.HealthObjective) == "" {
		return errors.New("followed sources are published with the human goal and require a health objective")
	}
	type agentChoice struct {
		id      string
		request proto.Message
	}
	choices := []agentChoice{{cfg.JudgeAgentID, &contracts.HealthJudgeRequest{}}}
	if cfg.ProvidedPlan == nil {
		choices = append(choices, agentChoice{cfg.DeployerAgentID, &contracts.SourceDeploymentRequest{}})
	}
	for _, choice := range choices {
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
		if _, _, err := c.Broker.OperabilityReport(ctx, id, operability.StageDeployment); err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", fmt.Errorf("accepted source's operability report: %w", err)
		}
		recorded := deploymentFromBroker(evidence)
		readiness, err := c.Readiness.VerifyAccepted(ctx, prior.Commit, cfg.Namespace, recorded)
		if err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", fmt.Errorf("verify accepted source: %w", refusedByPolicy(err, prior.Commit))
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
	plan := cfg.ProvidedPlan
	if plan == nil {
		agentPlan, nextFeedback, err := c.agentPlan(ctx, id, cfg, attempt, feedback, proposal)
		if err != nil || agentPlan == nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, nextFeedback, err
		}
		plan = agentPlan
		// App files are never published: edits no patch reproduces go back to
		// the agent as a correction before anything is deployed.
		scratch, err := c.Broker.ScratchFeedback(ctx, id, broker.InitialPhase)
		if err != nil {
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return broker.Receipt{}, DeploymentReceipt{}, false, "", err
		}
		if scratch != "" {
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return broker.Receipt{}, DeploymentReceipt{}, false, scratch, nil
		}
	}
	if err := admissiblePlan(plan); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "the deployment plan is not deployable: " + err.Error(), nil
	}
	// Go writes the canonical plan into the proposal, whatever the agent left
	// at that path: the accepted tree then answers "what is deployed and how".
	if err := planfile.Write(proposal.Path, plan); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "", fmt.Errorf("write deployment plan: %w", err)
	}
	var offlineReport operability.Report
	if report, decision, checked, err := c.offlineGate(ctx, cfg, proposal, plan); err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "", err
	} else if checked && decision.Kind != operability.Accepted {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		reason, err := offlineOutcome(report, decision)
		return broker.Receipt{}, DeploymentReceipt{}, false, reason, err
	} else if checked {
		offlineReport = report
	}
	var readiness DeploymentReceipt
	err = c.retryInfrastructure(ctx, cfg, "independent source deployment", func() (err error) {
		readiness, err = c.Readiness.ApplyAndVerify(ctx, proposal.Path, plan)
		return err
	})
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		var infrastructure *InfrastructureError
		if errors.As(err, &infrastructure) || ctx.Err() != nil {
			return broker.Receipt{}, DeploymentReceipt{}, false, "", err
		}
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent source deployment failed: " + err.Error(), nil
	}
	if !completeDeploymentReceipt(readiness) {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent source deployment returned incomplete evidence", nil
	}
	if !rendered.SameDeploymentSource(readiness.Source, plan.GetSource()) {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "independent deployment source differs from proposal", nil
	}
	report, decision, err := c.deploymentReport(ctx, cfg, proposal, plan, offlineReport, readiness)
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "", err
	}
	if decision.Kind != operability.Accepted {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		reason, err := deployedOutcome(report, decision)
		return broker.Receipt{}, DeploymentReceipt{}, false, reason, err
	}
	receipt, err := c.Broker.PublishVerifiedSource(ctx, id, deploymentToBroker(readiness), report)
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, DeploymentReceipt{}, false, "broker rejected deployment: " + err.Error(), nil
	}
	return receipt, readiness, true, "", nil
}

// agentPlan dispatches the deployer agent for a plan. A nil plan with a nil
// error carries correction feedback; the proposal is already released then.
func (c Coordinator) agentPlan(ctx context.Context, id string, cfg Config, attempt int, feedback string, proposal broker.Proposal) (*contracts.DeploymentPlan, string, error) {
	plan, nextFeedback, err := c.proposePlan(ctx, id, cfg, attempt, feedback, proposal)
	if plan == nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
	}
	return plan, nextFeedback, err
}

// proposePlan is one deployer task over a proposal checkout; it releases
// nothing. A nil plan with a nil error carries feedback for the next round.
func (c Coordinator) proposePlan(ctx context.Context, id string, cfg Config, attempt int, feedback string, proposal broker.Proposal) (*contracts.DeploymentPlan, string, error) {
	request := &contracts.SourceDeploymentRequest{Application: cfg.Application, Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, CorrectionFeedback: feedback, Attempt: uint32(attempt), RegistryHost: cfg.RegistryHost, Renderers: pluginDescriptors(c.Renderers.Describe()), ImageSources: pluginDescriptors(c.ImageSources.Describe())}
	task, err := c.newTask(id, cfg.DeployerAgentID, proposal, request)
	if err != nil {
		return nil, "", err
	}
	response := &contracts.SourceDeploymentResult{}
	if err := c.execute(ctx, task, response); err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, err.Error(), nil
	}
	if !response.GetProposed() || strings.TrimSpace(response.GetAgentSessionId()) == "" || response.GetPlan() == nil {
		return nil, "deployment worker did not propose a complete source plan: " + response.GetSummary(), nil
	}
	return response.GetPlan(), "", nil
}

// advanceFeedbackPreamble tells the deployer what an advance correction is: its
// checkout is not an onboarding proposal but the accepted tree with an upstream
// revision merged in, which an operability check refused before anything was
// deployed.
const advanceFeedbackPreamble = "An upstream advance is held before deployment. Your checkout is the accepted source with the new upstream revision merged in; " +
	"the current plan is .sdo/deployment/plan.yaml. Correct only deployment files and patches under .sdo/deployment (app files are never edited) " +
	"and return the full corrected plan. The hold: "

// CorrectDeployment is the deployment agent's route in an upstream advance
// (operational contract plan section 5, E1-E3): one deployer task over the
// advance checkout with the hold as its request. cfg names the application, its
// namespace, health objective and registry; the task ID is the advance's and
// the attempt, so a restarted controller re-attaches to the same task. A task
// that fails or proposes no plan is an error (the advance feeds its text back).
func (c Coordinator) CorrectDeployment(ctx context.Context, cfg Config, correction DeploymentCorrection) (*contracts.DeploymentPlan, error) {
	if c.Registry == nil || c.Workers == nil {
		return nil, errors.New("deployment correction needs the agent registry and workers")
	}
	id := fmt.Sprintf("%s-deployer-%d", correction.AdvanceID, correction.Attempt)
	plan, feedback, err := c.proposePlan(ctx, id, cfg, correction.Attempt, advanceFeedbackPreamble+correction.Feedback, correction.Proposal)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, errors.New(feedback)
	}
	return plan, nil
}

// admissiblePlan requires the deployment source: which renderer renders the
// tree and how. The deployer, not the coordinator, decodes and checks it.
func admissiblePlan(plan *contracts.DeploymentPlan) error {
	if plan.GetSource() == nil {
		return errors.New("the plan names no deployment source; set source to a renderer ID from the request's renderers and its config")
	}
	if plan.GetStrategyId() != "" || plan.GetStrategyConfig() != nil {
		return errors.New("the plan sets the retired strategy fields; describe the deployment only with source, images and expected workload IDs")
	}
	return nil
}

func completeDeploymentReceipt(receipt DeploymentReceipt) bool {
	return receipt.Ready && receipt.Source != nil && receipt.BaseTree != "" && receipt.EffectiveTree != "" && receipt.PatchesDigest != "" && receipt.ObservedRevision != "" && receipt.RenderDigest != "" && receipt.EnvDigest != "" && len(receipt.Images) > 0 && len(receipt.ResourceIDs) > 0
}

func sameDeploymentIdentity(left, right DeploymentReceipt) bool {
	if left.BaseTree != right.BaseTree || left.EffectiveTree != right.EffectiveTree || left.PatchesDigest != right.PatchesDigest || left.ObservedRevision != right.ObservedRevision || left.RenderDigest != right.RenderDigest || left.EnvDigest != right.EnvDigest || !renderenv.Same(left.Environment, right.Environment) || !rendered.SameDeploymentSource(left.Source, right.Source) {
		return false
	}
	leftImages, rightImages := slices.Clone(left.Images), slices.Clone(right.Images)
	leftResources, rightResources := slices.Clone(left.ResourceIDs), slices.Clone(right.ResourceIDs)
	slices.Sort(leftImages)
	slices.Sort(rightImages)
	slices.Sort(leftResources)
	slices.Sort(rightResources)
	return reflect.DeepEqual(left.ContainerImages, right.ContainerImages) && slices.Equal(leftImages, rightImages) && slices.Equal(leftResources, rightResources)
}

// deploymentToBroker records the receipt. The admitted plan is not part of it:
// it is the accepted tree's .sdo/deployment/plan.yaml.
func deploymentToBroker(receipt DeploymentReceipt) broker.SourceEvidence {
	evidence := broker.SourceEvidence{
		ContainerImages: append([]rendered.DeployedImage(nil), receipt.ContainerImages...), BaseTree: receipt.BaseTree, EffectiveTree: receipt.EffectiveTree, PatchesDigest: receipt.PatchesDigest, ObservedRevision: receipt.ObservedRevision,
		RenderDigest: receipt.RenderDigest, EnvDigest: receipt.EnvDigest,
		Images: append([]string(nil), receipt.Images...), ResourceIDs: append([]string(nil), receipt.ResourceIDs...), Ready: receipt.Ready,
	}
	if receipt.Environment != nil {
		evidence.RenderEnvironment = proto.Clone(receipt.Environment).(*contracts.RenderEnvironment)
	}
	if receipt.Policy != nil {
		evidence.DeploymentPolicy = proto.Clone(receipt.Policy).(*contracts.DeploymentPolicy)
	}
	if receipt.Source != nil {
		evidence.DeploymentSource = proto.Clone(receipt.Source).(*contracts.DeploymentSource)
	}
	return evidence
}

func deploymentFromBroker(evidence broker.SourceEvidence) DeploymentReceipt {
	receipt := DeploymentReceipt{
		ContainerImages: append([]rendered.DeployedImage(nil), evidence.ContainerImages...), BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree, PatchesDigest: evidence.PatchesDigest, ObservedRevision: evidence.ObservedRevision,
		RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest,
		Images: append([]string(nil), evidence.Images...), ResourceIDs: append([]string(nil), evidence.ResourceIDs...), Ready: evidence.Ready,
	}
	if evidence.DeploymentSource != nil {
		receipt.Source = proto.Clone(evidence.DeploymentSource).(*contracts.DeploymentSource)
	}
	if evidence.DeploymentPolicy != nil {
		receipt.Policy = proto.Clone(evidence.DeploymentPolicy).(*contracts.DeploymentPolicy)
	}
	if evidence.RenderEnvironment != nil {
		receipt.Environment = proto.Clone(evidence.RenderEnvironment).(*contracts.RenderEnvironment)
	}
	return receipt
}

func (c Coordinator) architecture(ctx context.Context, id string, cfg Config, sourceCommit string, deployment DeploymentReceipt) (broker.Receipt, bool, string, error) {
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
	facts, err := c.sourceFacts(ctx, proposal.Path, sourceCommit, cfg.Namespace, deployment)
	if err != nil {
		return broker.Receipt{}, false, "", fmt.Errorf("read accepted source inventory for architecture: %w", err)
	}
	if err := c.Bootstrap.WriteArchitecture(proposal.Path, cfg.Application, sourceCommit, memory.ArchitectureInventory(facts)); err != nil {
		return broker.Receipt{}, false, "", err
	}
	receipt, err := c.Broker.Publish(ctx, id, broker.Upkeep, broker.InitialPhase)
	if err != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return broker.Receipt{}, false, "broker rejected architecture: " + err.Error(), nil
	}
	return receipt, true, "", nil
}

func shortCommit(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

// sourceFacts is the accepted source's logical inventory: the injected
// projection, else the rendered desired state at commit read from checkout.
func (c Coordinator) sourceFacts(ctx context.Context, checkout, commit, namespace string, deployment DeploymentReceipt) (memory.SourceFacts, error) {
	if c.SourceFacts != nil {
		return c.SourceFacts(ctx, checkout, commit)
	}
	return RenderedSourceFacts(ctx, c.Renderers, checkout, commit, namespace, deployment.Source, deployment.Environment)
}

// RenderedSourceFacts renders the deployment source at commit through the
// renderer catalog and projects the desired objects into SourceFacts. It is
// the only way the lifecycle learns what a source declares.
func RenderedSourceFacts(ctx context.Context, catalog *render.Catalog, checkout, commit, namespace string, source *contracts.DeploymentSource, environment *contracts.RenderEnvironment) (memory.SourceFacts, error) {
	reader := rendered.Source{Repository: checkout, Deployment: source, Catalog: catalog, Environment: environment}
	inventory, err := reader.AcceptedInventory(ctx, commit, namespace)
	if err != nil {
		return memory.SourceFacts{}, fmt.Errorf("render accepted source %s: %w", commit, err)
	}
	return memory.SourceFactsOf(inventory.Objects, source.GetRendererId()), nil
}

// judge dispatches the health judge until the broker accepts its diagnostics
// or the validation retry bound is spent. Every broker rejection is recorded
// durably, with its typed diagnostics, before the checkout is released, so the
// attempt number derived from that log survives a restart.
func (c Coordinator) judge(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string, deployment DeploymentReceipt) (broker.Receipt, bool, string, error) {
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
		// Another round over the same source would repeat a spent budget or a
		// repeated rejection: end the run with the typed cause instead.
		if stop := judgeStop(log, architectureCommit); stop != nil {
			return broker.Receipt{}, false, "", stop
		}
		receipt, ready, nextFeedback, rejected, err := c.judgeOnce(ctx, id, cfg, attempt, sourceCommit, architectureCommit, feedback, deployment, log)
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

// maxRejectionHistory bounds the diagnostics carried into a retry request.
const maxRejectionHistory = 40

// rejectionHistory is every distinct diagnostic the broker rejected, newest
// rejection first, each once. A retry is a fresh session on a rolled-back
// checkout, so the latest rejection alone hides what earlier sessions tried
// and what the broker already told them; the last rejection is often the least
// informative one.
func rejectionHistory(rejections []*contracts.ProposalRejection) []*contracts.MemoryDiagnostic {
	type key struct {
		rule, path, message string
		line                int32
	}
	seen := map[key]bool{}
	var history []*contracts.MemoryDiagnostic
	for i := len(rejections) - 1; i >= 0; i-- {
		for _, d := range rejections[i].GetDiagnostics() {
			k := key{d.GetRule(), d.GetPath(), d.GetMessage(), d.GetLine()}
			if seen[k] || len(history) >= maxRejectionHistory {
				continue
			}
			seen[k] = true
			history = append(history, d)
		}
	}
	return history
}

// judgeOnce runs one judge dispatch. A non-nil rejected slice means the broker
// rejected the proposal with typed diagnostics and the caller owns the retry.
func (c Coordinator) judgeOnce(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string, deployment DeploymentReceipt, log *contracts.ProposalRejectionLog) (broker.Receipt, bool, string, []*contracts.MemoryDiagnostic, error) {
	validationAttempt := uint32(len(log.GetRejections()) + 1)
	var previous []*contracts.MemoryDiagnostic
	if n := len(log.GetRejections()); n > 0 {
		previous = rejectionHistory(log.GetRejections())
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
	if validationAttempt == 1 {
		// The base is the same for every retry of this proposal: check it once,
		// before the agent can be blamed for what it did not write.
		base, err := c.Broker.CheckBase(ctx, id, broker.HealthJudge, broker.InitialPhase)
		if err != nil {
			return broker.Receipt{}, false, "", nil, fmt.Errorf("check the accepted base before the health judge: %w", err)
		}
		if len(base) > 0 {
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return broker.Receipt{}, false, "", nil, NewBaseInvalidError(architectureCommit, base)
		}
	}
	architectureSummary, err := c.Bootstrap.ReadArchitecture(proposal.Path)
	if err != nil {
		return broker.Receipt{}, false, "", nil, err
	}
	facts, err := c.sourceFacts(ctx, proposal.Path, sourceCommit, cfg.Namespace, deployment)
	if err != nil {
		return broker.Receipt{}, false, "", nil, fmt.Errorf("read accepted source inventory: %w", err)
	}
	request := &contracts.HealthJudgeRequest{Telemetry: cfg.Telemetry, Application: cfg.Application, Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, AcceptedSourceCommit: sourceCommit, CorrectionFeedback: feedback, Attempt: uint32(attempt), ArchitectureSummaryMarkdown: architectureSummary, SourceResources: facts.Resources, ValidationAttempt: validationAttempt, PreviousDiagnostics: previous}
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
		// Cancellation does not mean the remote Job stopped. Keep its checkout
		// and immutable task identity for a later bounded reattachment.
		if ctx.Err() != nil {
			return broker.Receipt{}, false, "", nil, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return broker.Receipt{}, false, "", nil, err
		}
		var failedJob *worker_registry.JobFailedError
		if errors.As(err, &failedJob) {
			// Preserve the failed attempt and proposal for the bounded retry.
			return broker.Receipt{}, false, err.Error(), []*contracts.MemoryDiagnostic{{Rule: jobFailedRule, Message: err.Error()}}, nil
		}
		var vanished *worker_registry.JobVanishedError
		if errors.As(err, &vanished) {
			// Redispatch already failed too: record a typed failed attempt and
			// let the bounded retry start a fresh task rather than end the process.
			return broker.Receipt{}, false, err.Error(), []*contracts.MemoryDiagnostic{{Rule: vanishedWorkerRule, Message: err.Error()}}, nil
		}
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail(err.Error())
	}

	if !response.GetAuthored() || len(response.GetDetectorIds()) == 0 ||
		strings.TrimSpace(response.GetAgentSessionId()) == "" {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail("health judge did not author detectors: " + response.GetSummary())
	}
	if response.GetArtifactProposal() != nil {
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return fail("direct-edit health judge rejects deprecated artifact_proposal")
	}
	outcome, err := c.gateCandidate(ctx, id, cfg, deployment, log)
	if err != nil {
		return broker.Receipt{}, false, "", nil, err
	}
	return outcome.receipt, outcome.ready, outcome.feedback, outcome.rejected, nil
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
	var vanished *worker_registry.JobVanishedError
	if discarder, ok := c.Workers.(VanishedDiscarder); ok && errors.As(err, &vanished) {
		// Lifecycle workers only author proposals in isolated checkouts; the
		// controller applies and verifies them. Running one twice repeats no
		// effect, so a Job lost to an earlier killed attempt is dispatched
		// again instead of failing every restart on the same stale evidence.
		if err := discarder.DiscardVanished(ctx, task); err != nil {
			return err
		}
		result, err = c.Workers.Execute(ctx, task)
	}
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

func pluginDescriptors(descs []plugindesc.Descriptor) []*contracts.PluginDescriptor {
	out := make([]*contracts.PluginDescriptor, 0, len(descs))
	for _, d := range descs {
		out = append(out, &contracts.PluginDescriptor{Id: d.ID, ConfigType: d.ConfigType, ConfigSchemaJson: d.ConfigSchemaJSON, Doc: d.Doc})
	}
	return out
}
