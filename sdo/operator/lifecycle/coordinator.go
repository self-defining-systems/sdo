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
	// HeadUndeployed reports a head whose effective source no recorded
	// deployment has applied: a push the head deployer has yet to deploy.
	HeadUndeployed(context.Context) (bool, error)
	ChangedPaths(context.Context, string, broker.Phase) ([]string, error)
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
	// BaseTree with the .sdo/ memory projected away (rendered.Effective). The
	// pod-template source stamp is EffectiveTree, so ObservedRevision (read
	// back from the cluster) equals it.
	EffectiveTree    string
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
	// Seeded marks the receipt of an install whose deployment SDO never
	// recorded (a seeded application): it is never Ready and binds nothing,
	// and a verifier judges a candidate's detectors against the live
	// namespace as found.
	Seeded bool
	// Recreated lists the objects the deployer deleted and created again
	// because an immutable field changed (audit; not deployment identity).
	Recreated []rendered.Recreation
}

// SourceReadinessVerifier is a trusted Go build/apply/observe adapter: it
// deploys a checkout's committed tree, with the optional deployment override
// as plan. No agent can attest its own deployment.
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
	// Now reads the clock the pending window is measured on; nil reads the
	// wall clock.
	Now func() time.Time
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
	// DeploymentSource and RenderEnvironment tell RefreshHealth what to
	// render: the accepted checkout's plan source and the recorded install
	// environment. Run takes both from the deployment receipt instead.
	DeploymentSource  *contracts.DeploymentSource
	RenderEnvironment *contracts.RenderEnvironment
	MaxAttempts       int
	// JourneysRequired (onboarding.journeys_required) makes Run refuse an
	// objective that carries no user journeys, before any commit or agent.
	JourneysRequired bool
	// StopAfterArchitecture ends Run once the goal, schema, accepted source and
	// seeded architecture are committed: no health judge and no controller
	// install. The onboarding agent starts from that accepted head.
	StopAfterArchitecture bool
	// SettingsDigest is settings.Config.DetectionDigest of the configuration the operator runs under; it joins
	// every operability report subject this run builds.
	SettingsDigest string
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
	// SteadyState marks a judge run for an application that is already onboarded (a
	// health refresh): the onboarding-only evidence (existing records) does not gate it.
	SteadyState bool
	// MaxAttempts bounds the deployer's correction tasks and the rounds of the
	// architecture and health judge. The fields below bound the waits that
	// never consume one; zero selects the default.
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
		goalObjective := cfg.HealthObjective
		// Every later stage and agent reads the objective as text, journeys listed, not as a goal document.
		text, err := memory.ObjectiveText(cfg.HealthObjective)
		if err != nil {
			return result, fmt.Errorf("health objective: %w", err)
		}
		cfg.HealthObjective = text
		goal, err := c.bootstrap(ctx, cfg.OperationID+"-goal", broker.Human, func(checkout string) error {
			return c.Bootstrap.WriteGoal(checkout, cfg.Application, goalObjective, cfg.Sources)
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
	// One accepted source per operation: later rounds resume it instead of
	// redeploying, unless the deployed application itself failed the
	// complete-stage checks (a deployer-owned verdict); that round gives the
	// deployer the verdict on a new source proposal.
	sourceReceipt, deployment, err := c.deploy(ctx, cfg.OperationID+"-source", cfg, "")
	if err != nil {
		return result, err
	}
	feedback := ""
	var tracker feedbackTracker
	lastRole := operability.OwnerDesigner
	redeploy := false
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		result.Attempts = attempt
		if redeploy {
			if sourceReceipt, deployment, err = c.deploy(ctx, fmt.Sprintf("%s-source-%d", cfg.OperationID, attempt), cfg, feedback); err != nil {
				return result, err
			}
			feedback, redeploy = "", false
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
		if cfg.StopAfterArchitecture {
			return result, nil
		}
		judgeID := fmt.Sprintf("%s-judge-%d", cfg.OperationID, attempt)
		// A judge commit accepted by an earlier run was verified before it was
		// accepted; only a crash between acceptance and install gets here.
		_, resumed, err := c.Broker.AcceptedCommit(ctx, judgeID, broker.InitialPhase)
		if err != nil {
			return result, err
		}
		judgeReceipt, judgeReady, nextFeedback, deployerOwned, err := c.judge(ctx, judgeID, cfg, attempt, sourceReceipt.Commit, architectureReceipt.Commit, feedback, deployment)
		if err != nil {
			if isTerminalOperability(err) {
				return result, err
			}
			return result, fmt.Errorf("health judge attempt %d: %w", attempt, err)
		}
		if !judgeReady {
			feedback, lastRole, redeploy = nextFeedback, operability.OwnerDesigner, false
			if deployerOwned {
				lastRole, redeploy = operability.OwnerDeployer, true
			}
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
	var held *DeploymentHoldError
	_, offlineGate := asOperabilityError(err)
	return offlineGate || errors.As(err, &held)
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
	if cfg.JourneysRequired {
		if err := memory.RequireJourneys(cfg.HealthObjective); err != nil {
			return err
		}
	}
	if len(cfg.Sources) > 0 && strings.TrimSpace(cfg.HealthObjective) == "" {
		return errors.New("followed sources are published with the human goal and require a health objective")
	}
	type agentChoice struct {
		id      string
		request proto.Message
	}
	choices := []agentChoice{{cfg.JudgeAgentID, &contracts.HealthJudgeRequest{}}}
	if cfg.DeployerAgentID != "" {
		// A named deployer must be registered; an unnamed one is optional (no
		// registered deployer makes every deployment failure a human hold).
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
	if c.Registry == nil {
		return "", "", fmt.Errorf("%w: no agent registry", ErrAgentRegistration)
	}
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

// deploy is the one deployment path. It deploys the proposal's commit as it is
// (offline gate, ApplyAndVerify, report) and publishes it when it passes: an
// application that ships its own deployment needs no agent. A failure the
// deployer owns dispatches the deployer agent on the same proposal with the
// failing step as its correction feedback; the agent commits files like an
// engineer and Go deploys its last commit, up to cfg.MaxAttempts tasks. A tree
// with no deployment files fails offline (no cluster or build time), so that
// is the deployer's first task. Without a registered deployer every failure is
// a human hold (*DeploymentHoldError).
//
// A non-empty feedback is a failure of the accepted source found after it was
// deployed (the complete-stage checks): the deployer is dispatched with it
// before Go deploys again.
func (c Coordinator) deploy(ctx context.Context, id string, cfg Config, feedback string) (broker.Receipt, DeploymentReceipt, error) {
	if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, err
	} else if ok {
		return c.resumeDeployment(ctx, id, cfg, prior)
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, err
	}
	mode := operability.ModeFresh
	if _, _, err := c.target(cfg.DeployerAgentID, &contracts.SourceDeploymentRequest{}); err != nil {
		mode = operability.ModeNoDeployer
	}
	var tracker feedbackTracker
	task := 0
	if feedback != "" {
		if mode == operability.ModeNoDeployer {
			return broker.Receipt{}, DeploymentReceipt{}, &DeploymentHoldError{Stage: "verify", Reason: feedback}
		}
		task++
		if err := c.correctDeployment(ctx, fmt.Sprintf("%s-deployer-%d", id, task), cfg, task, feedback, proposal); err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, err
		}
	}
	for ; ; task++ {
		receipt, deployment, feedback, err := c.deployCommit(ctx, id, cfg, mode, proposal)
		if err != nil {
			var opErr *OperabilityError
			if errors.As(err, &opErr) && mode == operability.ModeNoDeployer {
				err = &DeploymentHoldError{Stage: "deploy", Reason: opErr.Decision.Feedback(), Report: &opErr.Report}
			}
			return broker.Receipt{}, DeploymentReceipt{}, err
		}
		if feedback == "" {
			return receipt, deployment, nil
		}
		if mode == operability.ModeNoDeployer {
			return broker.Receipt{}, DeploymentReceipt{}, &DeploymentHoldError{Stage: "deploy", Reason: feedback}
		}
		if task >= cfg.MaxAttempts {
			return broker.Receipt{}, DeploymentReceipt{}, NewAgentNotConvergingError(operability.OwnerDeployer, proposal.BaseCommit, []string{fmt.Sprintf("exhausted %d deployer tasks; last failure: %s", cfg.MaxAttempts, operability.Clip(feedback))}, true)
		}
		if err := tracker.repeated(operability.OwnerDeployer, proposal.BaseCommit, feedback); err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, err
		}
		if err := c.correctDeployment(ctx, fmt.Sprintf("%s-deployer-%d", id, task+1), cfg, task+1, feedback, proposal); err != nil {
			return broker.Receipt{}, DeploymentReceipt{}, err
		}
	}
}

// resumeDeployment re-verifies a source accepted by an earlier run.
func (c Coordinator) resumeDeployment(ctx context.Context, id string, cfg Config, prior broker.Receipt) (broker.Receipt, DeploymentReceipt, error) {
	evidence, found, err := c.Broker.SourceEvidence(ctx, id)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, fmt.Errorf("load broker source evidence: %w", err)
	}
	if !found {
		return broker.Receipt{}, DeploymentReceipt{}, errors.New("accepted source lacks broker-validated deployment evidence")
	}
	if evidence.AcceptedCommit != prior.Commit {
		return broker.Receipt{}, DeploymentReceipt{}, errors.New("source evidence belongs to another accepted commit")
	}
	if _, _, err := c.Broker.OperabilityReport(ctx, id, operability.StageDeployment); err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, fmt.Errorf("accepted source's operability report: %w", err)
	}
	recorded := deploymentFromBroker(evidence)
	readiness, err := c.Readiness.VerifyAccepted(ctx, prior.Commit, cfg.Namespace, recorded)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, fmt.Errorf("verify accepted source: %w", refusedByPolicy(err, prior.Commit))
	}
	if !completeDeploymentReceipt(readiness) || !sameDeploymentIdentity(recorded, readiness) {
		return broker.Receipt{}, DeploymentReceipt{}, errors.New("accepted source has incomplete or changed deployment evidence")
	}
	return prior, readiness, nil
}

// deployCommit deploys and gates the proposal's current commit once. A
// non-empty feedback is a failure the deployer owns (the proposal is kept for
// its correction); an error is terminal or infrastructure.
func (c Coordinator) deployCommit(ctx context.Context, id string, cfg Config, mode operability.Mode, proposal broker.Proposal) (broker.Receipt, DeploymentReceipt, string, error) {
	plan, err := planfile.Read(proposal.Path, c.planCatalogs())
	switch {
	case errors.Is(err, planfile.ErrAbsent):
		plan = nil
	case err != nil:
		return broker.Receipt{}, DeploymentReceipt{}, "the deployment override .sdo/deployment/plan.yaml does not parse: " + err.Error(), nil
	}
	var offlineReport operability.Report
	if report, decision, checked, err := c.offlineGate(ctx, cfg, mode, proposal, plan); err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, "", err
	} else if checked && decision.Kind != operability.Accepted {
		reason, err := offlineOutcome(report, decision)
		return broker.Receipt{}, DeploymentReceipt{}, reason, err
	} else if checked {
		offlineReport = report
	}
	var readiness DeploymentReceipt
	err = c.retryInfrastructure(ctx, cfg, "independent source deployment", func() (err error) {
		readiness, err = c.Readiness.ApplyAndVerify(ctx, proposal.Path, plan)
		return err
	})
	if err != nil {
		var infrastructure *InfrastructureError
		if errors.As(err, &infrastructure) || ctx.Err() != nil {
			return broker.Receipt{}, DeploymentReceipt{}, "", err
		}
		return broker.Receipt{}, DeploymentReceipt{}, "independent source deployment failed: " + err.Error(), nil
	}
	if !completeDeploymentReceipt(readiness) {
		return broker.Receipt{}, DeploymentReceipt{}, "independent source deployment returned incomplete evidence", nil
	}
	report, decision, err := c.deploymentReport(ctx, cfg, mode, proposal, plan, offlineReport, readiness)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, "", err
	}
	if decision.Kind != operability.Accepted {
		reason, err := deployedOutcome(report, decision)
		return broker.Receipt{}, DeploymentReceipt{}, reason, err
	}
	receipt, err := c.Broker.PublishVerifiedSource(ctx, id, deploymentToBroker(readiness), report)
	if err != nil {
		return broker.Receipt{}, DeploymentReceipt{}, "broker rejected deployment: " + err.Error(), nil
	}
	return receipt, readiness, "", nil
}

// planCatalogs decode the override's renderer and image-source configs.
func (c Coordinator) planCatalogs() planfile.Catalogs {
	var catalogs planfile.Catalogs
	if c.Renderers != nil {
		catalogs.Renderers = c.Renderers
	}
	if c.ImageSources != nil {
		catalogs.Images = c.ImageSources
	}
	return catalogs
}

// correctDeployment is one deployer task over the proposal checkout: the agent
// commits what is missing and replies proposed. Go deploys the checkout's last
// commit whatever the reply says, so a task that fails or does not propose
// only costs an attempt; the next deploy reports what still fails.
func (c Coordinator) correctDeployment(ctx context.Context, taskID string, cfg Config, attempt int, feedback string, proposal broker.Proposal) error {
	request := &contracts.SourceDeploymentRequest{Application: cfg.Application, Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, CorrectionFeedback: feedback, Attempt: uint32(attempt), RegistryHost: cfg.RegistryHost, Renderers: pluginDescriptors(c.Renderers.Describe()), ImageSources: pluginDescriptors(c.ImageSources.Describe())}
	task, err := c.newTask(taskID, cfg.DeployerAgentID, proposal, request)
	if err != nil {
		return err
	}
	response := &contracts.SourceDeploymentResult{}
	if err := c.execute(ctx, task, response); err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func completeDeploymentReceipt(receipt DeploymentReceipt) bool {
	return receipt.Ready && receipt.Source != nil && receipt.BaseTree != "" && receipt.EffectiveTree != "" && receipt.ObservedRevision != "" && receipt.RenderDigest != "" && receipt.EnvDigest != "" && len(receipt.Images) > 0 && len(receipt.ResourceIDs) > 0
}

func sameDeploymentIdentity(left, right DeploymentReceipt) bool {
	if left.BaseTree != right.BaseTree || left.EffectiveTree != right.EffectiveTree || left.ObservedRevision != right.ObservedRevision || left.RenderDigest != right.RenderDigest || left.EnvDigest != right.EnvDigest || !renderenv.Same(left.Environment, right.Environment) {
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

// deploymentToBroker records the receipt. The optional override is not part of
// it: it is the accepted tree's .sdo/deployment/plan.yaml.
func deploymentToBroker(receipt DeploymentReceipt) broker.SourceEvidence {
	evidence := broker.SourceEvidence{
		ContainerImages: append([]rendered.DeployedImage(nil), receipt.ContainerImages...), BaseTree: receipt.BaseTree, EffectiveTree: receipt.EffectiveTree, ObservedRevision: receipt.ObservedRevision,
		RenderDigest: receipt.RenderDigest, EnvDigest: receipt.EnvDigest,
		Images: append([]string(nil), receipt.Images...), ResourceIDs: append([]string(nil), receipt.ResourceIDs...), Ready: receipt.Ready,
		Recreated: append([]rendered.Recreation(nil), receipt.Recreated...),
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
		ContainerImages: append([]rendered.DeployedImage(nil), evidence.ContainerImages...), BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree, ObservedRevision: evidence.ObservedRevision,
		RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest,
		Images: append([]string(nil), evidence.Images...), ResourceIDs: append([]string(nil), evidence.ResourceIDs...), Ready: evidence.Ready,
		Recreated: append([]rendered.Recreation(nil), evidence.Recreated...),
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
func (c Coordinator) judge(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string, deployment DeploymentReceipt) (broker.Receipt, bool, string, bool, error) {
	for {
		if prior, ok, err := c.Broker.AcceptedCommit(ctx, id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", false, err
		} else if ok {
			return prior, true, "", false, nil
		}
		log, err := c.Broker.Rejections(id)
		if err != nil {
			return broker.Receipt{}, false, "", false, err
		}
		// Another round over the same source would repeat a spent budget or a
		// repeated rejection: end the run with the typed cause instead.
		if stop := judgeStop(log, architectureCommit); stop != nil {
			return broker.Receipt{}, false, "", false, stop
		}
		receipt, ready, nextFeedback, rejected, deployerOwned, err := c.judgeOnce(ctx, id, cfg, attempt, sourceCommit, architectureCommit, feedback, deployment, log)
		if err != nil || ready || rejected == nil {
			return receipt, ready, nextFeedback, deployerOwned, err
		}
		if _, err := c.Broker.RecordRejection(id, rejected); err != nil {
			return broker.Receipt{}, false, "", false, err
		}
		if err := c.Broker.ReleaseProposal(id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", false, err
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
func (c Coordinator) judgeOnce(ctx context.Context, id string, cfg Config, attempt int, sourceCommit, architectureCommit, feedback string, deployment DeploymentReceipt, log *contracts.ProposalRejectionLog) (broker.Receipt, bool, string, []*contracts.MemoryDiagnostic, bool, error) {
	validationAttempt := uint32(len(log.GetRejections()) + 1)
	var previous []*contracts.MemoryDiagnostic
	if n := len(log.GetRejections()); n > 0 {
		previous = rejectionHistory(log.GetRejections())
		// A crash may have left the rejected checkout behind.
		if err := c.Broker.ReleaseProposal(id, broker.InitialPhase); err != nil {
			return broker.Receipt{}, false, "", nil, false, err
		}
	}
	fail := func(feedback string) (broker.Receipt, bool, string, []*contracts.MemoryDiagnostic, bool, error) {
		return broker.Receipt{}, false, feedback, nil, false, nil
	}
	proposal, err := c.Broker.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return broker.Receipt{}, false, "", nil, false, err
	}
	if proposal.BaseCommit != architectureCommit {
		return broker.Receipt{}, false, "", nil, false, fmt.Errorf("judge proposal base %q differs from accepted architecture %q", proposal.BaseCommit, architectureCommit)
	}
	if validationAttempt == 1 {
		// The base is the same for every retry of this proposal: check it once,
		// before the agent can be blamed for what it did not write.
		base, err := c.Broker.CheckBase(ctx, id, broker.HealthJudge, broker.InitialPhase)
		if err != nil {
			return broker.Receipt{}, false, "", nil, false, fmt.Errorf("check the accepted base before the health judge: %w", err)
		}
		if len(base) > 0 {
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return broker.Receipt{}, false, "", nil, false, NewBaseInvalidError(architectureCommit, base)
		}
	}
	architectureSummary, err := c.Bootstrap.ReadArchitecture(proposal.Path)
	if err != nil {
		return broker.Receipt{}, false, "", nil, false, err
	}
	facts, err := c.sourceFacts(ctx, proposal.Path, sourceCommit, cfg.Namespace, deployment)
	if err != nil {
		return broker.Receipt{}, false, "", nil, false, fmt.Errorf("read accepted source inventory: %w", err)
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
		return broker.Receipt{}, false, "", nil, false, err
	}
	response := &contracts.HealthJudgeResult{}
	if err := c.execute(ctx, task, response); err != nil {
		// Cancellation does not mean the remote Job stopped. Keep its checkout
		// and immutable task identity for a later bounded reattachment.
		if ctx.Err() != nil {
			return broker.Receipt{}, false, "", nil, false, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return broker.Receipt{}, false, "", nil, false, err
		}
		var failedJob *worker_registry.JobFailedError
		if errors.As(err, &failedJob) {
			// Preserve the failed attempt and proposal for the bounded retry.
			return broker.Receipt{}, false, err.Error(), []*contracts.MemoryDiagnostic{{Rule: jobFailedRule, Message: err.Error()}}, false, nil
		}
		var vanished *worker_registry.JobVanishedError
		if errors.As(err, &vanished) {
			// Redispatch already failed too: record a typed failed attempt and
			// let the bounded retry start a fresh task rather than end the process.
			return broker.Receipt{}, false, err.Error(), []*contracts.MemoryDiagnostic{{Rule: vanishedWorkerRule, Message: err.Error()}}, false, nil
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
		return broker.Receipt{}, false, "", nil, false, err
	}
	return outcome.receipt, outcome.ready, outcome.feedback, outcome.rejected, outcome.deployer, nil
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
