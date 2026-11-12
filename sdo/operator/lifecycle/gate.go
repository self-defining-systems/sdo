package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
)

const (
	defaultDesignerAttempts = 2
	// maxCandidateRestages bounds how often a candidate whose base moved is
	// staged and verified again before the run gives up.
	maxCandidateRestages = 3
	// operabilityRulePrefix marks a recorded rejection that came from the
	// operability gate (a designer correction) rather than the shared validator.
	operabilityRulePrefix = "operability."
	trafficDetectorPrefix = "traffic-"
)

// LiveChecker computes the live deployment checks (O1, O2, O6 budget) for a
// receipt against the desired state of a checkout. The trusted Deployer
// implements it. A nil outcome trusts the receipt; the coordinator never
// re-applies for the gate.
type LiveChecker interface {
	CheckLive(ctx context.Context, checkout string, plan *contracts.DeploymentPlan, receipt DeploymentReceipt, outcome error) ([]operability.Check, error)
}

func (c Coordinator) mode(cfg Config) operability.Mode {
	if cfg.ProvidedPlan != nil {
		return operability.ModeProvided
	}
	return operability.ModeFresh
}

// candidateReport is the complete-stage report for a staged candidate: the live
// deployment checks, the memory checks of what the shared validator accepted at
// staging, and the independent detector evaluation of the staged checkout. The
// detector evaluation is skipped, never passed, when a deployment check failed.
//
// A verdict still pending at the bound is not an error here: it is the O4 check
// pending, so the gate can return it to the designer whose probe never passed.
// The second result is the bounded-window error for the case nobody can correct.
func (c Coordinator) candidateReport(ctx context.Context, cfg Config, candidate broker.Candidate, deployment DeploymentReceipt) (operability.Report, *PendingVerificationError, error) {
	live, err := c.Readiness.CheckLive(ctx, candidate.Path, &contracts.DeploymentPlan{Source: deployment.Source}, deployment, nil)
	if err != nil {
		return operability.Report{}, nil, fmt.Errorf("live deployment checks: %w", err)
	}
	gaps := make([]offline.Finding, 0, len(candidate.CoverageGaps))
	for _, gap := range candidate.CoverageGaps {
		gaps = append(gaps, offline.Finding{Rule: gap.Rule, Path: gap.Path, Line: gap.Line, Message: gap.Message})
	}
	// Staging ran the shared validator and rejected nothing: no blocking finding.
	checks := append(live, offline.CheckMemory(offline.Memory{Notes: gaps, OwnershipChecked: true})...)
	var applyChecks []operability.Check
	var pending *PendingVerificationError
	if !failing(live) {
		if applyChecks, err = c.applyTelemetry(ctx, candidate.Path); err != nil {
			return operability.Report{}, nil, err
		}
		checks = append(checks, applyChecks...)
	}
	switch {
	case failing(live):
		checks = append(checks, operability.Check{ID: "O4.detectors", Phase: operability.PhaseLive, Status: operability.StatusSkipped, Detail: "not run: a deployment check failed"})
	case failing(applyChecks):
		checks = append(checks, operability.Check{ID: "O4.detectors", Phase: operability.PhaseLive, Status: operability.StatusSkipped, Detail: "not run: the telemetry apply phase failed"})
	default:
		verdict, err := c.awaitVerdict(ctx, cfg, func() (HealthVerdict, error) {
			return c.Verifier.VerifyCandidate(ctx, candidate.Path, candidate.Commit, cfg.Namespace, cfg.HealthObjective, deployment)
		})
		if err != nil && !errors.As(err, &pending) {
			return operability.Report{}, nil, err
		}
		checks = append(checks, verdictChecks(verdict)...)
	}
	subject := operability.Subject{
		AcceptedCommit: candidate.Commit, BaseTree: deployment.BaseTree, EffectiveTree: deployment.EffectiveTree,
		PatchesDigest: deployment.PatchesDigest, RenderDigest: deployment.RenderDigest, EnvDigest: deployment.EnvDigest,
		SettingsDigest:  cfg.SettingsDigest,
		DiagnosticsTree: candidate.DiagnosticsTree,
	}
	report, err := operability.New(operability.StageComplete, subject).With(checks...)
	return report, pending, err
}

func failing(checks []operability.Check) bool {
	return slices.ContainsFunc(checks, func(c operability.Check) bool { return c.Status == operability.StatusFail })
}

// verdictChecks turns the independent detector verdict into the O4 checks and
// puts the designer-owned detectors that fire in the check's paths. It is the O4 discriminator's
// provenance rule: a firing detector Go wrote (the generic floor) is the
// application's fault; a firing detector or traffic probe the health judge
// authored is detector_fault first. A failing verdict that names no firing
// detector cannot be attributed to the designer, so it is the application's.
func verdictChecks(verdict HealthVerdict) []operability.Check {
	return append(detectorVerdictChecks(verdict), coverageNoteChecks(verdict.CoverageNotes)...)
}

// coverageNoteChecks records the detectors that cleared without observing part
// of their scope as one O3 note: a coverage gap the report shows and no reader
// acts on.
func coverageNoteChecks(notes []DetectorState) []operability.Check {
	if len(notes) == 0 {
		return nil
	}
	parts := make([]string, 0, len(notes))
	for _, note := range notes {
		parts = append(parts, note.DetectorID+": "+note.Reason)
	}
	slices.Sort(parts)
	return []operability.Check{{ID: "O3.detector-coverage", Phase: operability.PhaseLive, Status: operability.StatusNote, Detail: operability.Clip(strings.Join(parts, "; "))}}
}

func detectorVerdictChecks(verdict HealthVerdict) []operability.Check {
	const id = "O4.detectors"
	live := func(status operability.Status, cause operability.Cause, detail string, paths []string) operability.Check {
		return operability.Check{ID: id, Phase: operability.PhaseLive, Status: status, Cause: cause, Detail: operability.Clip(detail), Paths: paths}
	}
	switch verdict.Outcome {
	case VerificationClear:
		return []operability.Check{live(operability.StatusPass, "", verdict.Summary, nil)}
	case VerificationPending:
		// The designer's probes that stayed pending are the paths to correct.
		var paths []string
		for _, state := range verdict.Detectors {
			if state.Status == "pending" && !memory.IsGenericHealthDetector(state.DetectorID) {
				paths = append(paths, detectorPath(state.DetectorID))
			}
		}
		slices.Sort(paths)
		return []operability.Check{live(operability.StatusPending, operability.CauseProbeUnavailable, verdict.Feedback(), paths)}
	}
	var floor, designer []DetectorState
	for _, state := range verdict.Detectors {
		if state.Status != "firing" {
			continue
		}
		if memory.IsGenericHealthDetector(state.DetectorID) {
			floor = append(floor, state)
		} else {
			designer = append(designer, state)
		}
	}
	if len(floor) > 0 || len(designer) == 0 {
		return []operability.Check{live(operability.StatusFail, operability.CauseSourceFault, verdict.Feedback(), nil)}
	}
	var paths []string
	for _, state := range designer {
		paths = append(paths, detectorPath(state.DetectorID))
	}
	slices.Sort(paths)
	return []operability.Check{live(operability.StatusFail, operability.CauseDetectorFault, verdict.Feedback(), paths)}
}

// detectorPath is where the health judge's detector or traffic probe lives.
func detectorPath(id string) string {
	if name, ok := strings.CutPrefix(id, trafficDetectorPrefix); ok {
		return layout.TrafficWorkloadsDir + "/" + name + ".yaml"
	}
	if check, ok := memory.DataCheckOfDetector(id); ok {
		return layout.DataCheckPath(check)
	}
	return layout.DetectorManifestPath(layout.ClassHealth, id)
}

// decisionHasPaths reports whether some check names a file the designer owns.
func decisionHasPaths(d operability.Decision) bool {
	return slices.ContainsFunc(d.Checks, func(c operability.Check) bool { return len(c.Paths) > 0 })
}

// operabilityDiagnostics records a designer correction as typed diagnostics, so
// the durable rejection log carries the retry count and the next dispatch sees
// the findings in PreviousDiagnostics. Each path is one diagnostic; a check with
// no path is one diagnostic without a path.
func operabilityDiagnostics(checks []operability.Check) []*contracts.MemoryDiagnostic {
	var out []*contracts.MemoryDiagnostic
	for _, check := range checks {
		rule := operabilityRulePrefix + check.ID
		message := fmt.Sprintf("[%s] %s", check.Cause, check.Detail)
		if len(check.Paths) == 0 {
			out = append(out, &contracts.MemoryDiagnostic{Rule: rule, Message: message})
		}
		for _, path := range check.Paths {
			out = append(out, &contracts.MemoryDiagnostic{Rule: rule, Path: path, Message: message})
		}
	}
	return out
}

// priorDesignerCorrections counts the gate's corrections in the durable log and
// returns the paths of the most recent one.
func priorDesignerCorrections(log *contracts.ProposalRejectionLog) (count int, lastPaths []string) {
	for _, rejection := range log.GetRejections() {
		var paths []string
		gate := false
		for _, d := range rejection.GetDiagnostics() {
			if strings.HasPrefix(d.GetRule(), operabilityRulePrefix) {
				gate = true
				if d.GetPath() != "" {
					paths = append(paths, d.GetPath())
				}
			}
		}
		if gate {
			count++
			slices.Sort(paths)
			lastPaths = paths
		}
	}
	return count, lastPaths
}

// reattribute applies the O4 discriminator's cap. After DesignerAttempts
// corrections, a designer-owned detector set that still fires unchanged is
// probably right, so its firing is the application's: detector_fault becomes
// source_fault once, and the report routes to the deployer (fresh) or the human
// (provided plan).
func reattribute(report operability.Report, log *contracts.ProposalRejectionLog, attempts int) operability.Report {
	count, last := priorDesignerCorrections(log)
	if count < attempts {
		return report
	}
	out := report
	out.Items = slices.Clone(report.Items)
	for i, item := range out.Items {
		checks := slices.Clone(item.Checks)
		for j, check := range checks {
			if check.Status != operability.StatusFail || check.Cause != operability.CauseDetectorFault {
				continue
			}
			paths := slices.Clone(check.Paths)
			slices.Sort(paths)
			if slices.Equal(paths, last) {
				check.Cause = operability.CauseSourceFault
				check.Detail = operability.Clip(fmt.Sprintf("still firing after %d designer corrections with the same detectors: %s", count, check.Detail))
				checks[j] = check
			}
		}
		out.Items[i].Checks = checks
	}
	return out
}

// retryChecks marks an infrastructure decision of the gate so the bounded
// infrastructure retry applies and no attempt is spent.
type retryChecks struct{ decision operability.Decision }

func (e *retryChecks) Error() string {
	return "operability checks hit an infrastructure failure:\n" + e.decision.Feedback()
}

func (e *retryChecks) InfrastructureOutcome() bool { return true }

// gateOutcome is what the judge loop does after the gate decided.
type gateOutcome struct {
	receipt  broker.Receipt
	ready    bool
	feedback string
	rejected []*contracts.MemoryDiagnostic
}

// publishUnverified is the health refresh of an install SDO never deployed (a
// seeded application): there is no recorded deployment to verify the judge's
// candidate against, so the broker's own validation is the only gate. It is
// logged, never silent, and only RefreshHealth can reach it: a Run always has a
// deployment receipt.
func (c Coordinator) publishUnverified(ctx context.Context, id string) (gateOutcome, error) {
	slog.Warn("health_refresh_published_unverified", "proposal", id, "reason", "accepted source has no recorded deployment")
	receipt, err := c.Broker.Publish(ctx, id, broker.HealthJudge, broker.InitialPhase)
	if err != nil {
		if diagnostics, typed := broker.RejectionDiagnostics(err); typed {
			return gateOutcome{rejected: diagnostics}, nil
		}
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return gateOutcome{feedback: "broker rejected health diagnostics: " + err.Error()}, nil
	}
	return gateOutcome{receipt: receipt, ready: true}, nil
}

// gateCandidate stages the health judge's proposal, verifies the staged
// candidate (live deployment checks, memory checks, independent detector
// evaluation), decides with operability.Decide and acts on the owner: accept
// into the operational branch, return a typed correction to the designer or
// the deployer, or end the run. A candidate whose base moved is staged and
// verified again.
func (c Coordinator) gateCandidate(ctx context.Context, id string, cfg Config, deployment DeploymentReceipt, log *contracts.ProposalRejectionLog) (gateOutcome, error) {
	if !deployment.Ready {
		return c.publishUnverified(ctx, id)
	}
	mode := c.mode(cfg)
	attempts := orDefault(cfg.DesignerAttempts, defaultDesignerAttempts)
	for restage := 0; ; restage++ {
		candidate, err := c.Broker.StageCandidate(ctx, id, broker.HealthJudge, broker.InitialPhase)
		if err != nil {
			if diagnostics, typed := broker.RejectionDiagnostics(err); typed {
				return gateOutcome{rejected: diagnostics}, nil
			}
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return gateOutcome{feedback: "broker rejected health diagnostics: " + err.Error()}, nil
		}
		if candidate.Accepted || candidate.Commit == "" {
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return gateOutcome{feedback: "health judge produced no new diagnostics commit to verify"}, nil
		}
		var report operability.Report
		var decision operability.Decision
		var pending *PendingVerificationError
		err = c.retryInfrastructure(ctx, cfg, "operability verification", func() error {
			var err error
			if report, pending, err = c.candidateReport(ctx, cfg, candidate, deployment); err != nil {
				return err
			}
			report = reattribute(report, log, attempts)
			decision = report.DecideIn(mode, operability.StageItems(operability.StageComplete)...)
			if decision.Kind == operability.Retry {
				return &retryChecks{decision}
			}
			return nil
		})
		if err != nil {
			_ = c.Broker.DiscardCandidate(id, broker.InitialPhase)
			return gateOutcome{}, err
		}
		switch decision.Kind {
		case operability.Accepted:
			receipt, err := c.Broker.Accept(ctx, candidate, broker.CandidateEvidence{Commit: candidate.Commit, Tree: candidate.Tree, EffectiveTree: deployment.EffectiveTree, PatchesDigest: deployment.PatchesDigest, Operability: &report})
			if errors.Is(err, broker.ErrCandidateStale) && restage < maxCandidateRestages {
				continue
			}
			if err != nil {
				_ = c.Broker.DiscardCandidate(id, broker.InitialPhase)
				return gateOutcome{}, fmt.Errorf("accept verified health diagnostics: %w", err)
			}
			return gateOutcome{receipt: receipt, ready: true}, nil
		case operability.Correct:
			_ = c.Broker.DiscardCandidate(id, broker.InitialPhase)
			if decision.Owner == operability.OwnerDesigner {
				return gateOutcome{rejected: operabilityDiagnostics(decision.Checks)}, nil
			}
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return gateOutcome{feedback: decision.Feedback()}, nil
		}
		_ = c.Broker.DiscardCandidate(id, broker.InitialPhase)
		if decision.Kind == operability.Wait && pending != nil {
			// The designer's probes never answered within the bounded window:
			// the correction carries what they observed. After the designer
			// bound the window itself is the terminal answer.
			if count, _ := priorDesignerCorrections(log); count < attempts && decisionHasPaths(decision) {
				return gateOutcome{rejected: operabilityDiagnostics(decision.Checks)}, nil
			}
			_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
			return gateOutcome{}, pending
		}
		_ = c.Broker.ReleaseProposal(id, broker.InitialPhase)
		return gateOutcome{}, c.terminal(mode, "verify", report, decision)
	}
}

// terminal is the error a run ends with when no agent can act on the decision:
// for a provided plan the typed plan error the harness already reads, otherwise
// an OperabilityError.
func (c Coordinator) terminal(mode operability.Mode, stage string, report operability.Report, decision operability.Decision) error {
	if mode == operability.ModeProvided {
		return &ProvidedPlanError{Stage: stage, Reason: decision.Feedback(), Report: &report}
	}
	return &OperabilityError{Report: report, Decision: decision}
}
