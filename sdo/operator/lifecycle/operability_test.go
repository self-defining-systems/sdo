package lifecycle_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
)

// offlineReadiness adds the offline checks to the fake deployer: results are
// consumed one per CheckOffline call and the last one repeats.
type offlineReadiness struct {
	*fakeReadiness
	results []offlineResult
	calls   int
}

type offlineResult struct {
	checks []operability.Check
	err    error
}

func (r *offlineReadiness) CheckOffline(_ context.Context, checkout string, _ *contracts.DeploymentPlan) (lifecycle.OfflineChecks, error) {
	index := min(r.calls, len(r.results)-1)
	r.calls++
	res := r.results[index]
	if res.err != nil {
		return lifecycle.OfflineChecks{}, res.err
	}
	out := lifecycle.OfflineChecks{Checks: res.checks}
	if !strings.Contains(checkout, "never") {
		out.BaseTree, out.EffectiveTree, out.PatchesDigest, out.RenderDigest, out.EnvDigest = "tree", "effective", "patches", "render", "env"
	}
	return out, nil
}

func ck(id string, status operability.Status, cause operability.Cause, detail string) operability.Check {
	return operability.Check{ID: id, Phase: operability.PhaseOffline, Status: status, Cause: cause, Detail: detail}
}

// passing is a complete deployment-stage check set.
func passing() []operability.Check {
	return []operability.Check{
		ck("O1.render", operability.StatusPass, "", "ok"),
		ck("O2.images", operability.StatusPass, "", "ok"),
		ck("O6.scope", operability.StatusNote, "", "1 foreign-namespace objects"),
	}
}

func failing(c operability.Check) []operability.Check {
	return []operability.Check{c, ck("O2.images", operability.StatusSkipped, "", "not run"), ck("O6.scope", operability.StatusSkipped, "", "not run")}
}

func withOffline(h *harness, results ...offlineResult) *offlineReadiness {
	r := &offlineReadiness{fakeReadiness: h.readiness, results: results}
	h.coordinator.Readiness = r
	return r
}

func TestOfflineGatePassesThroughToApply(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := withOffline(h, offlineResult{checks: passing()})
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if r.calls != 1 || h.readiness.calls != 1 || h.workers.deployerAttempts() != 1 || h.installer.installs != 1 {
		t.Fatalf("offline=%d apply=%d attempts=%d installs=%d", r.calls, h.readiness.calls, h.workers.deployerAttempts(), h.installer.installs)
	}
}

func TestOfflineDeployerFailureIsCorrectedBeforeAnyApply(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := withOffline(h,
		offlineResult{checks: failing(ck("O1.patches", operability.StatusFail, operability.CausePatch, "patch conflict 0001-x.patch"))},
		offlineResult{checks: passing()},
	)
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 2 || r.calls != 2 || h.readiness.calls != 1 {
		t.Fatalf("attempts=%d offline=%d apply=%d: the failing attempt must not reach the cluster", result.Attempts, r.calls, h.readiness.calls)
	}
	feedback := h.workers.deployer[1].GetCorrectionFeedback()
	if !strings.Contains(feedback, "O1.patches [patch]: patch conflict 0001-x.patch") || !strings.Contains(feedback, "no cluster was touched") {
		t.Fatalf("deployer feedback = %q", feedback)
	}
}

func TestOfflineProvidedPlanFailureIsTerminalAndCarriesTheReport(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := withOffline(h, offlineResult{checks: failing(ck("O1.render", operability.StatusFail, operability.CauseRender, "chart values: unknown key"))})
	_, err := h.coordinator.Run(context.Background(), providedConfig(providedPlans(t)["helm"]))
	var terminal *lifecycle.ProvidedPlanError
	if !errors.As(err, &terminal) || terminal.Stage != "deploy" || terminal.Report == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("error = %v, want a deploy-stage ProvidedPlanError with the report", err)
	}
	if h.readiness.calls != 0 || h.workers.deployerAttempts() != 0 || r.calls != 1 {
		t.Fatalf("apply=%d agent attempts=%d offline=%d", h.readiness.calls, h.workers.deployerAttempts(), r.calls)
	}
	s := terminal.Report.Subject
	if s.AcceptedCommit == "" || s.AcceptedCommit == operability.Unavailable || s.EffectiveTree != "effective" {
		t.Fatalf("subject = %+v", s)
	}
	if err := terminal.Report.Validate(); err != nil {
		t.Fatalf("terminal report is not a valid report: %v", err)
	}
	if d := terminal.Report.DecideIn(operability.ModeProvided, operability.StageItems(operability.StageDeployment)...); d.Owner != operability.OwnerHuman {
		t.Fatalf("a provided plan's render failure routes to %s", d.Owner)
	}
}

func TestOfflineProvidedPlanNotesDoNotBlock(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	withOffline(h, offlineResult{checks: passing()})
	if _, err := h.coordinator.Run(context.Background(), providedConfig(providedPlans(t)["raw"])); err != nil {
		t.Fatal(err)
	}
	if h.readiness.calls != 1 {
		t.Fatalf("apply calls = %d", h.readiness.calls)
	}
}

func TestOfflineInfrastructureCauseRetriesWithoutSpendingAnAttempt(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := withOffline(h,
		offlineResult{checks: failing(ck("O1.patches", operability.StatusFail, operability.CauseInfrastructure, "git unavailable"))},
		offlineResult{checks: passing()},
	)
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 1 || r.calls != 2 || len(h.waits) != 1 || h.workers.deployerAttempts() != 1 {
		t.Fatalf("attempts=%d offline=%d waits=%v deployer attempts=%d", result.Attempts, r.calls, h.waits, h.workers.deployerAttempts())
	}

	h = newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r = withOffline(h, offlineResult{checks: failing(ck("O1.patches", operability.StatusFail, operability.CauseInfrastructure, "git unavailable"))})
	_, err = h.coordinator.Run(context.Background(), config())
	var infra *lifecycle.InfrastructureError
	if !errors.As(err, &infra) || infra.Attempts != 3 || r.calls != 3 || h.workers.deployerAttempts() != 1 || h.readiness.calls != 0 {
		t.Fatalf("err=%v offline=%d attempts=%d apply=%d", err, r.calls, h.workers.deployerAttempts(), h.readiness.calls)
	}
}

func TestOfflineNonAgentCauseEndsTheRunWithTheReport(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	withOffline(h, offlineResult{checks: failing(ck("O1.render", operability.StatusFail, operability.CauseInternal, "no render function was supplied"))})
	_, err := h.coordinator.Run(context.Background(), config())
	var opErr *lifecycle.OperabilityError
	if !errors.As(err, &opErr) || opErr.Decision.Owner != operability.OwnerHuman || opErr.Report.Stage != operability.StageDeployment {
		t.Fatalf("err = %v, want an OperabilityError routed to the human", err)
	}
	if h.workers.deployerAttempts() != 1 || h.readiness.calls != 0 {
		t.Fatalf("attempts=%d apply=%d: no correction and no apply for a non-agent cause", h.workers.deployerAttempts(), h.readiness.calls)
	}
}

func TestOfflineCheckerErrorFailsTheRun(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	withOffline(h, offlineResult{err: errors.New("tree digest: not a repository")})
	_, err := h.coordinator.Run(context.Background(), config())
	if err == nil || !strings.Contains(err.Error(), "not a repository") || h.readiness.calls != 0 {
		t.Fatalf("err=%v apply=%d", err, h.readiness.calls)
	}
}

// A readiness verifier without CheckOffline keeps today's behavior exactly.
func TestReadinessWithoutOfflineChecksSkipsTheGate(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	if _, ok := any(h.readiness).(lifecycle.OfflineChecker); ok {
		t.Fatal("the plain fake must not implement the gate")
	}
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
}

// The deployed source is published with a stage-deployment report that holds the
// offline checks and the receipt's live checks, bound to the receipt's identity
// and to the commit the deployer's proposal was based on.
func TestDeployedSourceIsPublishedWithItsDeploymentReport(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	withOffline(h, offlineResult{checks: passing()})
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	fake := h.coordinator.Broker.(*fakeBroker)
	report, found, err := fake.OperabilityReport(context.Background(), "op-source-1", operability.StageDeployment)
	if err != nil || !found {
		t.Fatalf("no deployment report was published: found=%v err=%v", found, err)
	}
	if report.Stage != operability.StageDeployment || report.Subject.EffectiveTree != "effective" || report.Subject.AcceptedCommit == "" || report.Subject.AcceptedCommit == operability.Unavailable {
		t.Fatalf("report subject/stage = %+v %s", report.Subject, report.Stage)
	}
	if decision := report.Decide(operability.StageItems(operability.StageDeployment)...); decision.Kind != operability.Accepted {
		t.Fatalf("published report is not accepted: %+v", decision)
	}
	ids := map[string]bool{}
	for _, item := range report.Items {
		for _, check := range item.Checks {
			ids[check.ID] = true
		}
	}
	if !ids["O1.deploy"] || !ids["O6.budget"] {
		t.Fatalf("report lacks the receipt's live checks: %v", ids)
	}
}
