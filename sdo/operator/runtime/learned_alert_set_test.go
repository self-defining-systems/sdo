package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/requestmetrics"
)

// routeFix is a learned playbook for one symptom: it links the findings that
// name it as a playbook hint and does nothing else.
type routeFix struct{ id string }

func (p routeFix) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.id} }
func (p routeFix) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}
func (p routeFix) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{}, nil
}

// routeErrors is the real-traffic alert of the fault's own route, naming the
// learned playbook.
func routeErrors(t *testing.T, playbook string) sdk.Finding {
	t.Helper()
	finding := sdk.Finding{DetectorID: memory.RequestMetricsDetectorID, RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive,
		Severity: sdk.SeverityWarning, Summary: "operation commit-file: 9 of 20 requests failed", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "demo", Name: "web"}, Playbooks: []string{playbook}}
	finding.Fingerprint = FindingFingerprint(finding)
	return finding
}

// churnController is an incident that opened on routine churn, whose responder
// is running with the learned playbook loaded. Each automatic pass attempt
// shows as one count of the health check that guards it.
func churnController(t *testing.T, now time.Time) (*Controller, *atomic.Int32) {
	t.Helper()
	c, _ := alertSetController(t, now, &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"})
	c.config.RequestMetricsOpens = true
	c.responderDone = false
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	registry, err := program.NewRegistry([]program.Diagnoser{routeFix{id: "route-fix"}}, []program.Mitigator{routeFix{id: "route-fix"}})
	if err != nil {
		t.Fatal(err)
	}
	c.responderLoop = &ResponderLoop{Programs: registry}
	passes := new(atomic.Int32)
	c.CheckHealth = func(context.Context) error {
		passes.Add(1)
		return errors.New("synthetic: stop after counting the pass")
	}
	return c, passes
}

// The fault's own symptom joins the alert set after a churn alert opened the
// incident. The learned playbook for it is evaluated when it joins.
func TestJoinedAlertIsEvaluatedAgainstLearnedPlaybooks(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, passes := churnController(t, now)
	c.admitActivation(context.Background(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	c.Wait()
	if len(c.currentIncidentRequest.JoinedKeys) != 1 {
		t.Fatalf("the symptom did not join the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	if passes.Load() != 1 {
		t.Fatalf("automatic passes = %d, want one for the joined symptom", passes.Load())
	}
}

// A pass that found nothing to do settles the stage; a symptom that joins
// later is new evidence, so the stage looks again.
func TestNewJoinedSymptomReopensASettledStage(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, passes := churnController(t, now)
	c.midStage.settled = true
	c.admitActivation(context.Background(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	c.Wait()
	if passes.Load() != 1 {
		t.Fatalf("automatic passes = %d, want one: a settled stage ignored a new symptom", passes.Load())
	}
}

// A playbook's restart makes the opening workload alert re-fire after the
// stage first saw it clear, and the verification grace runs out while the
// alerts are already clear and only the stability window is still counting.
// Escalation says the responder returned before Recovered held; Recovered is
// holding, so the incident must close success instead.
func TestIncidentDoesNotEscalateWhileTheStabilityWindowRuns(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	condition := &switchedProbe{id: memory.KubernetesConditionDetectorIDs()[0], scenario: "not-ready"}
	c, closed := alertSetController(t, now, condition)
	opening := sdk.Finding{DetectorID: condition.id, RuleID: "workload-container-not-ready", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "web not ready", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}}
	opening.Fingerprint = FindingFingerprint(opening)
	c.currentIncidentRequest.Findings, c.currentIncidentRequest.OpeningKeys = []sdk.Finding{opening}, findingKeys([]sdk.Finding{opening})
	c.incidentFindingKeys = findingKeys([]sdk.Finding{opening})
	c.config.ClosureStabilityWindow = 20 * time.Second
	c.config.VerificationTimeout = 5 * time.Second
	for range 40 {
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		if !c.escalatedAt.IsZero() {
			t.Fatalf("escalated at %s with clear alerts and a running stability window (completed %s)", now, c.responderCompletedAt)
		}
		now = now.Add(time.Second)
	}
	if closed.Result == nil || closed.Result.Status != IncidentCompleted {
		t.Fatalf("the incident did not close after the window: %+v", closed.Result)
	}
}

// A symptom that joins while the first response pipeline is running is not
// lost: one pass evaluates it when the pipeline ends.
func TestSymptomJoiningDuringThePipelineIsEvaluatedAfterIt(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, passes := churnController(t, now)
	c.responseStagesRunning = true
	c.admitActivation(context.Background(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	c.resumePendingLearnedPass(context.Background())
	c.Wait()
	if passes.Load() != 0 {
		t.Fatalf("a pass ran while the pipeline was still running: %d", passes.Load())
	}
	c.mu.Lock()
	c.responseStagesRunning = false
	c.mu.Unlock()
	c.resumePendingLearnedPass(context.Background())
	c.Wait()
	if passes.Load() != 1 {
		t.Fatalf("automatic passes = %d, want one after the pipeline ended", passes.Load())
	}
}

// A symptom that joins while a pass is running queues one more pass, and the
// running pass finding nothing does not settle the stage over it.
func TestSymptomJoiningDuringAPassQueuesOneMorePass(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, _ := churnController(t, now)
	c.midStage.running = true
	c.admitActivation(context.Background(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	if !c.midStage.dirty {
		t.Fatal("the joined symptom queued no pass behind the running one")
	}
}
