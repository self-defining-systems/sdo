package runtime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestUnencodableActionRetainsRecoveryWrite(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	action := program.SetField{Ref: ref, Path: "spec.replicas"}
	for _, name := range []string{"unobserved generation", "partial failure", "pending rollout"} {
		t.Run(name, func(t *testing.T) {
			app := RecordedPlanApplication{ID: "app", Result: PlanApplicationResult{Applied: true, Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}}}
			workload := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, Generation: 2}}
			if name == "partial failure" {
				app.Error = "second action failed"
				app.Result.Applied = false
				workload.Status.ObservedGeneration = 2
			}
			if name == "pending rollout" {
				app.Result.RolloutPending = &contracts.RolloutPending{}
				workload.Status.ObservedGeneration = 2
			}
			view := sdktest.Snapshot{DeploymentList: []appsv1.Deployment{workload}}
			check := func(got RecordedPlanApplication) {
				t.Helper()
				writes, _, settled := sdoWrites(verificationInput{result: &IncidentResult{PlanApplications: []RecordedPlanApplication{got}}, view: view})
				touched := touchedObjects(got.Result)
				if len(writes) != 1 || settled != (name == "partial failure") || len(touched) != 1 || objectRefState(touched[0]) != ref {
					t.Fatalf("writes=%v settled=%v touched=%v", writes, settled, touched)
				}
			}
			check(app)
			wire, err := recordedPlanContract(app)
			if err != nil {
				t.Fatal(err)
			}
			restored := planApplicationState(wire)
			check(restored)
			raw, err := json.Marshal(restored.Result)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &restored.Result); err != nil {
				t.Fatal(err)
			}
			check(restored)
		})
	}
}

func TestResultProjectionKeepsGoodEvidenceWithMalformedFields(t *testing.T) {
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{}, verifyT0)
	if err != nil {
		t.Fatal(err)
	}
	result := completedResult("synthetic")
	result.ResponderSessionID = "session"
	result.ResolutionPath = "responder"
	result.Error = "failed with \xff"
	result.ProposedArtifactIDs = []string{"/invalid/reference", "valid-playbook"}
	result.PlanApplications = []RecordedPlanApplication{{ID: "recorded"}}
	c.currentIncidentResult, c.dispatchError = &result, "dispatch failed with \xff"
	check := func(got *contracts.IncidentResult) {
		t.Helper()
		if got.GetStatus() != string(result.Status) || got.GetResponderSessionId() != "session" ||
			got.GetResolutionPath() != "responder" || got.GetError() != "failed with �" ||
			len(got.GetProposedArtifactIds()) != 1 || got.GetProposedArtifactIds()[0] != "valid-playbook" ||
			len(got.GetPlanApplications()) != 1 || got.GetPlanApplications()[0].GetApplicationId() != "recorded" {
			t.Fatalf("result projection discarded good fields: %v", got)
		}
		if _, err := protojson.Marshal(got); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.GetDispatchError() != "dispatch failed with �" {
		t.Fatalf("dispatch error = %q", checkpoint.GetDispatchError())
	}
	check(checkpoint.GetIncidentResult())
	dispatched, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	check(dispatched)
	stage, err := incidentRequestToProto(IncidentRequest{PriorStages: []*contracts.StageReport{{Error: result.Error}}})
	if err != nil || stage.GetPriorStages()[0].GetError() != "failed with �" {
		t.Fatalf("prior stage = %v, %v", stage, err)
	}
	closure, err := incidentClosureContract(IncidentClosure{Result: &result, DispatchError: c.dispatchError})
	if err != nil || closure.GetDispatchError() != checkpoint.GetDispatchError() {
		t.Fatalf("closure = %v, %v", closure, err)
	}
	check(closure.GetResult())
}

type failingFreezeUsage struct {
	recordingDispatcher
	calls int
}

func (d *failingFreezeUsage) ResponderUsage(string) (*contracts.UsageMetrics, error) {
	d.calls++
	return nil, errors.New("synthetic freeze failure")
}

func TestClosureFreezeRetryStopsAndReportsOnce(t *testing.T) {
	now := verifyT0
	clock := &settlingClock{now: now}
	world := &settlingDetector{id: "service-health", class: sdk.DetectorClassHealth, clock: clock, healAt: now.Add(time.Second)}
	findings := []sdk.Finding{{DetectorID: world.id, RuleID: world.id, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{Kind: "Pod", APIVersion: "v1", Namespace: "app", Name: "worker"}, Evidence: "synthetic", Summary: "unready"}}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.ClosureRetry.MaxAttempts = 2
	dispatcher := &failingFreezeUsage{}
	c, err := NewController(cfg, []sdk.Detector{world}, staticProvider{}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = clock.Now
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: findings,
		OpeningKeys: findingKeys(findings), ResponseDeadline: now.Add(time.Minute)}
	c.incidentOpen = true
	c.incidentDetectedAt, c.incidentDispatchedAt = now, now
	result := completedResult("synthetic")
	c.handleDispatchCompletion(dispatchCompletion{incidentID: result.IncidentID, result: result}, now)
	var reports int
	c.OnError = func(err error) {
		if strings.Contains(err.Error(), "synthetic freeze failure") {
			reports++
		}
	}
	autoStep(t, c, clock, time.Second)
	autoStep(t, c, clock, 5*time.Second)
	for range 4 {
		autoStep(t, c, clock, time.Second)
	}
	if dispatcher.calls != 2 || reports != 1 {
		t.Fatalf("freeze attempts=%d cause reports=%d, want 2 and 1", dispatcher.calls, reports)
	}
}
