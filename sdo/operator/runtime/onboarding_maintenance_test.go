package runtime

import (
	"context"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"testing"
	"time"
)

func TestSourceInventoryOnboardingMaintenanceRequiresFreshSamples(t *testing.T) {
	ctx := context.Background()
	goal := inventoryHealth().(*sequenceDetector)
	goal.spec.ID = "health-objective"
	staged, client := stagedInstallerController(t, goal)
	snapshot := reachabilityApp(8083)
	snapshot.NamespaceName = "app"
	snapshot.ServiceList[0].Namespace = "app"
	snapshot.PodList[0].Namespace = "app"
	window := &servicehealth.ReachabilityWindow{ObservedAt: time.Unix(121, 0), Failures: 3, Interval: 10 * time.Second, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}}}}
	for _, second := range []int64{101, 111, 121} {
		window.Targets[0].Samples = append(window.Targets[0].Samples, servicehealth.DialSample{At: time.Unix(second, 0), OK: true})
	}
	snapshot.Reachability = window
	provider := &mutableSnapshots{snapshot: snapshot}
	reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}})
	c, err := NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	if err := c.ResumeObservation(ctx, time.Unix(130, 0)); err != nil {
		t.Fatal(err)
	}
	if marked, err := onboarding.Marked(ctx, client, "control", onboarding.AttemptConfigMapName); err != nil || marked {
		t.Fatalf("pre-resume samples consumed attempt: %v %v", marked, err)
	}
	for _, second := range []int64{131, 141, 151} {
		window.Targets[0].Samples = append(window.Targets[0].Samples, servicehealth.DialSample{At: time.Unix(second, 0), OK: true})
	}
	window.ObservedAt = time.Unix(151, 0)
	window.Targets[0].LastSuccess = &window.ObservedAt
	provider.set(snapshot)
	if err := c.EvaluateAll(ctx, window.ObservedAt); err != nil {
		t.Fatal(err)
	}
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record == nil {
		t.Fatalf("fresh post-resume window did not finalize: %v %v", record, err)
	}
}
