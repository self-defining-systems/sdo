package runtime

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

func TestSourceInventoryOnboardingConfirmsReachabilityReference(t *testing.T) {
	ctx := context.Background()
	goal := inventoryHealth().(*sequenceDetector)
	goal.spec.ID = "health-objective"
	staged, client := stagedInstallerController(t, goal)
	snapshot := reachabilityApp(8083)
	snapshot.NamespaceName = "app"
	snapshot.ServiceList[0].Namespace = "app"
	snapshot.PodList[0].Namespace = "app"
	for _, port := range []int32{8084, 8085} {
		snapshot.ServiceList[0].Spec.Ports = append(snapshot.ServiceList[0].Spec.Ports, corev1.ServicePort{Port: port, TargetPort: intstr.FromInt32(port)})
	}
	provider := &mutableSnapshots{snapshot: snapshot}
	reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}, Watches: servicehealth.ReachabilityWatches(), Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}})
	c, err := NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.OnError = func(err error) { t.Logf("controller error: %v", err) }
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	window := &servicehealth.ReachabilityWindow{Failures: 3, Interval: 10 * time.Second, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}}, {Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8084}}, {Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8085}}}}
	for round := 0; round < 20; round++ {
		now := time.Unix(int64(101+10*round), 0)
		window.ObservedAt = now
		for i := range window.Targets {
			sample := servicehealth.DialSample{At: now, OK: i == 0}
			if !sample.OK {
				sample.Class = servicehealth.DialRefused
			}
			window.Targets[i].Samples = append(window.Targets[i].Samples, sample)
			if sample.OK {
				at := now
				window.Targets[i].LastSuccess = &at
			}
		}
		snapshot.Reachability = window
		provider.set(snapshot)
		if err := c.EvaluateAll(ctx, now); err != nil {
			t.Fatal(err)
		}
		record, err := onboarding.Load(ctx, client, "control", "app")
		if err != nil {
			t.Fatal(err)
		}
		if round < 2 {
			if record != nil {
				t.Fatal("unconfirmed probes prematurely finalized onboarding")
			}
			if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unconfirmed window consumed attempt: %v", err)
			}
			if c.completeHealthyEvaluation() {
				t.Fatal("unconfirmed detector was recorded clear")
			}
		} else if record == nil {
			t.Fatalf("confirmed healthy reference did not finalize onboarding: state=%+v history=%+v active=%+v", c.onboardingReachability, c.history, c.tracker.ActiveFindings([]string{"service-reachability"}, nil))
		}
		if len(c.tracker.ActiveFindings([]string{"service-reachability"}, nil)) != 0 || c.IncidentOpen() {
			t.Fatal("never-served ports became an incident")
		}
	}
	// Restart consumes the same immutable reference, without recapture.
	reference, err := onboarding.LoadReachability(ctx, client, "control", "app")
	if err != nil || reference == nil || len(reference.Exclusions) != 2 {
		t.Fatalf("durable reference: %+v %v", reference, err)
	}
	restarted, err := NewController(c.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	c = restarted
	bound, err := c.provider.Snapshot(ctx)
	if err != nil || len(servicehealth.ReachabilityObservations(bound)) != 2 {
		t.Fatalf("restart lost typed observations: %v", err)
	}
	// A warm reinstall keeps the exact record, including reference bytes.
	warm, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := onboarding.Stage(ctx, client, "control", "app", c.config.SourceCommit, nil, time.Unix(350, 0)); err != nil {
		t.Fatal(err)
	}
	unchanged, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.ConfigMapName, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(warm, unchanged) {
		t.Fatalf("warm reinstall changed record: %v", err)
	}
	// Persist the idle warm restart, including its accepted onboarding authority.
	checkpoint := lifecycleio.NewConfigMapStateStore(client, "control", "sdo-controller-state")
	if _, err := checkpoint.Save(ctx, exportState(t, c), ""); err != nil {
		t.Fatal(err)
	}
	// A served port retains exactly the existing three-sample fault latency.
	for round := 0; round < 3; round++ {
		now := time.Unix(int64(301+10*round), 0)
		window.ObservedAt = now
		for i := range window.Targets {
			window.Targets[i].Samples = append(window.Targets[i].Samples, servicehealth.DialSample{At: now, Class: servicehealth.DialRefused})
		}
		provider.set(snapshot)
		if err := c.EvaluateAll(ctx, now); err != nil {
			t.Fatal(err)
		}
		active := c.tracker.ActiveFindings([]string{"service-reachability"}, nil)
		if round < 2 && len(active) != 0 {
			t.Fatal("fault fired before confirmation")
		}
		if round == 2 && len(active) != 1 {
			t.Fatalf("served-port fault latency changed: %+v", active)
		}
	}
	// The supported fresh reset retires the legacy authority and attempt.
	// Reinstall on this same lane must establish a new measured reference.
	if _, err := onboarding.ResetControlState(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	if err := onboarding.Stage(ctx, client, "control", "app", c.config.SourceCommit, nil, time.Unix(400, 0)); err != nil {
		t.Fatal(err)
	}
	fresh, err := NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(400, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.AttachStateStore(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := fresh.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	for i := range window.Targets {
		window.Targets[i].Samples = nil
		window.Targets[i].LastSuccess = nil
	}
	for round := 0; round < 3; round++ {
		now := time.Unix(int64(401+10*round), 0)
		window.ObservedAt = now
		for i := range window.Targets {
			sample := servicehealth.DialSample{At: now, OK: i == 0}
			if !sample.OK {
				sample.Class = servicehealth.DialRefused
			}
			window.Targets[i].Samples = append(window.Targets[i].Samples, sample)
			if sample.OK {
				at := now
				window.Targets[i].LastSuccess = &at
			}
		}
		provider.set(snapshot)
		if err := fresh.EvaluateAll(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record == nil || !record.EstablishedAt.AsTime().Equal(time.Unix(400, 0)) {
		t.Fatalf("fresh install reused old record: %+v %v", record, err)
	}
	freshReference, err := onboarding.LoadReachability(ctx, client, "control", "app")
	if err != nil || freshReference == nil || len(freshReference.Exclusions) != 2 || !freshReference.CapturedAt.Equal(time.Unix(421, 0)) {
		t.Fatalf("fresh reference missing: %+v %v", freshReference, err)
	}
	t.Log("install_to_record=21s; confirmed fault=20s after first refusal (unchanged three rounds at 10s)")
}

// A provisional reference cannot be learned from a window that contained an
// independently unhealthy goal, changed configuration, or uncertain probes.
func TestSourceInventoryOnboardingRejectsUnhealthyReachabilityReference(t *testing.T) {
	for _, mode := range []string{"goal", "configuration", "timeout", "transport", "commit", "served", "restart-clear", "restart-goal", "incident"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			goal := inventoryHealth().(*sequenceDetector)
			goal.spec.ID = "health-objective"
			staged, client := stagedInstallerController(t, goal)
			snapshot := reachabilityApp(8083)
			snapshot.NamespaceName = "app"
			snapshot.ServiceList[0].Namespace = "app"
			snapshot.PodList[0].Namespace = "app"
			provider := &mutableSnapshots{snapshot: snapshot}
			reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}, Watches: servicehealth.ReachabilityWatches(), Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}})
			c, err := NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
				t.Fatal(err)
			}
			w := &servicehealth.ReachabilityWindow{Failures: 3, Interval: 10 * time.Second, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}}}}
			for round := 0; round < 3; round++ {
				now := time.Unix(int64(101+10*round), 0)
				if mode == "restart-goal" && round == 0 {
					goal.errors = map[int]error{goal.calls: fmt.Errorf("goal observation interrupted")}
				}
				if (mode == "restart-clear" || mode == "restart-goal") && round == 1 {
					c, err = NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, now)
					if err != nil {
						t.Fatal(err)
					}
					if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "incident" && round == 1 {
					c.lastAcknowledgedIncidentID = "incident-during-window"
				}
				class := servicehealth.DialRefused
				if mode == "timeout" {
					class = servicehealth.DialTimeout
				}
				if mode == "transport" {
					w.LoadError = "prober unavailable"
				}
				if mode == "goal" && round == 0 {
					goal.errors = map[int]error{goal.calls: fmt.Errorf("independent goal unavailable")}
				}
				if mode == "commit" {
					c.config.SourceCommit = strings.Repeat("b", 40)
				}
				if mode == "configuration" && round == 1 {
					snapshot.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8888)
				}
				w.ObservedAt = now
				w.Targets[0].Samples = append(w.Targets[0].Samples, servicehealth.DialSample{At: now, Class: class, OK: mode == "served" && round == 0})
				snapshot.Reachability = w
				provider.set(snapshot)
				if err := c.EvaluateAll(ctx, now); err != nil {
					t.Fatal(err)
				}
			}
			if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
				t.Fatalf("unhealthy window granted authority: %v %v", record, err)
			}
			if mode == "restart-clear" || mode == "restart-goal" || mode == "incident" {
				if marked, err := onboarding.Marked(ctx, client, "control", onboarding.RejectedConfigMapName); err != nil || !marked {
					t.Fatalf("window rejection not durable: %v %v", marked, err)
				}
				if marked, err := onboarding.Marked(ctx, client, "control", onboarding.AttemptConfigMapName); err != nil || marked {
					t.Fatalf("unready window consumed sole attempt: %v %v", marked, err)
				}
			}
		})
	}
}

func TestSourceInventoryOnboardingConfirmationExpiryGrantsNoAuthority(t *testing.T) {
	goal := inventoryHealth().(*sequenceDetector)
	goal.spec.ID = "health-objective"
	goal.spec.Interval = time.Hour
	goal.spec.Watches = []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}
	staged, client := stagedInstallerController(t, goal)
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: 10 * time.Second})
	// Incomplete observation is a pending verdict, even with no finding.
	config := staged.config
	c, err := NewController(config, []sdk.Detector{goal, detector}, staged.provider, staged.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AttachPendingOnboarding(context.Background(), client, "control"); err != nil {
		t.Fatal(err)
	}
	if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if c.pendingOnboarding == nil {
		t.Fatal("confirmation did not get its window")
	}
	if err := c.StepEvents(context.Background(), time.Unix(135, 0), nil); err != nil {
		t.Fatal(err)
	}
	if c.pendingOnboarding != nil {
		t.Fatal("missing probes defer inventory indefinitely")
	}
	if record, err := onboarding.Load(context.Background(), client, "control", "app"); err != nil || record != nil {
		t.Fatalf("expiry granted authority: %+v %v", record, err)
	}
}

func TestSourceInventoryLegacyOnboardingReferenceAbsentFailsSafe(t *testing.T) {
	ctx := context.Background()
	goal := inventoryHealth().(*sequenceDetector)
	staged, client := stagedInstallerController(t, goal)
	pending, err := onboarding.Pending(ctx, client, "control", "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := onboarding.Complete(ctx, client, "control", pending); err != nil {
		t.Fatal(err)
	}
	snapshot := reachabilityApp(8083)
	snapshot.NamespaceName = "app"
	snapshot.ServiceList[0].Namespace = "app"
	snapshot.PodList[0].Namespace = "app"
	now := time.Unix(121, 0)
	snapshot.Reachability = &servicehealth.ReachabilityWindow{ObservedAt: now, Failures: 3, Interval: 10 * time.Second, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}, Samples: []servicehealth.DialSample{{At: time.Unix(101, 0), Class: servicehealth.DialRefused}, {At: time.Unix(111, 0), Class: servicehealth.DialRefused}, {At: now, Class: servicehealth.DialRefused}}}}}
	reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}, Watches: servicehealth.ReachabilityWatches(), Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}})
	c, err := NewController(staged.config, []sdk.Detector{goal, reach}, &mutableSnapshots{snapshot: snapshot}, staged.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	bound, err := c.provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := servicehealth.ReachabilityObservations(bound); len(got) != 0 {
		t.Fatalf("legacy record granted exclusions: %+v", got)
	}
	findings, err := reach.Detect(ctx, bound)
	if err != nil || len(findings) != 1 {
		t.Fatalf("legacy never-served port must retain ordinary finding: %+v %v", findings, err)
	}
	_, note, err := onboarding.LoadReachabilityWithNote(ctx, client, "control", "app")
	if err != nil || note == nil || note.Status != "reference_absent" || note.ConfigMap != onboarding.ConfigMapName || note.Key != onboarding.ReachabilityDataKey {
		t.Fatalf("missing typed note: %+v %v", note, err)
	}
}

func TestSourceInventoryOnboardingRejectsLateConfirmedSweep(t *testing.T) {
	ctx := context.Background()
	goal := inventoryHealth().(*sequenceDetector)
	goal.spec.Interval = time.Hour
	staged, client := stagedInstallerController(t, goal)
	snapshot := reachabilityApp(8083)
	snapshot.NamespaceName = "app"
	snapshot.ServiceList[0].Namespace = "app"
	snapshot.PodList[0].Namespace = "app"
	window := &servicehealth.ReachabilityWindow{Failures: 3, Interval: 10 * time.Second, ObservedAt: time.Unix(101, 0), Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}}}}
	snapshot.Reachability = window
	provider := &mutableSnapshots{snapshot: snapshot}
	reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: 10 * time.Second})
	c, err := NewController(staged.config, []sdk.Detector{goal, reach}, provider, staged.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	if err := c.StepEvents(ctx, time.Unix(101, 0), nil); err != nil {
		t.Fatal(err)
	}
	for _, second := range []int64{101, 111, 135} {
		window.Targets[0].Samples = append(window.Targets[0].Samples, servicehealth.DialSample{At: time.Unix(second, 0), OK: true})
	}
	window.ObservedAt = time.Unix(135, 0)
	window.Targets[0].LastSuccess = &window.ObservedAt
	provider.set(snapshot)
	if !detectorConfirmed(reach, snapshot) {
		t.Fatal("fixture must provide a clear confirmed verdict at expiry")
	}
	if err := c.StepEvents(ctx, window.ObservedAt, nil); err != nil {
		t.Fatal(err)
	}
	if c.pendingOnboarding != nil {
		t.Fatal("late confirmation did not consume the expired attempt")
	}
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("late clear verdict extended the confirmation budget: %v %v", record, err)
	}
}
