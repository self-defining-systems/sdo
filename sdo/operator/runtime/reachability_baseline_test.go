package runtime

import (
	"context"
	"errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"net"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"strings"
	"syscall"
	"testing"
	"time"
)

func baselineRepository(t *testing.T) (string, *broker.Broker) {
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	old := filepath.Join(root, ".sdo/diagnostics/detectors/health/health")
	next := filepath.Join(root, ".sdo/diagnostics/detectors/health/health-objective")
	if err := os.Rename(old, next); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(next, "manifest.yaml")
	raw, _ := os.ReadFile(manifest)
	os.WriteFile(manifest, []byte(strings.ReplaceAll(string(raw), "id: health\n", "id: health-objective\n")), 0644)
	pb := filepath.Join(root, ".sdo/playbooks/health-objective")
	if err := os.MkdirAll(pb, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pb, "README.md"), []byte("---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\nRepair <RESOURCE_NAME>\n"), 0644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(root, ".sdo/playbooks/README.md")
	data, _ := os.ReadFile(index)
	os.WriteFile(index, append(data, []byte("- [health-objective](health-objective/README.md)\n")...), 0644)
	if err := memory.PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "objective")
	authority, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	return root, authority
}

func TestReachabilityBaselineBrokerPublicationAndRestart(t *testing.T) {
	ctx := context.Background()
	root, authority := baselineRepository(t)
	now := time.Now().UTC()
	s := reachabilityApp(9600)
	s.ServiceList[0].Name = "opensearch"
	s.Reachability = &servicehealth.ReachabilityWindow{ObservedAt: now, Interval: time.Second, Failures: 3, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "opensearch", Port: 9600}, Samples: []servicehealth.DialSample{{At: now.Add(-2 * time.Second), Class: servicehealth.DialRefused}, {At: now.Add(-time.Second), Class: servicehealth.DialRefused}, {At: now, Class: servicehealth.DialRefused}}}}}
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	revision, err := memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := publishReachabilityBaseline(ctx, root, authority, revision, []sdk.Detector{goal}, s)
	if err != nil || !accepted {
		t.Fatalf("baseline not accepted: %v %v", accepted, err)
	}
	baseline, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil || baseline == nil || len(baseline.Exclusions) != 1 {
		t.Fatalf("no restart-visible memory: %+v %v", baseline, err)
	}
	if msg := nativeGit(t, root, "log", "-1", "--format=%B"); !strings.Contains(msg, "SDO-Actor: health_judge") {
		t.Fatalf("wrong authority: %s", msg)
	}
	head := nativeGit(t, root, "rev-parse", "HEAD")
	revision, err = memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = publishReachabilityBaseline(ctx, root, authority, revision, []sdk.Detector{goal}, s)
	if err != nil || accepted || nativeGit(t, root, "rev-parse", "HEAD") != head {
		t.Fatalf("restart republished: %v %v", accepted, err)
	}
	goal = controllerDetector("health-objective", time.Second, stateFinding("health-objective"))
	goal.spec.Class = sdk.DetectorClassHealth
	if _, err := healthyBaselineGoal(ctx, []sdk.Detector{goal}, s); err == nil {
		t.Fatal("unhealthy objective authorized exclusion")
	}
}

func TestStartupReachabilityBaselineNeverCapturesRestoredIncident(t *testing.T) {
	root := nativeRepository(t)
	authority, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Controller{{incidentOpen: true}, {currentIncidentRequest: &IncidentRequest{}}, {pendingClosure: &IncidentClosure{}, closureState: contracts.ClosureState_CLOSURE_STATE_PENDING}, {closureReceipt: &ClosureReceipt{}, closureState: contracts.ClosureState_CLOSURE_STATE_COMMITTED}, {pendingClosure: &IncidentClosure{}, closureReceipt: &ClosureReceipt{}, closureState: contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING}} {
		// Nil observation dependencies deliberately panic if a restored incident
		// reaches measurement, rather than proving only that publication was skipped.
		if err := acceptStartupReachabilityBaseline(context.Background(), c, root, authority, true, &ServiceReachabilityObserver{}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := acceptStartupReachabilityBaseline(context.Background(), &Controller{}, root, authority, false, &ServiceReachabilityObserver{}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestStartupReachabilityBaselineMeasuresPublishesAndLoads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, authority := baselineRepository(t)
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(9600)}
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	defer observer.Stop()
	provider := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	if err := acceptStartupReachabilityBaseline(ctx, &Controller{}, root, authority, true, observer, provider, []sdk.Detector{goal}); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("fresh startup did not publish/adopt: %v", err)
	}
	observer.Stop()
	baseline, source, objective, err := loadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.Exclusions) != 1 {
		t.Fatalf("no measured baseline: %+v", baseline)
	}
	provider.Baseline, provider.SourceInputs, provider.ObjectiveDigest = baseline, source, objective
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"})
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(ctx, snapshot)
	if err != nil || len(findings) != 0 {
		t.Fatalf("restart did not consume accepted baseline: %+v %v", findings, err)
	}
	changed := reachabilityApp(9600)
	changed.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8888)
	snapshots.set(changed)
	// The observer is stopped above. Obtain a complete fresh failure run for
	// the changed Service; Git publication can outlive the old sample window.
	for i := 0; i < observer.config.Failures; i++ {
		observer.Cycle(ctx)
	}
	snapshot, err = provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err = detector.Detect(ctx, snapshot)
	if err != nil || len(findings) != 1 {
		t.Fatalf("new wrong targetPort hidden: %+v %v", findings, err)
	}
}

func TestStartupReachabilityBaselineRejectsGoalFailureDuringMeasurement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, authority := baselineRepository(t)
	head := nativeGit(t, root, "rev-parse", "HEAD")
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(9600)}
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	defer observer.Stop()
	provider := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{}, stateFinding("health-objective"))
	goal.spec.Class = sdk.DetectorClassHealth
	if err := acceptStartupReachabilityBaseline(ctx, &Controller{}, root, authority, true, observer, provider, []sdk.Detector{goal}); err == nil || errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("unhealthy goal accepted baseline: %v", err)
	}
	if nativeGit(t, root, "rev-parse", "HEAD") != head {
		t.Fatal("unhealthy baseline changed accepted repository")
	}
	b, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil || b != nil {
		t.Fatalf("unhealthy baseline persisted: %+v %v", b, err)
	}
}

func TestStartupReachabilityBaselineAfterTerminalClosureAndSourceChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, authority := baselineRepository(t)
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(9600)}
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	defer observer.Stop()
	provider := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	capture := func(c *Controller) {
		t.Helper()
		if err := acceptStartupReachabilityBaseline(ctx, c, root, authority, true, observer, provider, []sdk.Detector{goal}); !errors.Is(err, memory.ErrHealthRefreshRequired) {
			t.Fatalf("healthy source revision did not publish/adopt: %v", err)
		}
		observer.Stop()
	}
	capture(&Controller{})
	previous, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "application-config.yaml"), []byte("version: 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", "application-config.yaml")
	nativeGit(t, root, "commit", "-qm", "change accepted source after terminal incident")
	failed := &Controller{closureState: contracts.ClosureState_CLOSURE_STATE_FAILED, pendingClosure: &IncidentClosure{}, closureReceipt: &ClosureReceipt{}}
	capture(failed)
	current, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil || current.Authorization.SourceInputs == previous.Authorization.SourceInputs {
		t.Fatalf("stale baseline survived source change: %v", err)
	}
	if failed.pendingClosure == nil || failed.closureReceipt == nil || failed.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED {
		t.Fatal("baseline acceptance discarded failed closure evidence")
	}
}

// A recorded exclusion binds to the target's configuration. When that configuration
// changes (restored ConfigMap, rolled pod template) while the target is still
// never served and the goal is healthy, the health judge records it again at startup.
func TestStartupReachabilityBaselineRefreshesAnExclusionWhoseConfigurationChanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, authority := baselineRepository(t)
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(9600)}
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	defer observer.Stop()
	provider := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	accept := func() error {
		return acceptStartupReachabilityBaseline(ctx, &Controller{}, root, authority, true, observer, provider, []sdk.Detector{goal})
	}
	if err := accept(); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("fresh startup did not publish: %v", err)
	}
	observer.Stop()
	first, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil || len(first.Exclusions) != 1 {
		t.Fatalf("no first baseline: %+v %v", first, err)
	}
	head := nativeGit(t, root, "rev-parse", "HEAD")
	if err := accept(); err != nil || nativeGit(t, root, "rev-parse", "HEAD") != head {
		t.Fatalf("unchanged configuration republished: %v", err)
	}
	changed := reachabilityApp(9600)
	changed.PodList[0].Spec.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: "restored"}}
	snapshots.set(changed)
	if err := accept(); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("stale exclusion was kept: %v", err)
	}
	observer.Stop()
	second, err := memory.ReadReachabilityBaseline(ctx, root, "HEAD")
	if err != nil || len(second.Exclusions) != 1 || second.Exclusions[0].Configuration == first.Exclusions[0].Configuration {
		t.Fatalf("exclusion not recorded for the new configuration: %+v %v", second, err)
	}
}
