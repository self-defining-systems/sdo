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

type baselineGate struct {
	t          *testing.T
	ctx        context.Context
	root       string
	snapshots  *mutableSnapshots
	observer   *ServiceReachabilityObserver
	provider   TrafficSnapshotProvider
	controller *Controller
	logged     []error
	authority  *broker.Broker
}

// newBaselineGate wires the real startup gate over a service whose every port is refused.
func newBaselineGate(t *testing.T, app func() *mutableSnapshots) *baselineGate {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	root, authority := baselineRepository(t)
	snapshots := app()
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	t.Cleanup(observer.Stop)
	g := &baselineGate{t: t, ctx: ctx, root: root, snapshots: snapshots, observer: observer, authority: authority}
	g.provider = TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	g.controller = &Controller{OnError: func(err error) { g.logged = append(g.logged, err) }}
	return g
}

func healthyGoal() sdk.Detector {
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	return goal
}

func firingGoal() sdk.Detector {
	goal := controllerDetector("health-objective", time.Second, stateFinding("health-objective"))
	goal.spec.Class = sdk.DetectorClassHealth
	return goal
}

func (g *baselineGate) accept(goal sdk.Detector) error {
	err := acceptStartupReachabilityBaseline(g.ctx, g.controller, g.root, g.authority, true, g.observer, g.provider, []sdk.Detector{goal})
	g.observer.Stop()
	return err
}

func (g *baselineGate) baseline() *servicehealth.ReachabilityBaseline {
	b, err := memory.ReadReachabilityBaseline(g.ctx, g.root, "HEAD")
	if err != nil || b == nil {
		g.t.Fatalf("no baseline: %v", err)
	}
	return b
}

func (g *baselineGate) head() string { return nativeGit(g.t, g.root, "rev-parse", "HEAD") }

func withMode(mode string) *mutableSnapshots {
	s := reachabilityApp(9600)
	if mode != "" {
		s.PodList[0].Spec.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: mode}}
	}
	return &mutableSnapshots{snapshot: s}
}

// The configuration R -> C -> R must publish three accepted revisions, not reuse
// the first proposal.
func TestStartupReachabilityBaselineRefreshesWhenConfigurationReturnsToAnEarlierValue(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("first capture: %v", err)
	}
	first := g.baseline().Exclusions[0].Configuration
	g.snapshots.set(withMode("restored").snapshot)
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("second capture: %v", err)
	}
	if g.baseline().Exclusions[0].Configuration == first {
		t.Fatal("second capture kept the first configuration")
	}
	g.snapshots.set(withMode("").snapshot)
	head := g.head()
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("third capture was not published: %v", err)
	}
	if g.head() == head || g.baseline().Exclusions[0].Configuration != first {
		t.Fatalf("record stayed stale after the configuration returned: %+v", g.baseline().Exclusions)
	}
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("a current record was refreshed again: %v", err)
	}
}

// Staleness only says a recorded exclusion must be measured again. A port that
// newly fails at the gate is a defect to report, never an exclusion.
func TestStartupReachabilityBaselineStalenessNeverAddsAnExclusion(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("first capture: %v", err)
	}
	changed := withMode("restored").snapshot
	changed.ServiceList[0].Spec.Ports = append(changed.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "metrics", Port: 9601, TargetPort: intstr.FromInt32(9601)})
	g.snapshots.set(changed)
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("refresh: %v", err)
	}
	exclusions := g.baseline().Exclusions
	if len(exclusions) != 1 || exclusions[0].Target.Port != 9600 {
		t.Fatalf("refresh recorded a port that was not already excluded: %+v", exclusions)
	}
	baseline, source, objective, err := loadReachabilityBaseline(g.ctx, g.root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	g.provider.Baseline, g.provider.SourceInputs, g.provider.ObjectiveDigest = baseline, source, objective
	for i := 0; i < g.observer.config.Failures; i++ {
		g.observer.Cycle(g.ctx)
	}
	snapshot, err := g.provider.Snapshot(g.ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"}).Detect(g.ctx, snapshot)
	if err != nil || len(findings) != 1 {
		t.Fatalf("newly failing port was hidden: %+v %v", findings, err)
	}
}

// A stale record with a goal that is firing now is kept and retried at the next
// gate; it must not make Run or resume fail.
func TestStartupReachabilityBaselineKeepsAStaleRecordWhileTheGoalIsFiring(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("first capture: %v", err)
	}
	first := g.baseline().Exclusions[0].Configuration
	head := g.head()
	g.snapshots.set(withMode("restored").snapshot)
	if err := g.accept(firingGoal()); err != nil {
		t.Fatalf("a firing goal made the gate fail: %v", err)
	}
	if g.head() != head || g.baseline().Exclusions[0].Configuration != first || len(g.logged) == 0 {
		t.Fatalf("stale record not kept and logged: head moved=%v logged=%v", g.head() != head, g.logged)
	}
	if err := g.accept(healthyGoal()); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("next gate did not retry: %v", err)
	}
	if g.baseline().Exclusions[0].Configuration == first {
		t.Fatal("retry kept the stale record")
	}
}

// The proposal id is unique per accepted revision: the same refresh inputs and
// measured configuration at a later accepted commit never reuse an earlier proposal.
func TestBaselineProposalIDIsUniquePerAcceptedRevision(t *testing.T) {
	baseline := &servicehealth.ReachabilityBaseline{Exclusions: []servicehealth.ReachabilityExclusion{{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 9600}, Configuration: "r"}}}
	first := baselineProposalID(memory.HealthRevision{Commit: "c1", SourceInputs: "s", Objective: "o", DetectorRevision: "d"}, baseline)
	later := baselineProposalID(memory.HealthRevision{Commit: "c3", SourceInputs: "s", Objective: "o", DetectorRevision: "d"}, baseline)
	if first == later {
		t.Fatalf("proposal id reused across accepted revisions: %s", first)
	}
}
