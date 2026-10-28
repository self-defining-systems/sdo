package runtime

import (
	"context"
	"encoding/json"
	"errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"net"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"strings"
	"syscall"
	"testing"
	"time"
)

func baselineRepository(t *testing.T) string {
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
	return root
}

func refusingObserver(t *testing.T, snapshots *mutableSnapshots) *ServiceReachabilityObserver {
	t.Helper()
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	t.Cleanup(observer.Stop)
	return observer
}

func servingObserver(t *testing.T, snapshots *mutableSnapshots) *ServiceReachabilityObserver {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	t.Cleanup(observer.Stop)
	return observer
}

func stateBaseline(t *testing.T, state string) *servicehealth.ReachabilityBaseline {
	t.Helper()
	b, err := memory.ReadReachabilityBaselineState(state)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func reachabilityFindings(t *testing.T, ctx context.Context, provider SnapshotProvider) []sdk.Finding {
	t.Helper()
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"}).Detect(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// Two clusters adopt one accepted tree. Each measures its own reachability
// baseline into its own controller state at startup; the shared tree stays
// byte-identical, and each detector judges against its own cluster's record.
func TestTwoClustersAdoptingOneAcceptedTreeEachKeepTheirOwnBaseline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := baselineRepository(t)
	head, tree := nativeGit(t, root, "rev-parse", "HEAD"), nativeGit(t, root, "rev-parse", "HEAD^{tree}")
	type cluster struct {
		state     string
		snapshots *mutableSnapshots
		observer  *ServiceReachabilityObserver
		reference *ReachabilityReference
		provider  TrafficSnapshotProvider
	}
	lane := func(uid string, serving bool) *cluster {
		app := reachabilityApp(9600)
		app.ServiceList[0].UID = types.UID(uid)
		c := &cluster{state: t.TempDir(), snapshots: &mutableSnapshots{snapshot: app}}
		if serving {
			c.observer = servingObserver(t, c.snapshots)
		} else {
			c.observer = refusingObserver(t, c.snapshots)
		}
		reference, err := loadReachabilityBaseline(ctx, c.state, root, head, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.reference = reference
		c.provider = TrafficSnapshotProvider{Base: c.snapshots, Reachability: c.observer, Reference: reference}
		acceptStartupReachabilityBaseline(ctx, &Controller{}, root, c.state, true, c.observer, c.provider, reference, []sdk.Detector{healthyGoal()})
		if err := reference.Unavailable(); err != nil {
			t.Fatalf("startup capture: %v", err)
		}
		c.observer.Stop()
		return c
	}
	refusing, serving := lane("uid-cluster-a", false), lane("uid-cluster-b", true)
	if nativeGit(t, root, "rev-parse", "HEAD") != head || nativeGit(t, root, "rev-parse", "HEAD^{tree}") != tree || nativeGit(t, root, "status", "--porcelain") != "" {
		t.Fatal("a cluster wrote its baseline into the shared accepted tree")
	}
	a, b := stateBaseline(t, refusing.state), stateBaseline(t, serving.state)
	if a == nil || len(a.Exclusions) != 1 || a.Exclusions[0].ServiceUID != "uid-cluster-a" {
		t.Fatalf("refusing cluster lacks its own measured exclusion: %+v", a)
	}
	if b == nil || len(b.Exclusions) != 0 {
		t.Fatalf("serving cluster inherited another cluster's exclusion: %+v", b)
	}
	if findings := reachabilityFindings(t, ctx, refusing.provider); len(findings) != 0 {
		t.Fatalf("refusing cluster did not consume its own baseline: %+v", findings)
	}
	// A restart reads the same cluster state and judges the same way.
	restarted, err := loadReachabilityBaseline(ctx, refusing.state, root, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	if findings := reachabilityFindings(t, ctx, TrafficSnapshotProvider{Base: refusing.snapshots, Reachability: refusing.observer, Reference: restarted}); len(findings) != 0 {
		t.Fatalf("restart lost the cluster baseline: %+v", findings)
	}
	// The serving cluster's port now fails: its own empty record hides nothing.
	failing := refusingObserver(t, serving.snapshots)
	for i := 0; i < failing.config.Failures; i++ {
		failing.Cycle(ctx)
	}
	if findings := reachabilityFindings(t, ctx, TrafficSnapshotProvider{Base: serving.snapshots, Reachability: failing, Reference: serving.reference}); len(findings) != 1 {
		t.Fatalf("another cluster's exclusion hid a real failure: %+v", findings)
	}
}

// A memory written before the move still carries a capture at the legacy path.
// The runtime never reads it: the recorded exclusion does not hide a failure on
// a cluster that has not measured it, and startup measures into cluster state.
func TestLegacyTreeCaptureIsIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := baselineRepository(t)
	revision, err := memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(9600)}
	observer := refusingObserver(t, snapshots)
	for i := 0; i < observer.config.Failures; i++ {
		observer.Cycle(ctx)
	}
	window, _ := observer.Window()
	legacy, err := servicehealth.CaptureReachabilityBaseline(withReachability(snapshots.snapshot, window), servicehealth.BaselineAuthorization{SourceInputs: revision.SourceInputs, ObjectiveDigest: objectiveDigest(revision.Objective), Reason: "healthy-baseline", GoalHealthy: true})
	if err != nil || len(legacy.Exclusions) != 1 {
		t.Fatalf("legacy fixture: %+v %v", legacy, err)
	}
	raw, _ := json.Marshal(legacy)
	path := filepath.Join(root, memory.LegacyReachabilityBaselinePath)
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "pre-move capture")
	state := t.TempDir()
	reference, err := loadReachabilityBaseline(ctx, state, root, "HEAD", nil)
	if err != nil {
		t.Fatal(err)
	}
	if findings := reachabilityFindings(t, ctx, TrafficSnapshotProvider{Base: snapshots, Reachability: observer, Reference: reference}); len(findings) != 1 {
		t.Fatalf("legacy tree capture hid a failure on an unmeasured cluster: %+v", findings)
	}
	acceptStartupReachabilityBaseline(ctx, &Controller{}, root, state, true, observer, TrafficSnapshotProvider{Base: snapshots, Reachability: observer, Reference: reference}, reference, []sdk.Detector{healthyGoal()})
	if err := reference.Unavailable(); err != nil {
		t.Fatal(err)
	}
	if b := stateBaseline(t, state); b == nil || len(b.Exclusions) != 1 {
		t.Fatalf("startup did not measure into cluster state: %+v", b)
	}
}

// Moving the record from the tree into cluster state preserves the detector's
// verdict on a recorded case: the same record bytes reach the detector.
func TestClusterStateBaselineKeepsRecordedVerdicts(t *testing.T) {
	ctx := context.Background()
	root := baselineRepository(t)
	revision, err := memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 28, 16, 19, 23, 0, time.UTC)
	app := reachabilityApp(9600)
	app.ServiceList[0].Spec.Ports = append(app.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "metrics", Port: 9601, TargetPort: intstr.FromInt32(9601)})
	refused := func(port int32, name string) servicehealth.TargetObservations {
		return servicehealth.TargetObservations{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: port, PortName: name}, Samples: []servicehealth.DialSample{{At: now.Add(-2 * time.Second), Class: servicehealth.DialRefused}, {At: now.Add(-time.Second), Class: servicehealth.DialRefused}, {At: now, Class: servicehealth.DialRefused}}}
	}
	signature, _ := servicehealth.ReachabilityTargetConfiguration(app, servicehealth.ReachabilityTarget{Service: "geo", Port: 9600, PortName: "grpc"})
	recorded := []byte(`{"version":1,"authorization":{"sourceInputs":"` + revision.SourceInputs + `","objectiveDigest":"` + objectiveDigest(revision.Objective) + `","reason":"healthy-baseline","goalHealthy":true},"capturedAt":"2026-10-28T16:19:23Z","exclusions":[{"namespace":"shop","target":{"service":"geo","port":9600,"portName":"grpc"},"serviceUID":"recorded-uid","targetPort":"9600","protocol":"TCP","selector":{"app":"geo"},"configuration":"` + signature + `","samples":[{"at":"2026-10-28T16:19:21Z","durationNs":1,"ok":false,"class":"refused"},{"at":"2026-10-28T16:19:22Z","durationNs":1,"ok":false,"class":"refused"},{"at":"2026-10-28T16:19:23Z","durationNs":1,"ok":false,"class":"refused"}]}]}`)
	decoded, err := memory.DecodeReachabilityBaseline(recorded)
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	if err := memory.WriteReachabilityBaselineState(state, decoded); err != nil {
		t.Fatal(err)
	}
	reference, err := loadReachabilityBaseline(ctx, state, root, "HEAD", nil)
	if err != nil {
		t.Fatal(err)
	}
	window := servicehealth.ReachabilityWindow{ObservedAt: now, Interval: time.Second, Failures: 3, Targets: []servicehealth.TargetObservations{refused(9600, "grpc"), refused(9601, "metrics")}}
	fromState := window
	reference.bind(&fromState)
	direct := window
	direct.Baseline, direct.SourceInputs, direct.ObjectiveDigest = decoded, revision.SourceInputs, objectiveDigest(revision.Objective)
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"})
	verdict := func(w servicehealth.ReachabilityWindow) string {
		findings, err := detector.Detect(ctx, withReachability(app, w))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(findings)
		return string(raw)
	}
	got, want := verdict(fromState), verdict(direct)
	// The recorded exclusion hides only 9600; the unrecorded 9601 still fires.
	if got != want || !strings.Contains(got, `"port":9601`) || strings.Contains(got, `"port":9600`) {
		t.Fatalf("cluster-state verdict differs from the recorded record's verdict:\n got %s\nwant %s", got, want)
	}
}

func TestReachabilityBaselineCaptureAndRestart(t *testing.T) {
	ctx := context.Background()
	root := baselineRepository(t)
	head := nativeGit(t, root, "rev-parse", "HEAD")
	now := time.Now().UTC()
	s := reachabilityApp(9600)
	s.ServiceList[0].Name = "opensearch"
	s.Reachability = &servicehealth.ReachabilityWindow{ObservedAt: now, Interval: time.Second, Failures: 3, Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "opensearch", Port: 9600}, Samples: []servicehealth.DialSample{{At: now.Add(-2 * time.Second), Class: servicehealth.DialRefused}, {At: now.Add(-time.Second), Class: servicehealth.DialRefused}, {At: now, Class: servicehealth.DialRefused}}}}}
	revision, err := memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	baseline, err := captureReachabilityBaseline(ctx, state, nil, revision, []sdk.Detector{healthyGoal()}, s)
	if err != nil || baseline == nil || len(baseline.Exclusions) != 1 {
		t.Fatalf("baseline not captured: %+v %v", baseline, err)
	}
	if recorded := stateBaseline(t, state); recorded == nil || !recorded.CapturedAt.Equal(baseline.CapturedAt) {
		t.Fatalf("no restart-visible cluster state: %+v", recorded)
	}
	if nativeGit(t, root, "rev-parse", "HEAD") != head || nativeGit(t, root, "status", "--porcelain") != "" {
		t.Fatal("capture changed the accepted repository")
	}
	again, err := captureReachabilityBaseline(ctx, state, baseline, revision, []sdk.Detector{healthyGoal()}, s)
	if err != nil || again != baseline {
		t.Fatalf("a current record was measured again: %v", err)
	}
	if _, err := healthyBaselineGoal(ctx, []sdk.Detector{firingGoal()}, s); err == nil {
		t.Fatal("unhealthy objective authorized exclusion")
	}
}

func TestStartupReachabilityBaselineNeverCapturesRestoredIncident(t *testing.T) {
	root := nativeRepository(t)
	state := t.TempDir()
	for _, c := range []*Controller{{incidentOpen: true}, {currentIncidentRequest: &IncidentRequest{}}, {pendingClosure: &IncidentClosure{}, closureState: contracts.ClosureState_CLOSURE_STATE_PENDING}, {closureReceipt: &ClosureReceipt{}, closureState: contracts.ClosureState_CLOSURE_STATE_COMMITTED}, {pendingClosure: &IncidentClosure{}, closureReceipt: &ClosureReceipt{}, closureState: contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING}} {
		// Nil observation dependencies deliberately panic if a restored incident
		// reaches measurement, rather than proving only that capture was skipped.
		acceptStartupReachabilityBaseline(context.Background(), c, root, state, true, &ServiceReachabilityObserver{}, nil, &ReachabilityReference{}, nil)
	}
	acceptStartupReachabilityBaseline(context.Background(), &Controller{}, root, state, false, &ServiceReachabilityObserver{}, nil, &ReachabilityReference{}, nil)
	if b := stateBaseline(t, state); b != nil {
		t.Fatalf("restored incident captured a baseline: %+v", b)
	}
}

func TestStartupReachabilityBaselineMeasuresAndJudgesInPlace(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("fresh startup did not capture: %v", err)
	}
	if len(g.baseline().Exclusions) != 1 {
		t.Fatalf("no measured baseline: %+v", g.baseline())
	}
	// The running controller judges against the new record without a restart.
	if findings := reachabilityFindings(t, g.ctx, g.provider); len(findings) != 0 {
		t.Fatalf("controller did not consume the measured baseline: %+v", findings)
	}
	changed := reachabilityApp(9600)
	changed.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8888)
	g.snapshots.set(changed)
	for i := 0; i < g.observer.config.Failures; i++ {
		g.observer.Cycle(g.ctx)
	}
	if findings := reachabilityFindings(t, g.ctx, g.provider); len(findings) != 1 {
		t.Fatalf("new wrong targetPort hidden: %+v", findings)
	}
}

func TestStartupReachabilityBaselineRejectsGoalFailureDuringMeasurement(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(firingGoal()); err == nil {
		t.Fatal("unhealthy goal accepted baseline")
	}
	if b := stateBaseline(t, g.state); b != nil {
		t.Fatalf("unhealthy baseline persisted: %+v", b)
	}
}

func TestStartupReachabilityBaselineAfterTerminalClosureAndSourceChange(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatal(err)
	}
	previous := g.baseline()
	if err := os.WriteFile(filepath.Join(g.root, "application-config.yaml"), []byte("version: 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, g.root, "add", "application-config.yaml")
	nativeGit(t, g.root, "commit", "-qm", "change accepted source after terminal incident")
	failed := &Controller{closureState: contracts.ClosureState_CLOSURE_STATE_FAILED, pendingClosure: &IncidentClosure{}, closureReceipt: &ClosureReceipt{}}
	g.controller = failed
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatal(err)
	}
	if g.baseline().Authorization.SourceInputs == previous.Authorization.SourceInputs {
		t.Fatal("stale baseline survived source change")
	}
	if failed.pendingClosure == nil || failed.closureReceipt == nil || failed.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED {
		t.Fatal("baseline acceptance discarded failed closure evidence")
	}
}

// A recorded exclusion binds to the target's configuration. When that configuration
// changes (restored ConfigMap, rolled pod template) while the target is still
// never served and the goal is healthy, the gate records it again at startup.
func TestStartupReachabilityBaselineRefreshesAnExclusionWhoseConfigurationChanged(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("fresh startup did not capture: %v", err)
	}
	first := g.baseline()
	if len(first.Exclusions) != 1 {
		t.Fatalf("no first baseline: %+v", first)
	}
	if err := g.accept(healthyGoal()); err != nil || !g.baseline().CapturedAt.Equal(first.CapturedAt) {
		t.Fatalf("unchanged configuration measured again: %v", err)
	}
	g.snapshots.set(withMode("restored").snapshot)
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatal(err)
	}
	second := g.baseline()
	if len(second.Exclusions) != 1 || second.Exclusions[0].Configuration == first.Exclusions[0].Configuration {
		t.Fatalf("exclusion not recorded for the new configuration: %+v", second)
	}
}

type baselineGate struct {
	t          *testing.T
	ctx        context.Context
	root       string
	state      string
	snapshots  *mutableSnapshots
	observer   *ServiceReachabilityObserver
	reference  *ReachabilityReference
	provider   TrafficSnapshotProvider
	controller *Controller
	logged     []error
}

// newBaselineGate wires the real startup gate over a service whose every port is refused.
func newBaselineGate(t *testing.T, app func() *mutableSnapshots) *baselineGate {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	root := baselineRepository(t)
	snapshots := app()
	observer := refusingObserver(t, snapshots)
	g := &baselineGate{t: t, ctx: ctx, root: root, state: t.TempDir(), snapshots: snapshots, observer: observer, reference: &ReachabilityReference{}}
	g.provider = TrafficSnapshotProvider{Base: snapshots, Reachability: observer, Reference: g.reference}
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
	head, tree := g.head(), nativeGit(g.t, g.root, "rev-parse", "HEAD^{tree}")
	acceptStartupReachabilityBaseline(g.ctx, g.controller, g.root, g.state, true, g.observer, g.provider, g.reference, []sdk.Detector{goal})
	g.observer.Stop()
	if g.head() != head || nativeGit(g.t, g.root, "rev-parse", "HEAD^{tree}") != tree {
		g.t.Fatal("the startup gate changed the accepted repository")
	}
	if err := g.reference.Unavailable(); err != nil {
		return err
	}
	return nil
}

func (g *baselineGate) baseline() *servicehealth.ReachabilityBaseline {
	b := stateBaseline(g.t, g.state)
	if b == nil {
		g.t.Fatal("no cluster baseline")
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

// The configuration R -> C -> R must be measured three times, not reuse the
// first record.
func TestStartupReachabilityBaselineRefreshesWhenConfigurationReturnsToAnEarlierValue(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("first capture: %v", err)
	}
	first := g.baseline().Exclusions[0].Configuration
	g.snapshots.set(withMode("restored").snapshot)
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("second capture: %v", err)
	}
	second := g.baseline()
	if second.Exclusions[0].Configuration == first {
		t.Fatal("second capture kept the first configuration")
	}
	g.snapshots.set(withMode("").snapshot)
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("third capture: %v", err)
	}
	third := g.baseline()
	if !third.CapturedAt.After(second.CapturedAt) || third.Exclusions[0].Configuration != first {
		t.Fatalf("record stayed stale after the configuration returned: %+v", third.Exclusions)
	}
	if err := g.accept(healthyGoal()); err != nil || !g.baseline().CapturedAt.Equal(third.CapturedAt) {
		t.Fatalf("a current record was refreshed again: %v", err)
	}
}

// Staleness only says a recorded exclusion must be measured again. A port that
// newly fails at the gate is a defect to report, never an exclusion.
func TestStartupReachabilityBaselineStalenessNeverAddsAnExclusion(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("first capture: %v", err)
	}
	changed := withMode("restored").snapshot
	changed.ServiceList[0].Spec.Ports = append(changed.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "metrics", Port: 9601, TargetPort: intstr.FromInt32(9601)})
	g.snapshots.set(changed)
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	exclusions := g.baseline().Exclusions
	if len(exclusions) != 1 || exclusions[0].Target.Port != 9600 {
		t.Fatalf("refresh recorded a port that was not already excluded: %+v", exclusions)
	}
	for i := 0; i < g.observer.config.Failures; i++ {
		g.observer.Cycle(g.ctx)
	}
	if findings := reachabilityFindings(t, g.ctx, g.provider); len(findings) != 1 {
		t.Fatalf("newly failing port was hidden: %+v", findings)
	}
}

// A stale record with a goal that is firing now is kept and retried at the next
// gate; the baseline is only unavailable, which cannot make Run or resume fail.
func TestStartupReachabilityBaselineKeepsAStaleRecordWhileTheGoalIsFiring(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("first capture: %v", err)
	}
	first := g.baseline().Exclusions[0].Configuration
	g.snapshots.set(withMode("restored").snapshot)
	var unavailable *BaselineUnavailableError
	if err := g.accept(firingGoal()); !errors.As(err, &unavailable) {
		t.Fatalf("a firing goal did not leave the baseline unavailable: %v", err)
	}
	if g.baseline().Exclusions[0].Configuration != first || len(g.logged) == 0 {
		t.Fatalf("stale record not kept and logged: logged=%v", g.logged)
	}
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("next gate did not retry: %v", err)
	}
	if g.baseline().Exclusions[0].Configuration == first {
		t.Fatal("retry kept the stale record")
	}
}

func withReachability(s sdktest.Snapshot, w servicehealth.ReachabilityWindow) sdk.DetectionContext {
	return trafficContext{DetectionContext: s, reachability: &w}
}

// After a first try that changed the accepted source, the controller resumes
// while the application is still unhealthy. No current baseline can be measured;
// that is a state, not a controller failure: the never-served port is unknown
// rather than an incident, and once the goal heals the retry measures a record
// that the running detector judges against.
func TestResumeOnUnhealthyAppKeepsObservingAndMeasuresOnceHealthy(t *testing.T) {
	g := newBaselineGate(t, func() *mutableSnapshots { return withMode("") })
	if err := g.accept(healthyGoal()); err != nil {
		t.Fatalf("first capture: %v", err)
	}
	previous := g.baseline()
	if err := os.WriteFile(filepath.Join(g.root, "application-config.yaml"), []byte("version: 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, g.root, "add", "application-config.yaml")
	nativeGit(t, g.root, "commit", "-qm", "first try changed the source")
	now := time.Date(2026, 10, 28, 18, 0, 0, 0, time.UTC)
	g.controller.now = func() time.Time { return now }

	var unavailable *BaselineUnavailableError
	if err := g.accept(firingGoal()); !errors.As(err, &unavailable) {
		t.Fatalf("resume on an unhealthy app did not leave the baseline unavailable: %v", err)
	}
	if !g.baseline().CapturedAt.Equal(previous.CapturedAt) {
		t.Fatal("an unhealthy app wrote a baseline")
	}
	if len(g.logged) == 0 || !errors.As(g.logged[len(g.logged)-1], &unavailable) {
		t.Fatalf("unavailable baseline not reported: %v", g.logged)
	}
	for i := 0; i < g.observer.config.Failures; i++ {
		g.observer.Cycle(g.ctx)
	}
	if findings := reachabilityFindings(t, g.ctx, g.provider); len(findings) != 0 {
		t.Fatalf("a never-served port fired without a current baseline: %+v", findings)
	}
	snapshot, err := g.provider.Snapshot(g.ctx)
	if err != nil {
		t.Fatal(err)
	}
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"})
	if reason := detectorConfirmationReason(detector, snapshot); !detectorConfirmed(detector, snapshot) || !strings.Contains(reason, "unknown") {
		t.Fatalf("reachability not reported unknown: confirmed=%v reason=%q", detectorConfirmed(detector, snapshot), reason)
	}

	// Not due yet, then due while an incident is open: nothing is measured, and
	// the attempt moves forward instead of spinning the loop.
	g.retry(healthyGoal())
	if g.reference.Unavailable() == nil {
		t.Fatal("retry measured before it was due")
	}
	now = now.Add(reachabilityBaselineCheck)
	g.controller.incidentOpen = true
	g.retry(healthyGoal())
	if g.reference.Unavailable() == nil || !g.reference.retryAt().After(now) {
		t.Fatalf("skipped retry did not move forward: %v", g.reference.retryAt())
	}
	g.controller.incidentOpen = false

	now = now.Add(reachabilityBaselineCheck)
	g.retry(healthyGoal())
	if err := g.reference.Unavailable(); err != nil {
		t.Fatalf("healthy app did not get a baseline: %v", err)
	}
	if !g.reference.retryAt().IsZero() {
		t.Fatal("a current record still schedules retries")
	}
	measured := g.baseline()
	if measured.Authorization.SourceInputs == previous.Authorization.SourceInputs || len(measured.Exclusions) != 1 {
		t.Fatalf("no baseline for the changed source: %+v", measured)
	}
	for i := 0; i < g.observer.config.Failures; i++ {
		g.observer.Cycle(g.ctx)
	}
	if findings := reachabilityFindings(t, g.ctx, g.provider); len(findings) != 0 {
		t.Fatalf("detector did not judge against the new record: %+v", findings)
	}
	if snapshot, err = g.provider.Snapshot(g.ctx); err != nil {
		t.Fatal(err)
	}
	if reason := detectorConfirmationReason(detector, snapshot); reason != "" {
		t.Fatalf("still unknown after the baseline: %q", reason)
	}
}

func (g *baselineGate) retry(goal sdk.Detector) {
	retryReachabilityBaseline(g.ctx, g.controller, g.root, g.state, true, g.observer, g.provider, g.reference, []sdk.Detector{goal})
	g.observer.Stop()
}
