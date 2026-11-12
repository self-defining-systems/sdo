//go:build integration

package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// harness runs the production controller loop (RunWithOptions) in a virtual
// time bubble against a simulated cluster, the real memory host manager with
// in-process hosts, and an agent stand-in, then checks the recorded log.
type harness struct {
	t     *testing.T
	plan  plan
	world *sim.World
	admin kubernetes.Interface // the environment: no faults, outlives restarts
	agent kubernetes.Interface // the agent's client: faults, outlives restarts
	rec   *sim.Recorder
	repo  string

	quiesced atomic.Bool
	start    time.Time

	mu       sync.Mutex
	manager  *memoryhost.Manager
	remote   *RemoteMemory
	active   *simActivationHost
	revision planRevision
	ctl      *Controller
	inc      *incarnation
	nextInc  int

	out          *lineLog
	crossings    map[crashBoundary]int
	watches      int
	callCounts   map[string]int
	agentCalls   atomic.Int32
	programCalls counter
	hung         map[string]bool
}

const harnessControlNamespace = hotel + "-sdo"

// The accepted repository is a value, not a git checkout: a simulation runs no
// process, so the scope, the commit and the (empty) manifest tree are injected.
const harnessAcceptedCommit = "0123456789abcdef0123456789abcdef01234567"

func harnessAcceptedScope(context.Context, string) (sdk.ObservationScope, string, error) {
	scope, err := observation.Parse([]byte("---\nschema_version: 1\n---\nSynthetic test architecture.\n"))
	return scope, harnessAcceptedCommit, err
}

// harnessPlanGit answers the accepted-manifest source's tree listing: the
// application has no manifests in source.
func harnessPlanGit(_ context.Context, _ string, args ...string) ([]byte, error) {
	if len(args) > 0 && args[0] == "ls-tree" {
		return nil, nil
	}
	return nil, fmt.Errorf("harness repository has no object %v", args)
}

func (h *harness) now() time.Time { return time.Now() }

func (h *harness) faultsOn() bool {
	return !h.quiesced.Load() && (h.plan.Seed != 0 || len(h.plan.Agents) > 0)
}

// ---- simulated cluster (see harness_cluster_integration_test.go) -----------

func faultConfigMapName(detector string) string { return "fault-" + detector }

// setBroken flips a fault object through the environment's own client: the
// environment's failures are not API faults. The write still goes through the
// simulated server, so watchers see it and resourceVersion rules apply.
func (h *harness) setBroken(detector string, broken bool) {
	ctx := context.Background()
	cm, err := h.admin.CoreV1().ConfigMaps(hotel).Get(ctx, faultConfigMapName(detector), metav1.GetOptions{})
	if err != nil {
		h.t.Errorf("fault object for %s: %v", detector, err)
		return
	}
	cm.Data = map[string]string{"broken": fmt.Sprint(broken)}
	if _, err := h.admin.CoreV1().ConfigMaps(hotel).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		h.t.Errorf("flip %s: %v", detector, err)
	}
}

// ---- stand-in detectors, loaded in the real memory host ------------------

type standinDetector struct {
	h         *harness
	spec      sdk.DetectorSpec
	behaviour detectorBehaviour
}

func (d standinDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d standinDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	behaviour := d.behaviour
	if d.h.quiesced.Load() {
		behaviour = behaveNormal
	}
	switch behaviour {
	case behaveError:
		return nil, errors.New("stand-in detector error")
	case behavePanic:
		panic("stand-in detector panic")
	case behaveHang:
		select {
		case <-ctx.Done():
		case <-time.After(time.Hour):
		}
		return nil, ctx.Err()
	case behaveWrong:
		return []sdk.Finding{d.finding(d.spec.ID + "-phantom")}, nil
	}
	for _, cm := range view.ConfigMaps() {
		if cm.Name == faultConfigMapName(d.spec.ID) && cm.Data["broken"] == "true" {
			return []sdk.Finding{d.finding(d.spec.ID)}, nil
		}
	}
	return nil, nil
}

func (d standinDetector) finding(fingerprint string) sdk.Finding {
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: "scenario-slo.fault", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "stand-in fault", Evidence: "fault object " + fingerprint,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: hotel, Name: faultConfigMapName(d.spec.ID)},
		Fingerprint:     fingerprint,
	}
}

func (h *harness) detectorsOf(rev planRevision) []sdk.Detector {
	var out []sdk.Detector
	for _, d := range rev.Detectors {
		out = append(out, standinDetector{h: h, behaviour: d.Behaviour, spec: sdk.DetectorSpec{
			ID: d.ID, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: d.Interval,
			Watches:     []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap", Namespace: hotel}},
			Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
			Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 50 * time.Millisecond}, OriginatingCommit: "lifecycle-bootstrap",
		}})
	}
	return out
}

// recordingMemory is the controller's view of the host manager. The controller
// evaluates a detector on the host it captured under the mirror barrier, so the
// recording wraps Capture: every evaluation the controller makes is recorded,
// and a result with an error, or a transport failure, is a failed attempt.
type recordingMemory struct {
	*memoryhost.Manager
	rec *sim.Recorder
}

func (m recordingMemory) Capture(ctx context.Context, view sdk.DetectionContext) (memoryhost.Host, uint64, func(), error) {
	host, sequence, release, err := m.Manager.Capture(ctx, view)
	if err != nil {
		return host, sequence, release, err
	}
	identity, hasIdentity := host.(interface{ MemoryCommit() string })
	rpc, hasRPC := host.(MemoryHostRPC)
	if !hasIdentity || !hasRPC {
		return host, sequence, release, nil
	}
	return recordingHost{Host: host, rpc: rpc, commit: identity.MemoryCommit(), rec: m.rec}, sequence, release, nil
}

type recordingHost struct {
	memoryhost.Host
	rpc    MemoryHostRPC
	commit string
	rec    *sim.Recorder
}

func (h recordingHost) MemoryCommit() string { return h.commit }
func (h recordingHost) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	return h.rpc.Diagnose(ctx, r)
}
func (h recordingHost) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	return h.rpc.Mitigate(ctx, r)
}
func (h recordingHost) Evaluate(ctx context.Context, request *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	response, err := h.rpc.Evaluate(ctx, request)
	failed := map[string]bool{}
	for _, result := range response.GetResults() {
		failed[result.GetDetectorId()] = result.GetError() != ""
	}
	for _, id := range request.GetDetectorIds() {
		h.rec.Add(sim.Evaluated{At: h.rec.Now(), Detector: id, Failed: err != nil || failed[id]})
	}
	return response, err
}

// ---- memory host lifecycle -------------------------------------------------

func (h *harness) newHost(rev planRevision) *simActivationHost {
	server, err := memoryhost.NewServer(rev.Name, "digest", h.detectorsOf(rev))
	if err != nil {
		h.t.Fatal(err)
	}
	programs, err := h.programsOf(rev)
	if err != nil {
		h.t.Fatal(err)
	}
	server.WithPrograms(programs)
	rpc, stop := memoryTestRPC(h.t, server)
	return &simActivationHost{testMemoryRPC: rpc, stop: stop}
}

func (h *harness) declsOf(rev planRevision) []sim.DetectorDecl {
	var out []sim.DetectorDecl
	for _, d := range rev.Detectors {
		out = append(out, sim.DetectorDecl{ID: d.ID, Interval: d.Interval})
	}
	return out
}

// activate is the memory host's two-commit activation the controller runs for
// an accepted memory commit: prepare the candidate beside the active host, make
// it active, and reconfigure the live controller without restarting it.
func (h *harness) activate(ctx context.Context, rev planRevision) error {
	host := h.newHost(rev)
	if err := h.manager.Prepare(ctx, host); err != nil {
		_ = host.Close()
		return err
	}
	if err := h.manager.Activate(ctx, host, rev.Name, "digest"); err != nil {
		return err
	}
	ready, err := host.Ready(ctx)
	if err != nil {
		return err
	}
	remote, err := NewRemoteMemory(ready, recordingMemory{Manager: h.manager, rec: h.rec})
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.active, h.remote, h.revision = host, remote, rev
	for _, d := range rev.Detectors {
		if d.Behaviour == behaveHang {
			h.hung[d.ID] = true
		}
	}
	ctl := h.ctl
	if ctl == nil {
		h.rec.Add(sim.Activated{At: h.now(), Revision: rev.Name, Detectors: h.declsOf(rev)})
	}
	h.mu.Unlock()
	if ctl == nil {
		return nil
	}
	return h.reconfigure(ctl, remote, rev)
}

// reconfigure swaps the running controller to the activated memory. Production
// swaps on the controller loop, between passes, and every reader of the
// scheduler and provider holds the observation lock; this goroutine is not the
// loop, so it takes the same lock and the swap lands between two passes. A pass
// holds the lock across virtual time (an API call waiting out a fault), and a
// goroutine blocked on a mutex keeps the bubble's clock from advancing, so the
// lock is polled with a sleep. It is taken without h.mu: a pass in flight calls
// into the fault layer, which takes h.mu.
func (h *harness) reconfigure(c *Controller, remote *RemoteMemory, rev planRevision) error {
	// test-polling: allow -- a mutex wait is not durable in the bubble; a sleep is
	for !c.observationMu.TryLock() {
		time.Sleep(10 * time.Millisecond)
	}
	defer c.observationMu.Unlock()
	h.rec.Add(sim.Activated{At: h.now(), Revision: rev.Name, Detectors: h.declsOf(rev)})
	return c.ReconfigureMemory(remote, providerOf(c), h.now().UTC())
}

// providerOf reads the controller's snapshot provider, which a memory swap
// replaces under c.mu.
func providerOf(c *Controller) SnapshotProvider {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.provider
}

func (h *harness) crashHost() {
	h.mu.Lock()
	host := h.active
	h.mu.Unlock()
	if host == nil {
		return
	}
	h.rec.Add(sim.ControllerDown{At: h.now(), Host: true})
	_ = host.Close()
	h.rec.Add(sim.ControllerUp{At: h.now()})
}

func (h *harness) recoverHost(ctx context.Context) (memoryhost.Host, string, string, uint64, error) {
	h.mu.Lock()
	rev := h.revision
	h.mu.Unlock()
	host := h.newHost(rev)
	if err := h.manager.Prepare(ctx, host); err != nil {
		return host, "", "", 0, err
	}
	ready, err := host.Ready(ctx)
	if err != nil {
		return host, "", "", 0, err
	}
	h.mu.Lock()
	h.active = host
	h.mu.Unlock()
	return host, rev.Name, "digest", ready.GetMirrorSequence(), nil
}

// ---- agent stand-in ----------------------------------------------------------

type harnessAgent struct {
	h   *harness
	inc *incarnation
}

// Dispatch answers like the Job dispatcher's boundary does: a result that does
// not satisfy the incident result contract is rejected as a dispatch error
// before the controller sees it (KubernetesJobDispatcher.dispatch).
func (a harnessAgent) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	if a.inc.dead.Load() {
		return IncidentResult{}, errHarnessDead
	}
	if a.h.boundary(crashDispatchBefore) {
		a.h.kill(a.inc, "dispatch handed off")
		return IncidentResult{}, errHarnessDead
	}
	result, err := a.respond(ctx, request)
	if a.h.boundary(crashDispatchAfter) {
		// The agent has acted; the process dies before it reads the answer.
		a.h.kill(a.inc, "agent answered")
		return IncidentResult{}, errHarnessDead
	}
	if err != nil {
		return IncidentResult{}, err
	}
	if err := validateIncidentResultFor(result, request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate agent result: %w", err)
	}
	return result, nil
}

func (a harnessAgent) respond(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	h := a.h
	n := int(h.agentCalls.Add(1)) - 1
	mode := agentFix
	if h.faultsOn() && n < len(h.plan.Agents) {
		mode = h.plan.Agents[n]
	}
	switch mode {
	case agentCrash:
		return IncidentResult{}, errors.New("agent crashed")
	case agentHang:
		// A hung agent never answers and never gives up. Only the controller
		// can end it (by cancelling the dispatch, or by retiring the incident):
		// the stand-in bounds itself by nothing, as a hung Job would not.
		<-ctx.Done()
		return IncidentResult{}, ctx.Err()
	case agentGarbage:
		return IncidentResult{IncidentID: "not-" + request.IncidentID, Status: "garbage"}, nil
	case agentFix:
		for _, id := range h.faultIDs() {
			h.agentSet(ctx, id, false)
		}
	case agentWorsen:
		for _, id := range h.faultIDs() {
			h.agentSet(ctx, id, true)
		}
	}
	return completedResult(request.IncidentID), nil
}

func (h *harness) faultIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, d := range h.revision.Detectors {
		out = append(out, d.ID)
	}
	sort.Strings(out)
	return out
}

// agentSet writes through the faulty API, as its own client, and retries
// like an agent would.
func (h *harness) agentSet(ctx context.Context, detector string, broken bool) {
	// test-polling: allow -- the agent stand-in retries on virtual time
	for attempt := 0; attempt < 5; attempt++ {
		cm, err := h.agent.CoreV1().ConfigMaps(hotel).Get(ctx, faultConfigMapName(detector), metav1.GetOptions{})
		if err == nil {
			cm.Data = map[string]string{"broken": fmt.Sprint(broken)}
			if _, err = h.agent.CoreV1().ConfigMaps(hotel).Update(ctx, cm, metav1.UpdateOptions{}); err == nil {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---- running the controller ----------------------------------------------------

// crashController kills the controller process that is running now.
func (h *harness) crashController(why string) {
	if inc := h.current(); inc != nil {
		h.kill(inc, why)
	}
}

func (h *harness) runController(ctx context.Context) {
	args := []string{"--namespace", hotel, "--control-namespace", harnessControlNamespace, "--app-root", h.repo,
		"--application", "hotel", "--synthetic-traffic=false", "--lease-duration", "3s", "--drift-batch-window", "20ms",
		"--verification-timeout", "20s", "--closure-stability-window", "5s", "--response-timeout", "30s", "--source-commit", harnessAcceptedCommit, "--repository-mount-path", h.t.TempDir()}
	for ctx.Err() == nil {
		runCtx, cancel := context.WithCancel(ctx)
		h.mu.Lock()
		h.nextInc++
		inc := &incarnation{n: h.nextInc, cancel: cancel, conns: map[io.Closer]struct{}{}}
		h.inc = inc
		remote := h.remote
		revision := h.revision
		h.mu.Unlock()
		err := RunWithOptions(runCtx, remote.Detectors, RuntimeOptions{
			Args: args, Stdout: h.out, Stderr: h.out, Client: h.clientFor("sdo-controller", inc, true), Dispatcher: harnessAgent{h, inc}, AcceptedScope: harnessAcceptedScope, PlanGit: harnessPlanGit,
			Programs: remote.Programs, InformerSync: fastSyncPolicy(), InformerResync: 10 * time.Minute,
			Wired: func(c *Controller) {
				c.SetLifecycleSink(tapSink{h, inc})
				h.mu.Lock()
				defer h.mu.Unlock()
				h.ctl = c
				if h.revision.Name != revision.Name {
					_ = c.ReconfigureMemory(h.remote, providerOf(c), h.now().UTC())
				}
			},
		})
		cancel()
		h.mu.Lock()
		h.ctl = nil
		h.inc = nil
		h.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if !inc.dead.Load() {
			h.t.Logf("%s controller exited on its own: %v", h.now().Sub(h.start).Round(time.Millisecond), err)
			inc.dead.Store(true)
			inc.sever()
			h.rec.Add(sim.ControllerExited{At: h.now(), Reason: fmt.Sprint(err)})
			h.rec.Add(sim.ControllerDown{At: h.now()})
		}
		time.Sleep(time.Second)
		h.rec.Add(sim.ControllerUp{At: h.now()})
	}
}

type harnessEvent struct {
	at   time.Duration
	name string
	do   func()
}

// runHarness plays one seed and returns the recorded log and what the checks
// should treat specially.
func runHarness(t *testing.T, seed uint64) (sim.Log, sim.Config) {
	t.Helper()
	p := planFor(seed)
	if mode := os.Getenv("SDO_SIM_AGENT"); mode != "" {
		if len(p.Agents) == 0 {
			p.Agents = []agentMode{""}
		}
		for i := range p.Agents {
			p.Agents[i] = agentMode(mode)
		}
	}
	if os.Getenv("SDO_SIM_NOLAG") != "" {
		p.CacheLag = 0
	}
	t.Logf("plan: %s", p)
	h := &harness{t: t, plan: p, rec: sim.NewRecorder(time.Now).ClosureIsTerminal(), repo: t.TempDir(), start: time.Now(),
		out: &lineLog{start: time.Now()}, crossings: map[crashBoundary]int{}, callCounts: map[string]int{}, hung: map[string]bool{}}
	h.world = sim.NewWorld(seed)
	h.world.Kube.Clock = time.Now
	h.world.Kube.WatchLag = func() time.Duration {
		if h.faultsOn() {
			return h.plan.CacheLag
		}
		return 0
	}
	h.admin = h.clientFor("environment", nil, false)
	h.agent = h.clientFor("agent", nil, true)
	h.populate()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	h.manager = memoryhost.NewManager(5 * time.Second)
	defer h.manager.Close()
	if err := h.manager.Publish(ctx, sdktest.Snapshot{NamespaceName: hotel}); err != nil {
		t.Fatal(err)
	}
	h.manager.SetRecovery(h.recoverHost)
	if err := h.activate(ctx, p.Revisions[0]); err != nil {
		t.Fatal(err)
	}

	var events []harnessEvent
	for _, b := range p.Breaks {
		b := b
		events = append(events, harnessEvent{b.At, "break " + b.Detector, func() { h.setBroken(b.Detector, true) }})
	}
	for _, rev := range p.Revisions[1:] {
		rev := rev
		events = append(events, harnessEvent{rev.At, "activate " + rev.Name, func() {
			if err := h.activate(ctx, rev); err != nil {
				t.Errorf("activate %s: %v", rev.Name, err)
			}
		}})
	}
	for _, at := range p.ControllerCrashes {
		events = append(events, harnessEvent{at, "controller crash", func() { h.crashController("scheduled") }})
	}
	for _, at := range p.HostCrashes {
		events = append(events, harnessEvent{at, "host crash", h.crashHost})
	}
	events = append(events, harnessEvent{p.Quiesce, "quiesce", func() {
		h.quiesced.Store(true)
		h.rec.Add(sim.FaultsQuiesced{At: h.now()})
	}})
	sort.SliceStable(events, func(i, j int) bool { return events[i].at < events[j].at })

	h.start = time.Now() // before any other goroutine reads it
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.runController(ctx) }()
	for _, e := range events {
		time.Sleep(time.Until(h.start.Add(e.at)))
		e.do()
	}
	time.Sleep(time.Until(h.start.Add(p.Quiesce + p.Settle)))
	h.rec.Add(sim.RunEnded{At: h.now()})
	if os.Getenv("SDO_SIM_OUT") != "" {
		for _, line := range h.out.lines() {
			t.Log(line)
		}
	}
	if os.Getenv("SDO_SIM_STATE") != "" {
		cm, err := h.admin.CoreV1().ConfigMaps(harnessControlNamespace).Get(context.Background(), controllerStateConfigMap, metav1.GetOptions{})
		if err == nil {
			for k, v := range cm.Data {
				t.Logf("state[%s] (%d bytes): %.1500s", k, len(v), v)
			}
		} else {
			t.Logf("state configmap: %v", err)
		}
	}
	if os.Getenv("SDO_SIM_DUMP") != "" {
		buf := make([]byte, 1<<20)
		t.Logf("goroutines at end:\n%s", buf[:runtime.Stack(buf, true)])
	}
	stop()
	wg.Wait()
	t.Logf("program calls:%s", h.programCalls.String())

	h.mu.Lock()
	hung := map[string]bool{}
	for id := range h.hung {
		hung[id] = true
	}
	h.mu.Unlock()
	return h.rec.Log(), sim.Config{GapFactor: 3, Slack: 12 * time.Second, Settle: p.Settle, Restart: 30 * time.Second, Hung: hung}
}

type tapSink struct {
	h   *harness
	inc *incarnation
}

func (s tapSink) RecordLifecycle(e *pb.LifecycleEvent) error {
	if s.inc.dead.Load() {
		return errHarnessDead // a dead process records nothing
	}
	s.h.t.Logf("%s lifecycle %s incident=%s dispatchError=%v", s.h.now().Sub(s.h.start).Round(time.Second), e.GetEvent(), e.GetIncidentId(), e.GetDispatchError())
	err := s.h.rec.RecordLifecycle(e)
	if e.GetEvent() == PhaseOpened && s.h.boundary(crashOpenedAfter) {
		s.h.kill(s.inc, "incident opened")
	}
	return err
}

// lineLog keeps the controller's stdout and stderr lines.
type lineLog struct {
	start time.Time
	mu    sync.Mutex
	buf   []byte
	all   []string
}

func (l *lineLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		l.all = append(l.all, fmt.Sprintf("%s %s", time.Since(l.start).Round(time.Second), l.buf[:i]))
		l.buf = l.buf[i+1:]
	}
	return len(p), nil
}

func (l *lineLog) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.all...)
}

// populate creates the application and the fault objects through the simulated
// API, so the server's defaults, uids and versions apply as in a cluster.
func (h *harness) populate() {
	ctx := context.Background()
	check := func(err error) {
		if err != nil {
			h.t.Fatalf("populate: %v", err)
		}
	}
	for _, object := range hotelObjects() {
		switch o := object.(type) {
		case *corev1.Service:
			_, err := h.admin.CoreV1().Services(hotel).Create(ctx, o, metav1.CreateOptions{})
			check(err)
		case *appsv1.Deployment:
			_, err := h.admin.AppsV1().Deployments(hotel).Create(ctx, o, metav1.CreateOptions{})
			check(err)
		case *corev1.ConfigMap:
			_, err := h.admin.CoreV1().ConfigMaps(hotel).Create(ctx, o, metav1.CreateOptions{})
			check(err)
		case *corev1.Secret:
			_, err := h.admin.CoreV1().Secrets(hotel).Create(ctx, o, metav1.CreateOptions{})
			check(err)
		}
	}
	seen := map[string]bool{}
	for _, rev := range h.plan.Revisions {
		for _, d := range rev.Detectors {
			if !seen[d.ID] {
				seen[d.ID] = true
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: faultConfigMapName(d.ID), Namespace: hotel}, Data: map[string]string{"broken": "false"}}
				_, err := h.admin.CoreV1().ConfigMaps(hotel).Create(ctx, cm, metav1.CreateOptions{})
				check(err)
			}
		}
	}
}
