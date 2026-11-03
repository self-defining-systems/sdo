//go:build integration

package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// harness runs the production controller loop (RunWithOptions) in a virtual
// time bubble against a simulated cluster, the real memory host manager with
// in-process hosts, and an agent stand-in, then checks the recorded log.
type harness struct {
	t      *testing.T
	plan   plan
	client *fake.Clientset
	rec    *sim.Recorder
	repo   string

	quiesced atomic.Bool
	start    time.Time

	mu       sync.Mutex
	manager  *memoryhost.Manager
	remote   *RemoteMemory
	active   *simActivationHost
	revision planRevision
	ctl      *Controller
	cancel   context.CancelFunc
	down     bool

	// gate lets a harness operation that takes the host manager's locks wait
	// until no stand-in detector is hung inside the host, and keeps one from
	// starting to hang meanwhile: a goroutine blocked on a mutex would stop the
	// bubble's clock, so a hang could never time out.
	gate struct {
		sync.Mutex
		hanging int
		busy    bool
	}
	out           *lineLog
	controlWrites map[string]int
	callCounts    map[string]int
	agentCalls    atomic.Int32
	hung          map[string]bool
}

const harnessControlNamespace = hotel + "-sdo"

func (h *harness) now() time.Time { return time.Now() }

func (h *harness) faultsOn() bool {
	return !h.quiesced.Load() && (h.plan.Seed != 0 || len(h.plan.Agents) > 0)
}

// ---- simulated cluster with faults -------------------------------------

var (
	errHarnessAPI     = errors.New("simulated API server error")
	errHarnessTimeout = errors.New("simulated API timeout after commit")
)

func (h *harness) installFaults() {
	h.client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if !h.faultsOn() {
			return false, nil, nil
		}
		verb := action.GetVerb()
		resource := action.GetResource().Resource
		name := ""
		if named, ok := action.(interface{ GetName() string }); ok {
			name = named.GetName()
		}
		key := verb + "/" + resource + "/" + action.GetNamespace() + "/" + name
		h.mu.Lock()
		n := h.callCounts[key]
		h.callCounts[key]++
		h.mu.Unlock()
		switch verb {
		case "get": // informer lists are left alone: see docs/simulation-fidelity.md
			if chance(h.plan.Seed, key, n, h.plan.ReadErrorPercent) {
				return true, nil, errHarnessAPI
			}
		case "create", "update", "patch", "delete":
			if chance(h.plan.Seed, key, n, h.plan.APIErrorPercent) {
				return true, nil, errHarnessAPI
			}
			commit := func() (bool, kruntime.Object, error) { return ktesting.ObjectReaction(h.client.Tracker())(action) }
			if chance(h.plan.Seed, "t"+key, n, h.plan.CommitThenTimeoutPercent) {
				_, _, _ = commit()
				return true, nil, errHarnessTimeout
			}
			if resource == "configmaps" && action.GetNamespace() == harnessControlNamespace {
				h.mu.Lock()
				h.controlWrites[""]++
				ordinal := h.controlWrites[""]
				h.mu.Unlock()
				for _, crashAt := range h.plan.WriteCrashes {
					if crashAt == ordinal {
						handled, obj, err := commit()
						h.crashController("write-" + fmt.Sprint(ordinal))
						return handled, obj, err
					}
				}
			}
		}
		return false, nil, nil
	})
	if h.plan.CacheLag > 0 {
		h.client.PrependWatchReactor("*", func(action ktesting.Action) (bool, watch.Interface, error) {
			inner, err := h.client.Tracker().Watch(action.GetResource(), action.GetNamespace())
			if err != nil {
				return true, nil, err
			}
			return true, newLaggingWatch(inner, func() time.Duration {
				if h.faultsOn() {
					return h.plan.CacheLag
				}
				return 0
			}), nil
		})
	}
}

// laggingWatch holds each event back by lag(), in order: an informer cache
// that serves stale reads while the lag lasts.
type laggingWatch struct {
	inner watch.Interface
	out   chan watch.Event
	stop  chan struct{}
	once  sync.Once
}

func newLaggingWatch(inner watch.Interface, lag func() time.Duration) *laggingWatch {
	w := &laggingWatch{inner: inner, out: make(chan watch.Event, 64), stop: make(chan struct{})}
	go func() {
		defer close(w.out)
		for {
			select {
			case <-w.stop:
				return
			case event, ok := <-inner.ResultChan():
				if !ok {
					return
				}
				if d := lag(); d > 0 {
					select {
					case <-time.After(d):
					case <-w.stop:
						return
					}
				}
				select {
				case w.out <- event:
				case <-w.stop:
					return
				}
			}
		}
	}()
	return w
}
func (w *laggingWatch) ResultChan() <-chan watch.Event { return w.out }
func (w *laggingWatch) Stop()                          { w.once.Do(func() { close(w.stop); w.inner.Stop() }) }

func faultConfigMapName(detector string) string { return "fault-" + detector }

// setBroken flips a fault object without going through the faulty API: the
// environment's own failures are not API faults.
func (h *harness) setBroken(detector string, broken bool) {
	cm, err := h.client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), hotel, faultConfigMapName(detector))
	if err != nil {
		h.t.Errorf("fault object for %s: %v", detector, err)
		return
	}
	next := cm.(*corev1.ConfigMap).DeepCopy()
	next.Data = map[string]string{"broken": fmt.Sprint(broken)}
	if err := h.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), next, hotel); err != nil {
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
		if d.h.enterStall() {
			select {
			case <-ctx.Done():
			case <-time.After(time.Hour):
			}
			d.h.exitStall()
			return nil, ctx.Err()
		}
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
		DetectorID: d.spec.ID, RuleID: "fault", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
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
// enterStall registers a detector about to stall inside the host (hang or slow),
// unless a harness operation is under way, in which case it answers normally.
func (h *harness) enterStall() bool {
	h.gate.Lock()
	defer h.gate.Unlock()
	if h.gate.busy {
		return false
	}
	h.gate.hanging++
	return true
}

func (h *harness) exitStall() {
	h.gate.Lock()
	h.gate.hanging--
	h.gate.Unlock()
}

func (h *harness) beginOperation() {
	for {
		h.gate.Lock()
		h.gate.busy = true
		idle := h.gate.hanging == 0
		h.gate.Unlock()
		if idle {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (h *harness) endOperation() {
	h.gate.Lock()
	h.gate.busy = false
	h.gate.Unlock()
}

func (h *harness) activate(ctx context.Context, rev planRevision) error {
	h.beginOperation()
	defer h.endOperation()
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
	defer h.mu.Unlock()
	h.active, h.remote, h.revision = host, remote, rev
	for _, d := range rev.Detectors {
		if d.Behaviour == behaveHang {
			h.hung[d.ID] = true
		}
	}
	h.rec.Add(sim.Activated{At: h.now(), Revision: rev.Name, Detectors: h.declsOf(rev)})
	if h.ctl != nil {
		return h.ctl.ReconfigureMemory(remote, h.ctl.provider, h.now().UTC())
	}
	return nil
}

func (h *harness) crashHost() {
	h.mu.Lock()
	host := h.active
	h.mu.Unlock()
	if host == nil {
		return
	}
	h.rec.Add(sim.ControllerDown{At: h.now(), Host: true})
	h.beginOperation()
	_ = host.Close()
	h.endOperation()
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

type harnessAgent struct{ h *harness }

// Dispatch answers like the Job dispatcher's boundary does: a result that does
// not satisfy the incident result contract is rejected as a dispatch error
// before the controller sees it (KubernetesJobDispatcher.dispatch).
func (a harnessAgent) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	result, err := a.respond(ctx, request)
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
		// The dispatcher, not the controller, owns the response timeout: the
		// Job executor gives up at request.ResponseDeadline.
		select {
		case <-ctx.Done():
			return IncidentResult{}, ctx.Err()
		case <-time.After(time.Until(request.ResponseDeadline)):
			return IncidentResult{}, errors.New("agent did not answer before the response deadline")
		}
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

// agentSet writes through the same faulty API the controller uses and retries
// like an agent would.
func (h *harness) agentSet(ctx context.Context, detector string, broken bool) {
	// test-polling: allow -- the agent stand-in retries on virtual time
	for attempt := 0; attempt < 5; attempt++ {
		cm, err := h.client.CoreV1().ConfigMaps(hotel).Get(ctx, faultConfigMapName(detector), metav1.GetOptions{})
		if err == nil {
			cm.Data = map[string]string{"broken": fmt.Sprint(broken)}
			if _, err = h.client.CoreV1().ConfigMaps(hotel).Update(ctx, cm, metav1.UpdateOptions{}); err == nil {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---- running the controller ----------------------------------------------------

func (h *harness) crashController(why string) {
	h.mu.Lock()
	cancel, already := h.cancel, h.down
	h.down = true
	h.mu.Unlock()
	if cancel == nil || already {
		return
	}
	h.t.Logf("%s crash: %s", h.now().Sub(h.start).Round(time.Millisecond), why)
	h.rec.Add(sim.ControllerDown{At: h.now()})
	cancel()
}

func (h *harness) runController(ctx context.Context) {
	args := []string{"--namespace", hotel, "--control-namespace", harnessControlNamespace, "--app-root", h.repo,
		"--application", "hotel", "--synthetic-traffic=false", "--lease-duration", "3s", "--drift-batch-window", "20ms",
		"--verification-timeout", "20s", "--closure-stability-window", "5s", "--response-timeout", "30s"}
	for ctx.Err() == nil {
		runCtx, cancel := context.WithCancel(ctx)
		h.mu.Lock()
		h.cancel, h.down = cancel, false
		remote := h.remote
		revision := h.revision
		h.mu.Unlock()
		err := RunWithOptions(runCtx, remote.Detectors, RuntimeOptions{
			Args: args, Stdout: h.out, Stderr: h.out, Client: h.client, Dispatcher: harnessAgent{h},
			Programs: remote.Programs, InformerSync: fastSyncPolicy(), InformerResync: 10 * time.Minute, TerminationLogPath: filepath.Join(h.t.TempDir(), "termination-log"),
			Wired: func(c *Controller) {
				c.SetLifecycleSink(tapSink{h})
				h.mu.Lock()
				defer h.mu.Unlock()
				h.ctl = c
				if h.revision.Name != revision.Name {
					_ = c.ReconfigureMemory(h.remote, c.provider, h.now().UTC())
				}
			},
		})
		cancel()
		h.mu.Lock()
		h.ctl = nil
		wasDown := h.down
		h.down = true
		h.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if !wasDown {
			h.t.Logf("%s controller exited on its own: %v", h.now().Sub(h.start).Round(time.Millisecond), err)
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
	h := &harness{t: t, plan: p, rec: sim.NewRecorder(time.Now).ClosureIsTerminal(), repo: gitRepository(t), start: time.Now(),
		out: &lineLog{start: time.Now()}, controlWrites: map[string]int{}, callCounts: map[string]int{}, hung: map[string]bool{}}
	objects := hotelObjects()
	seen := map[string]bool{}
	for _, rev := range p.Revisions {
		for _, d := range rev.Detectors {
			if !seen[d.ID] {
				seen[d.ID] = true
				objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: faultConfigMapName(d.ID), Namespace: hotel}, Data: map[string]string{"broken": "false"}})
			}
		}
	}
	h.client = fake.NewSimpleClientset(objects...)
	h.installFaults()

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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.runController(ctx) }()
	h.start = time.Now()
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
		cm, err := h.client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), harnessControlNamespace, controllerStateConfigMap)
		if err == nil {
			for k, v := range cm.(*corev1.ConfigMap).Data {
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

	h.mu.Lock()
	hung := map[string]bool{}
	for id := range h.hung {
		hung[id] = true
	}
	h.mu.Unlock()
	return h.rec.Log(), sim.Config{GapFactor: 3, Slack: 12 * time.Second, Settle: p.Settle, Hung: hung}
}

type tapSink struct{ h *harness }

func (s tapSink) RecordLifecycle(e *pb.LifecycleEvent) error {
	s.h.t.Logf("%s lifecycle %s incident=%s dispatchError=%v", s.h.now().Sub(s.h.start).Round(time.Second), e.GetEvent(), e.GetIncidentId(), e.GetDispatchError())
	return s.h.rec.RecordLifecycle(e)
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
