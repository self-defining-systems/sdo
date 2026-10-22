package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// simStreamDefaultRuns keeps the default sweep to about half a minute; a run replays
// hours of virtual time through a durable store.
const simStreamDefaultRuns = 24

// simStreamServices is the number of Services the fault stream draws from.
const simStreamServices = 4

// simStreamMaxTicks bounds a run in virtual one-second ticks, so a stall fails
// loudly instead of hanging.
const simStreamMaxTicks = 6000

// simWorld runs the real controller against a simulated cluster, a scripted
// responder and a scripted broker on a virtual clock. Effects are executed by
// the pump (drain), never concurrently with the test goroutine's reads.
type simWorld struct {
	t          *testing.T
	rng        *rand.Rand
	clock      *simClock
	kube       *simKube
	cache      *simCache
	controller *Controller
	responder  *simResponder
	broker     *simBroker
	config     ControllerConfig
	client     *fake.Clientset
	restarts   int
	storeFails int

	outcomes *memory.JSONStore

	mu         sync.Mutex
	closed     []IncidentClosure
	violations []string
}

func simServiceRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: name}
}

func simServiceName(i int) string { return fmt.Sprintf("svc-%d", i) }

func (w *simWorld) violate(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.violations = append(w.violations, fmt.Sprintf("t+%s: ", w.clock.now.Sub(simEpoch))+fmt.Sprintf(format, args...))
}

// setPort writes a Service's targetPort straight into the simulated server, as
// a foreign actor would (fault injection), visible to the cache at once.
func (w *simWorld) setPort(name string, port int64) {
	ref := simServiceRef(name)
	object := w.kube.objects[ref]
	ports, _, _ := unstructuredPorts(object.Object)
	ports[0].(map[string]any)["targetPort"] = port
	w.kube.bump(object)
}

func unstructuredPorts(object map[string]any) ([]any, bool, error) {
	spec, _ := object["spec"].(map[string]any)
	ports, _ := spec["ports"].([]any)
	return ports, len(ports) > 0, nil
}

func (w *simWorld) faulty() []string {
	var names []string
	for i := 0; i < simStreamServices; i++ {
		ports, _, _ := unstructuredPorts(w.kube.objects[simServiceRef(simServiceName(i))].Object)
		if ports[0].(map[string]any)["targetPort"] == int64(9999) {
			names = append(names, simServiceName(i))
		}
	}
	return names
}

// simBehavior is what the scripted responder does with one dispatched incident.
type simBehavior int

const (
	simFixAll simBehavior = iota
	simFixNone
	simFixFirst
	simDispatchError
)

// simResponder is the scripted responder: it repairs the simulated cluster the
// way its behavior says and returns a well-formed result.
type simResponder struct {
	world    *simWorld
	behavior func() simBehavior
	mu       sync.Mutex
	calls    int
}

func (r *simResponder) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	w := r.world
	w.mu.Lock()
	behavior := r.behavior()
	var resources []sdk.ObjectRef
	switch behavior {
	case simDispatchError:
		w.mu.Unlock()
		return IncidentResult{}, errors.New("simulated: result transport failure")
	case simFixAll, simFixFirst:
		for _, name := range w.faulty() {
			ref := simServiceRef(name)
			patch, _ := json.Marshal(map[string]any{"spec": map[string]any{"ports": []any{map[string]any{"port": 80, "targetPort": 8080}}}})
			if err := w.kube.Patch(ctx, ref, types.MergePatchType, patch); err != nil {
				w.mu.Unlock()
				return IncidentResult{}, err
			}
			resources = append(resources, ref)
			if behavior == simFixFirst {
				break
			}
		}
	}
	w.mu.Unlock()
	if len(resources) == 0 {
		resources = []sdk.ObjectRef{simServiceRef(simServiceName(0))}
	}
	return fastloopGateResult(request, resources), nil
}

// simBroker is the scripted incident broker. It rejects some closures once.
type simBroker struct {
	world  *simWorld
	reject func() bool
	mu     sync.Mutex
	failed map[string]bool
}

func (b *simBroker) PrepareIncident(_ context.Context, id string) (IncidentWorkspace, error) {
	return IncidentWorkspace{IncidentID: id, Worktree: "/worktrees/" + id, BaseCommit: "base"}, nil
}

func (b *simBroker) ProcessClosure(_ context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	id := closure.Request.IncidentID
	b.world.mu.Lock()
	reject := b.reject()
	b.world.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if reject && !b.failed[id] {
		b.failed[id] = true
		return ClosureReceipt{}, errors.New("simulated: broker validation rejected the closure")
	}
	// Only a persisted closure reaches the broker, and a retry of it must derive
	// the identical record.
	b.world.checkOutcomeDocument(closure)
	return ClosureReceipt{IncidentID: id, Worktree: "/worktrees/" + id, BaseCommit: "base", OutcomeCommit: "outcome-" + id, AckToken: "ack-" + id}, nil
}

func (b *simBroker) AcknowledgeClosure(context.Context, ClosureReceipt) error { return nil }
func (b *simBroker) ReleaseIncident(context.Context, string) error            { return nil }

var simEpoch = newSimClock().now

func newSimWorld(t *testing.T, seed uint64) *simWorld {
	t.Helper()
	rng := newSimRNG(seed)
	w := &simWorld{t: t, rng: rng, clock: newSimClock(), outcomes: memory.NewJSONStore(t.TempDir())}
	objects := make([]*unstructured.Unstructured, 0, simStreamServices)
	for i := 0; i < simStreamServices; i++ {
		objects = append(objects, simServiceNamed(simServiceName(i), 8080))
	}
	faults := drawSimFaults(rng)
	faults.failAfterCommit = func() bool { return false }
	faults.churnPerPatch = func() int { return 0 }
	w.kube = newSimKube(faults, objects...)
	w.cache = newSimCache(w.kube, faults)
	rejectP, errorP, noneP, firstP := 0.3*rng.Float64(), 0.3*rng.Float64(), 0.3*rng.Float64(), 0.3*rng.Float64()
	w.responder = &simResponder{world: w, behavior: func() simBehavior {
		switch x := rng.Float64(); {
		case x < errorP:
			return simDispatchError
		case x < errorP+noneP:
			return simFixNone
		case x < errorP+noneP+firstP:
			return simFixFirst
		}
		return simFixAll
	}}
	w.broker = &simBroker{world: w, reject: func() bool { return rng.Float64() < rejectP }, failed: map[string]bool{}}
	w.config = ControllerConfig{Application: "app", Namespace: "app", SourceCommit: "c", DeployedCommit: "c", RepositoryWorktree: "/tmp/app",
		ResponseTimeout: time.Minute, VerificationTimeout: 3 * time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions",
		MaxFollowUps: 2, FollowUpCooldown: 25 * time.Second}
	w.client = fake.NewSimpleClientset()
	w.injectStoreFaults(0.15 * rng.Float64())
	w.startController()
	return w
}

// injectStoreFaults makes the ConfigMap state store fail writes with the given
// probability, either before the write lands or after it, when the caller
// cannot tell whether it persisted.
func (w *simWorld) injectStoreFaults(p float64) {
	configMaps := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	for _, verb := range []string{"create", "update"} {
		verb := verb
		w.client.PrependReactor(verb, "configmaps", func(action clienttesting.Action) (bool, kruntime.Object, error) {
			w.mu.Lock()
			x := w.rng.Float64()
			w.mu.Unlock()
			if x >= p {
				return false, nil, nil
			}
			w.storeFails++
			if x < p/2 {
				return true, nil, errors.New("simulated: state write failed before landing")
			}
			var err error
			object := action.(clienttesting.CreateAction)
			if verb == "update" {
				err = w.client.Tracker().Update(configMaps, object.GetObject(), action.GetNamespace())
			} else {
				err = w.client.Tracker().Create(configMaps, object.GetObject(), action.GetNamespace())
			}
			if err != nil {
				return true, nil, err
			}
			return true, nil, errors.New("simulated: state write landed but the reply was lost")
		})
	}
}

// startController builds a controller over the durable state store, as a fresh
// process would, restoring whatever the previous one persisted.
func (w *simWorld) startController() {
	controller, err := NewController(w.config, []sdk.Detector{fastloopDetector{}}, w.cache, w.responder, w.clock.now)
	if err != nil {
		w.t.Fatal(err)
	}
	controller.now = w.clock.Now
	controller.dispatchJitter = w.rng.Float64
	controller.OnError = func(err error) { w.t.Logf("controller effect: %v", err) }
	controller.OnIncidentClosed = w.onClosed
	if err := controller.SetIncidentBroker(w.broker); err != nil {
		w.t.Fatal(err)
	}
	if err := controller.AttachStateStore(context.Background(), lifecycleio.NewConfigMapStateStore(w.client, "app", "sdo-controller-state")); err != nil {
		w.t.Fatalf("restore controller state: %v", err)
	}
	if err := controller.EvaluateAll(context.Background(), w.clock.Now()); err != nil {
		w.t.Fatal(err)
	}
	w.controller = controller
	w.t.Cleanup(controller.routingSweeps.Wait)
}

// restart replaces the controller with a fresh process over the same state.
func (w *simWorld) restart() {
	w.restarts++
	w.startController()
}

func (w *simWorld) persist() {
	if err := w.controller.PersistState(context.Background()); err != nil {
		w.t.Logf("persist controller state: %v", err)
	}
}

// onClosed checks the verified-close invariant at the moment of closure.
func (w *simWorld) onClosed(closure IncidentClosure) {
	w.mu.Lock()
	w.closed = append(w.closed, closure)
	w.mu.Unlock()
	if faulty := w.faulty(); len(faulty) > 0 {
		w.violate("incident %s closed verified while %v still faulty", closure.Request.IncidentID, faulty)
	}
}

// checkOutcomeDocument derives the closure's outcome the way the broker's edge
// does and requires a record that satisfies its contract and survives the
// repository adapter byte-for-byte: written, read back, and written again.
func (w *simWorld) checkOutcomeDocument(closure IncidentClosure) {
	id := closure.Request.IncidentID
	ctx := context.Background()
	derived, err := ProtoOutcomeDeriver{}.Derive(ctx, closure, "outcome-commit", "codex", "sim")
	if err != nil {
		w.violate("incident %s: outcome derivation failed: %v", id, err)
		return
	}
	if err := protovalidate.Validate(derived.Record); err != nil {
		w.violate("incident %s: outcome record violates its contract: %v", id, err)
		return
	}
	stored, err := w.outcomes.Put(ctx, derived.Record)
	if err != nil {
		w.violate("incident %s: outcome write failed: %v", id, err)
		return
	}
	read, err := w.outcomes.Get(ctx, id)
	if err != nil {
		w.violate("incident %s: outcome read failed: %v", id, err)
		return
	}
	if !proto.Equal(read.Record, derived.Record) || read.IncidentID != stored.IncidentID {
		w.violate("incident %s: outcome did not round-trip through the repository", id)
	}
	if _, err := w.outcomes.Put(ctx, read.Record); err != nil {
		w.violate("incident %s: re-writing the stored outcome failed: %v", id, err)
	}
}

func (w *simWorld) step() {
	if err := w.controller.Step(context.Background(), w.clock.Now(), nil); err != nil {
		w.t.Fatalf("step at %s: %v", w.clock.Now(), err)
	}
	w.persist()
}

func (w *simWorld) await(ready func() bool) {
	w.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(200 * time.Microsecond)
	}
	w.t.Fatal("timed out waiting for a simulated effect to finish")
}

// drain executes every pending controller effect, stepping after each, until
// the controller has nothing left to do at the current virtual time.
func (w *simWorld) drain() {
	ctx := context.Background()
	for round := 0; round < 100; round++ {
		progressed := false
		w.persist()
		if effect, ok := w.controller.PendingWorkspaceEffect(); ok {
			if err := w.controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				return
			}
			w.await(func() bool { return len(w.controller.workspaceResults) > 0 })
			progressed = true
		} else if effect, ok := w.controller.PendingDispatchEffect(); ok {
			if err := w.controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				return
			}
			w.await(func() bool { return len(w.controller.results) > 0 })
			progressed = true
		} else if effect, ok := w.controller.PendingClosureEffect(); ok {
			if err := w.controller.ExecuteClosureEffect(ctx, effect); err != nil {
				return
			}
			w.await(func() bool { return len(w.controller.closureResults) > 0 })
			progressed = true
		} else if effect, ok := w.controller.PendingClosureAcknowledgmentEffect(); ok {
			if err := w.controller.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
				return
			}
			w.await(func() bool { return len(w.controller.acknowledgmentResults) > 0 })
			progressed = true
		} else if effect, ok := w.controller.PendingReleaseEffect(); ok {
			if err := w.controller.ExecuteReleaseEffect(ctx, effect); err != nil {
				return
			}
			w.await(func() bool { return len(w.controller.releaseResults) > 0 })
			progressed = true
		}
		if !progressed {
			return
		}
		w.step()
	}
	w.violate("effect pump did not settle in 100 rounds")
}

func (w *simWorld) idle() bool {
	if w.controller.IncidentOpen() || len(w.faulty()) > 0 {
		return false
	}
	_, closing := w.controller.PendingIncidentClosure()
	return !closing
}

// simInjection breaks one Service at a virtual tick.
type simInjection struct{ tick, service int }

// simStreamSchedule is the fault schedule of one stream run: when Services
// break, and how often the controller process restarts (per half-tick).
type simStreamSchedule struct {
	injections []simInjection
	restartP   float64
}

// drawSimStreamSchedule is the seed's default schedule.
func drawSimStreamSchedule(rng *rand.Rand) simStreamSchedule {
	sched := simStreamSchedule{injections: make([]simInjection, 3+rng.IntN(4))}
	for i := range sched.injections {
		sched.injections[i] = simInjection{tick: 2 + rng.IntN(600), service: rng.IntN(simStreamServices)}
	}
	sched.restartP = 0.02 * rng.Float64()
	return sched
}

// decodeSimStreamSchedule reads a schedule from fuzz bytes: one byte of restart
// probability, then three bytes per injection (tick high, tick low, service).
func decodeSimStreamSchedule(raw []byte) simStreamSchedule {
	sched := simStreamSchedule{}
	if len(raw) > 0 {
		sched.restartP = 0.02 * float64(raw[0]) / 255
		raw = raw[1:]
	}
	for ; len(raw) >= 3 && len(sched.injections) < 8; raw = raw[3:] {
		sched.injections = append(sched.injections, simInjection{
			tick:    2 + (int(raw[0])<<8|int(raw[1]))%600,
			service: int(raw[2]) % simStreamServices,
		})
	}
	return sched
}

// simStreamRun drives one incident stream; a nil schedule draws the seed's.
func simStreamRun(t *testing.T, seed uint64, sched *simStreamSchedule) {
	t.Helper()
	w := newSimWorld(t, seed)
	if sched == nil {
		drawn := drawSimStreamSchedule(w.rng)
		sched = &drawn
	}
	for tick := 0; tick < simStreamMaxTicks; tick++ {
		w.clock.now = w.clock.now.Add(time.Second + time.Duration(w.rng.IntN(400))*time.Millisecond)
		for _, injection := range sched.injections {
			if injection.tick == tick {
				w.setPort(simServiceName(injection.service), 9999)
			}
		}
		if w.rng.Float64() < sched.restartP {
			w.restart()
		}
		w.step()
		if w.rng.Float64() < sched.restartP {
			w.restart()
		}
		w.drain()
		if tick > 650 {
			if review, _ := w.controller.DetectorReviewStatus(); review || w.idle() {
				break
			}
		}
	}
	// A responder that never repairs exhausts the follow-ups and leaves the incident in
	// detector review, which by design waits for health to clear (docs/architecture.md).
	// Any other non-idle end is a stall.
	review, reason := w.controller.DetectorReviewStatus()
	if !w.idle() && !review {
		w.violate("stalled: incident open=%t faulty=%v after %d ticks", w.controller.IncidentOpen(), w.faulty(), simStreamMaxTicks)
	}
	if review {
		t.Logf("ended in detector review: %s", reason)
	}
	t.Logf("injections=%d closures=%d dispatches=%d restarts=%d storeFaults=%d", len(sched.injections), len(w.closed), w.responder.calls, w.restarts, w.storeFails)
	if len(w.violations) > 0 {
		t.Fatalf("invariants violated (replay with SDO_SIM_SEED=%d):\n%v", seed, w.violations)
	}
}

func TestSimIncidentStream(t *testing.T) {
	sweepSimSeedsN(t, simStreamDefaultRuns, func(t *testing.T, seed uint64) { simStreamRun(t, seed, nil) })
}

// FuzzSimIncidentStream lets the Go fuzzer search seeds and fault schedules:
//
//	go test ./sdo/operator/runtime -run '^$' -fuzz FuzzSimIncidentStream -fuzztime 4m
//
// A crashing input is saved under testdata/fuzz and replays as a normal test.
func FuzzSimIncidentStream(f *testing.F) {
	f.Add(uint64(1), []byte{40, 0, 10, 1, 0, 200, 2, 1, 90, 0})
	f.Add(uint64(9), []byte{200, 0, 5, 0, 0, 5, 1, 0, 5, 2})
	f.Add(uint64(21), []byte{}) // no injections: the stream stays idle
	f.Fuzz(func(t *testing.T, seed uint64, raw []byte) {
		sched := decodeSimStreamSchedule(raw)
		simStreamRun(t, seed, &sched)
	})
}

// TestSimSoak runs seeded plan and stream sweeps until SDO_SIM_SOAK (a Go
// duration, capped at five minutes) elapses, and reports the first failing seed.
//
//	SDO_SIM_SOAK=4m go test ./sdo/operator/runtime -run TestSimSoak
func TestSimSoak(t *testing.T) {
	raw := os.Getenv("SDO_SIM_SOAK")
	if raw == "" {
		t.Skip("set SDO_SIM_SOAK=<duration> to run the soak")
	}
	budget, err := time.ParseDuration(raw)
	if err != nil || budget <= 0 {
		t.Fatalf("invalid SDO_SIM_SOAK %q", raw)
	}
	budget = min(budget, simSoakCap)
	deadline := time.Now().Add(budget)
	runs := 0
	for seed := uint64(1_000_000); time.Now().Before(deadline); seed++ {
		runs++
		if !t.Run(fmt.Sprintf("stream/seed=%d", seed), func(t *testing.T) { simStreamRun(t, seed, nil) }) {
			t.Fatalf("soak failed at seed %d (replay: SDO_SIM_SEED=%d go test -run TestSimIncidentStream)", seed, seed)
		}
	}
	t.Logf("soak ran %d stream seeds in %s", runs, budget)
}

// simSoakCap is the project's wall-clock cap for fuzz and property runs.
const simSoakCap = 5 * time.Minute
