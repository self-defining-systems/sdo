package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	kclock "k8s.io/utils/clock"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/podread"
)

// Bounds of background app-state reads.
const (
	// AppStateReadTimeout is the deadline of one ReadState call.
	AppStateReadTimeout = 5 * time.Second
	// AppStateMaxConcurrent is how many reads run at once across readers.
	AppStateMaxConcurrent = 4
	// appStateMinInterval floors a reader's interval.
	appStateMinInterval = 10 * time.Second
	appStateTick        = time.Second
)

// appStateReaderDetector is a registered detector that is an app-state reader.
// The memory host runs it; the controller only calls AppStateRead.
type appStateReaderDetector interface {
	sdk.Detector
	AppStateReader() bool
	AppStateNoisy() []string
	// AppStateRead runs the reader against the frozen results of the pod reads
	// it declares (nil when it declares none).
	AppStateRead(context.Context, map[string]podread.Result) (carried.Values, error)
}

// AppStateObserver reads every registered app-state reader on its own interval
// in the background, off the detection path: each read has a deadline of
// AppStateReadTimeout, at most AppStateMaxConcurrent run at once, a reader
// still running is skipped (never queued) and only the last finished result is
// kept. A snapshot attaches what is already finished and never waits.
//
// A reader's pod reads belong to the reader, not to the detection cycle: the
// observer collects them for that reader alone just before each read, under a
// full podread.CollectionBudget and podread.MaxConcurrent, one reader's
// collection at a time (podTurn), so any number of readers neither starve each
// other nor the detection designer's own pod reads. The reader's interval is
// at least podread.MinInterval, which bounds the exec load.
type AppStateObserver struct {
	store *AppStateStore
	clock kclock.WithTicker
	now   func() time.Time

	mu      sync.Mutex
	readers []appStateReader
	running map[string]bool
	next    map[string]time.Time
	gen     uint64
	slots   chan struct{}
	cancel  context.CancelFunc
	wait    sync.WaitGroup

	podReads  podReadEnv
	podTurn   chan struct{}
	latest    sdk.DetectionContext
	logged    map[string]string
}

// podReadEnv is what a reader's pod reads run with.
type podReadEnv struct {
	client    kubernetes.Interface
	exec      kubereads.PodExec
	protected string
}

type appStateReader struct {
	id       string
	interval time.Duration
	read     func(context.Context, map[string]podread.Result) (carried.Values, error)
	pods     *PodReadObserver // nil when the reader declares no pod reads
	podsErr  string           // why its pod reads cannot run, when they cannot
}

// NewAppStateObserver returns an observer that records into store. clock
// drives its ticker and now stamps snapshots.
func NewAppStateObserver(store *AppStateStore, clock kclock.WithTicker, now func() time.Time) *AppStateObserver {
	return &AppStateObserver{store: store, clock: timerClock(clock), now: now, running: map[string]bool{}, next: map[string]time.Time{}, slots: make(chan struct{}, AppStateMaxConcurrent), podTurn: make(chan struct{}, 1), logged: map[string]string{}}
}

// UsePodReads gives the observer what it needs to run the pod reads readers
// declare. Without it a reader that declares one reports why it cannot run.
func (o *AppStateObserver) UsePodReads(client kubernetes.Interface, exec kubereads.PodExec, protected string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.podReads = podReadEnv{client: client, exec: exec, protected: protected}
}

// observe keeps the newest cluster snapshot, which names the Pods a reader's
// pod reads target.
func (o *AppStateObserver) observe(base sdk.DetectionContext) {
	o.mu.Lock()
	o.latest = base
	o.mu.Unlock()
}

// Store is the state the observer records into.
func (o *AppStateObserver) Store() *AppStateStore { return o.store }

// Reconfigure replaces the reader set from a memory revision's detectors. It
// refuses duplicate readers and more than carried.MaxReaders.
func (o *AppStateObserver) Reconfigure(detectors []sdk.Detector) error {
	var readers []appStateReader
	o.mu.Lock()
	env := o.podReads
	o.mu.Unlock()
	declared := map[string][]string{}
	for _, d := range detectors {
		r, ok := d.(appStateReaderDetector)
		if !ok || !r.AppStateReader() {
			continue
		}
		spec := d.Spec()
		if _, dup := declared[spec.ID]; dup {
			return fmt.Errorf("duplicate app-state reader %q", spec.ID)
		}
		declared[spec.ID] = r.AppStateNoisy()
		reader := appStateReader{id: spec.ID, interval: max(spec.Interval, appStateMinInterval), read: r.AppStateRead}
		queries, err := podread.Queries([]sdk.Detector{d})
		if err != nil {
			return fmt.Errorf("app-state reader %q: %w", spec.ID, err)
		}
		if len(queries) > 0 {
			reader.interval = max(reader.interval, podread.MinInterval)
			if env.client == nil {
				reader.podsErr = "skipped: no Kubernetes client to run the reader's pod reads"
			} else {
				reader.pods = newPodReadObserver(env.client, env.exec, env.protected, queries, o.now)
			}
		}
		readers = append(readers, reader)
	}
	if len(readers) > carried.MaxReaders {
		return fmt.Errorf("%d app-state readers exceed the limit of %d", len(readers), carried.MaxReaders)
	}
	o.store.Retain(declared)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readers = readers
	o.gen++
	clear(o.logged)
	ids := make([]string, len(readers))
	for i, r := range readers {
		ids[i] = r.id
	}
	slog.Info("app_state_readers_configured", "count", len(ids), "readers", ids)
	for id := range o.next {
		if _, ok := declared[id]; !ok {
			delete(o.next, id)
		}
	}
	return nil
}

// Start reads until Stop; calling it on a running observer is a no-op.
func (o *AppStateObserver) Start(ctx context.Context) {
	o.mu.Lock()
	if o.cancel != nil {
		o.mu.Unlock()
		return
	}
	running, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.mu.Unlock()
	o.wait.Add(1)
	go func() {
		defer o.wait.Done()
		ticker := o.clock.NewTicker(appStateTick)
		defer ticker.Stop()
		for {
			o.dispatch(running)
			select {
			case <-running.Done():
				return
			case <-ticker.C():
			}
		}
	}()
}

// Stop ends the loop and waits for reads in flight (each is bounded by
// AppStateReadTimeout and by the cancelled context).
func (o *AppStateObserver) Stop() {
	o.mu.Lock()
	cancel := o.cancel
	o.cancel = nil
	o.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	o.wait.Wait()
}

// dispatch starts every due reader that is not running while a slot is free.
func (o *AppStateObserver) dispatch(ctx context.Context) {
	now := o.now()
	o.mu.Lock()
	defer o.mu.Unlock()
	// Longest-waiting first, so slow reads of early readers cannot starve later ones.
	due := slices.Clone(o.readers)
	sort.SliceStable(due, func(i, j int) bool {
		if a, b := o.next[due[i].id], o.next[due[j].id]; !a.Equal(b) {
			return a.Before(b)
		}
		return due[i].id < due[j].id
	})
	for _, r := range due {
		if o.running[r.id] || now.Before(o.next[r.id]) {
			continue
		}
		if r.pods != nil && o.latest == nil {
			continue // its pod reads have no target before the first snapshot
		}
		select {
		case o.slots <- struct{}{}:
		default:
			continue
		}
		o.running[r.id] = true
		o.next[r.id] = now.Add(r.interval)
		o.wait.Add(1)
		go o.run(ctx, r, o.gen)
	}
}

func (o *AppStateObserver) run(ctx context.Context, r appStateReader, gen uint64) {
	defer o.wait.Done()
	reads, err := o.collectPodReads(ctx, r)
	var values carried.Values
	if err == nil {
		readCtx, cancel := context.WithTimeout(ctx, AppStateReadTimeout)
		values, err = r.read(readCtx, reads)
		cancel()
	}
	snap := carried.Snapshot{At: o.now().UTC(), Values: values}
	if err != nil {
		snap = carried.Snapshot{At: snap.At, Error: boundedAppStateError(err)}
	}
	o.mu.Lock()
	// A read that outlived its revision (or a Stop) is dropped, never recorded
	// against a reader that was removed and added back.
	if ctx.Err() == nil && gen == o.gen {
		o.store.Record(r.id, snap)
		o.logRead(r.id, snap)
	}
	delete(o.running, r.id)
	o.mu.Unlock()
	<-o.slots
}

// collectPodReads runs the reader's own pod reads, or says why it cannot.
func (o *AppStateObserver) collectPodReads(ctx context.Context, r appStateReader) (map[string]podread.Result, error) {
	if r.podsErr != "" {
		return nil, errors.New(r.podsErr)
	}
	if r.pods == nil {
		return nil, nil
	}
	o.mu.Lock()
	base := o.latest
	o.mu.Unlock()
	if base == nil {
		return nil, errors.New("skipped: no cluster snapshot has been observed yet, so the reader's pod reads have no target")
	}
	select {
	case o.podTurn <- struct{}{}:
		defer func() { <-o.podTurn }()
	case <-ctx.Done():
		return nil, fmt.Errorf("skipped: waiting for another reader's pod reads: %w", ctx.Err())
	}
	return r.pods.collectFresh(ctx, base), nil
}

// logRead logs a reader's first result and each change of its outcome, so the
// controller log shows every reader's state outside an incident without a line
// per read. Called with o.mu held.
func (o *AppStateObserver) logRead(id string, snap carried.Snapshot) {
	outcome := "ok"
	if snap.Error != "" {
		outcome = "error: " + snap.Error
	}
	if prev, seen := o.logged[id]; seen && prev == outcome {
		return
	}
	o.logged[id] = outcome
	if snap.Error != "" {
		slog.Warn("app_state_reader", "reader", id, "outcome", "error", "error", snap.Error)
		return
	}
	slog.Info("app_state_reader", "reader", id, "outcome", "ok", "keys", len(snap.Values.Keys()))
}

// boundedAppStateError keeps a reader's error to carried.MaxValueBytes: it
// reaches clues and the observation, so it is as bounded as a value.
func boundedAppStateError(err error) string {
	text := err.Error()
	if len(text) > carried.MaxValueBytes {
		text = text[:carried.MaxValueBytes]
	}
	return text
}

// AppStateSnapshotProvider attaches each reader's last finished result to the
// snapshot. It never waits for a read.
type AppStateSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *AppStateObserver
}

func (p AppStateSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return base, err
	}
	p.Observer.observe(base)
	return inputs.New(base).WithAppState(appStateResults(p.Observer.store.Results())), nil
}

type appStateResults map[string]carried.Result

func (r appStateResults) AppStateResult(id string) (carried.Result, bool) {
	v, ok := r[id]
	return v, ok
}
func (r appStateResults) AppStateResults() map[string]carried.Result {
	out := make(map[string]carried.Result, len(r))
	for id, v := range r {
		out[id] = v
	}
	return out
}
