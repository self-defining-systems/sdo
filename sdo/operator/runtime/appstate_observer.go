package runtime

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	kclock "k8s.io/utils/clock"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
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
	AppStateRead(context.Context) (carried.Values, error)
}

// AppStateObserver reads every registered app-state reader on its own interval
// in the background, off the detection path: each read has a deadline of
// AppStateReadTimeout, at most AppStateMaxConcurrent run at once, a reader
// still running is skipped (never queued) and only the last finished result is
// kept. A snapshot attaches what is already finished and never waits.
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
}

type appStateReader struct {
	id       string
	interval time.Duration
	read     func(context.Context) (carried.Values, error)
}

// NewAppStateObserver returns an observer that records into store. clock
// drives its ticker and now stamps snapshots.
func NewAppStateObserver(store *AppStateStore, clock kclock.WithTicker, now func() time.Time) *AppStateObserver {
	return &AppStateObserver{store: store, clock: timerClock(clock), now: now, running: map[string]bool{}, next: map[string]time.Time{}, slots: make(chan struct{}, AppStateMaxConcurrent)}
}

// Store is the state the observer records into.
func (o *AppStateObserver) Store() *AppStateStore { return o.store }

// Reconfigure replaces the reader set from a memory revision's detectors. It
// refuses duplicate readers and more than carried.MaxReaders.
func (o *AppStateObserver) Reconfigure(detectors []sdk.Detector) error {
	var readers []appStateReader
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
		readers = append(readers, appStateReader{id: spec.ID, interval: max(spec.Interval, appStateMinInterval), read: r.AppStateRead})
	}
	if len(readers) > carried.MaxReaders {
		return fmt.Errorf("%d app-state readers exceed the limit of %d", len(readers), carried.MaxReaders)
	}
	o.store.Retain(declared)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readers = readers
	o.gen++
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
	readCtx, cancel := context.WithTimeout(ctx, AppStateReadTimeout)
	values, err := r.read(readCtx)
	cancel()
	snap := carried.Snapshot{At: o.now().UTC(), Values: values}
	if err != nil {
		snap = carried.Snapshot{At: snap.At, Error: boundedAppStateError(err)}
	}
	o.mu.Lock()
	// A read that outlived its revision (or a Stop) is dropped, never recorded
	// against a reader that was removed and added back.
	if ctx.Err() == nil && gen == o.gen {
		o.store.Record(r.id, snap)
	}
	delete(o.running, r.id)
	o.mu.Unlock()
	<-o.slots
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
