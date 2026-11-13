package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
)

// fakeReader is a registered reader detector.
type fakeReader struct {
	id       string
	interval time.Duration
	read     func(context.Context) (carried.Values, error)
	noisy    []string
}

func (f fakeReader) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: f.id, Class: sdk.DetectorClassHealth, Interval: f.interval}
}
func (fakeReader) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (f fakeReader) AppStateReader() bool                                     { return true }
func (f fakeReader) AppStateNoisy() []string                                  { return f.noisy }
func (f fakeReader) AppStateRead(ctx context.Context, _ sdk.DetectionContext, _ map[string]podread.Result) (carried.Values, error) {
	return f.read(ctx)
}

func valuesOf(kv ...string) carried.Values {
	var v carried.Values
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return v
}

func newTestObserver(t *testing.T, readers ...fakeReader) (*AppStateObserver, *AppStateStore) {
	t.Helper()
	store := NewAppStateStore(DefaultStateSettle)
	observer := NewAppStateObserver(store, lifecycleio.Clock(), lifecycleio.Clock().Now)
	detectors := make([]sdk.Detector, len(readers))
	for i, r := range readers {
		detectors[i] = r
	}
	if err := observer.Reconfigure(detectors); err != nil {
		t.Fatal(err)
	}
	return observer, store
}

func TestAppStateObserverReadsEachReaderOnItsOwnInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fast, slow atomic.Int32
		observer, store := newTestObserver(t,
			fakeReader{id: "fast", interval: time.Minute, read: func(context.Context) (carried.Values, error) {
				fast.Add(1)
				return valuesOf("n", "1"), nil
			}},
			fakeReader{id: "slow", interval: 3 * time.Minute, read: func(context.Context) (carried.Values, error) {
				slow.Add(1)
				return valuesOf("n", "1"), nil
			}})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(3*time.Minute + 5*time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		if fast.Load() != 4 || slow.Load() != 2 {
			t.Fatalf("reads: fast=%d slow=%d, want 4 and 2", fast.Load(), slow.Load())
		}
		if r, ok := store.Result("fast"); !ok || len(r.Recent) != 4 {
			t.Fatalf("fast ring = %+v", r)
		}
	})
}

func TestAppStateObserverSkipsAReaderStillRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var started atomic.Int32
		release := make(chan struct{})
		observer, store := newTestObserver(t, fakeReader{id: "stuck", interval: 10 * time.Second, read: func(ctx context.Context) (carried.Values, error) {
			started.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
				return carried.Values{}, ctx.Err()
			}
			return valuesOf("n", "1"), nil
		}})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if started.Load() != 1 {
			t.Fatalf("started = %d", started.Load())
		}
		close(release)
		synctest.Wait()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		if started.Load() > 4 {
			t.Fatalf("a running reader was queued: %d starts", started.Load())
		}
		if _, ok := store.Result("stuck"); !ok {
			t.Fatal("no result recorded")
		}
	})
}

func TestAppStateObserverEnforcesTheFiveSecondDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer, store := newTestObserver(t, fakeReader{id: "slow", interval: time.Hour, read: func(ctx context.Context) (carried.Values, error) {
			<-ctx.Done()
			return carried.Values{}, ctx.Err()
		}})
		ctx, cancel := context.WithCancel(context.Background())
		start := lifecycleio.Clock().Now()
		observer.Start(ctx)
		time.Sleep(AppStateReadTimeout + time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		r, ok := store.Result("slow")
		if !ok || len(r.Recent) != 1 || r.Recent[0].Error == "" {
			t.Fatalf("result = %+v", r)
		}
		if got := r.Recent[0].At.Sub(start); got != AppStateReadTimeout {
			t.Fatalf("the read ended after %s, want %s", got, AppStateReadTimeout)
		}
	})
}

func TestAppStateObserverRunsAtMostFourReadsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		running, peak := 0, 0
		var readers []fakeReader
		for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
			readers = append(readers, fakeReader{id: id, interval: time.Minute, read: func(ctx context.Context) (carried.Values, error) {
				mu.Lock()
				running++
				peak = max(peak, running)
				mu.Unlock()
				time.Sleep(2 * time.Second)
				mu.Lock()
				running--
				mu.Unlock()
				return valuesOf("n", "1"), nil
			}})
		}
		observer, store := newTestObserver(t, readers...)
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		if peak != AppStateMaxConcurrent {
			t.Fatalf("peak concurrency = %d, want %d", peak, AppStateMaxConcurrent)
		}
		for _, r := range readers {
			if _, ok := store.Result(r.id); !ok {
				t.Errorf("reader %s was starved", r.id)
			}
		}
	})
}

func TestAppStateSnapshotNeverWaitsForARead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		observer, store := newTestObserver(t, fakeReader{id: "blocked", interval: time.Minute, read: func(ctx context.Context) (carried.Values, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return valuesOf("n", "1"), nil
		}})
		store.Record("blocked", carried.Snapshot{At: lifecycleio.Clock().Now(), Values: valuesOf("n", "0")})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		synctest.Wait()
		provider := AppStateSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, Observer: observer}
		before := lifecycleio.Clock().Now()
		view, err := provider.Snapshot(context.Background())
		if err != nil || time.Since(before) != 0 {
			t.Fatalf("snapshot err=%v waited=%s", err, time.Since(before))
		}
		got, ok := inputs.New(view).AppStateResult("blocked")
		if !ok || len(got.Recent) != 1 {
			t.Fatalf("snapshot must carry the last finished result: %+v", got)
		}
		if v, _ := got.Recent[0].Values.Get("n"); v != "0" {
			t.Fatalf("carried %q, want the last finished value", v)
		}
		close(release)
		synctest.Wait()
		cancel()
		observer.Stop()
	})
}

func TestAppStateFailureIsRecordedAsAReadError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer, store := newTestObserver(t, fakeReader{id: "bad", interval: time.Minute, read: func(context.Context) (carried.Values, error) {
			return carried.Values{}, errors.New("store down")
		}})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		synctest.Wait()
		cancel()
		observer.Stop()
		r, _ := store.Result("bad")
		if len(r.Recent) != 1 || r.Recent[0].Error != "store down" || r.Recent[0].Values.Len() != 0 {
			t.Fatalf("result = %+v", r)
		}
		if _, err := carried.Require(inputs.New(sdktest.Snapshot{}).WithAppState(appStateSource{"bad": r}), "bad"); err == nil {
			t.Fatal("Require must fail for a failed reader")
		}
	})
}

func TestAppStateReconfigureRefusesTooManyOrDuplicateReaders(t *testing.T) {
	observer := NewAppStateObserver(NewAppStateStore(0), lifecycleio.Clock(), lifecycleio.Clock().Now)
	var many []sdk.Detector
	for i := 0; i <= carried.MaxReaders; i++ {
		many = append(many, fakeReader{id: string(rune('a'+i)) + "x", interval: time.Minute})
	}
	if err := observer.Reconfigure(many); err == nil {
		t.Fatal("more than 16 readers must be refused")
	}
	if err := observer.Reconfigure([]sdk.Detector{fakeReader{id: "a", interval: time.Minute}, fakeReader{id: "a", interval: time.Minute}}); err == nil {
		t.Fatal("duplicate reader ids must be refused")
	}
}

type appStateSource map[string]carried.Result

func (s appStateSource) AppStateResult(id string) (carried.Result, bool) {
	r, ok := s[id]
	return r, ok
}

func TestAppStateRoundRobinsSoSlowReadersCannotStarveLaterOnes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var readers []fakeReader
		for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
			readers = append(readers, fakeReader{id: id, interval: 10 * time.Second, read: func(ctx context.Context) (carried.Values, error) {
				time.Sleep(4 * time.Second)
				return valuesOf("n", "1"), nil
			}})
		}
		observer, store := newTestObserver(t, readers...)
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(25 * time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		for _, r := range readers {
			if res, ok := store.Result(r.id); !ok || len(res.Recent) < 2 {
				t.Errorf("reader %s was starved: %d reads", r.id, len(res.Recent))
			}
		}
	})
}

func TestAppStateLongErrorsAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer, store := newTestObserver(t, fakeReader{id: "loud", interval: time.Minute, read: func(context.Context) (carried.Values, error) {
			return carried.Values{}, errors.New(string(make([]byte, 5000)))
		}})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		synctest.Wait()
		cancel()
		observer.Stop()
		r, _ := store.Result("loud")
		if len(r.Recent[0].Error) > carried.MaxValueBytes {
			t.Fatalf("error is %d bytes", len(r.Recent[0].Error))
		}
	})
}

func TestAppStateReadFinishingAfterReconfigureIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		slow := fakeReader{id: "r", interval: time.Minute, read: func(ctx context.Context) (carried.Values, error) {
			<-release
			return valuesOf("old", "1"), nil
		}}
		observer, store := newTestObserver(t, slow)
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		synctest.Wait()
		_ = observer.Reconfigure(nil)
		_ = observer.Reconfigure([]sdk.Detector{fakeReader{id: "r", interval: time.Hour, read: func(context.Context) (carried.Values, error) { return valuesOf("new", "1"), nil }}})
		close(release)
		synctest.Wait()
		if res, ok := store.Result("r"); ok && res.Recent[0].Values.Len() > 0 {
			if _, stale := res.Recent[0].Values.Get("old"); stale {
				t.Fatal("a read from the previous revision was recorded")
			}
		}
		cancel()
		observer.Stop()
	})
}
