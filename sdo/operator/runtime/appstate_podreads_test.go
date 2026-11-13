package runtime

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/podread"
)

// podReader is an app-state reader that declares one pod read and returns what
// the controller collected for it.
type podReader struct{ id string }

func (r podReader) queryID() string { return "appstate-" + r.id }
func (r podReader) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: r.id, Class: sdk.DetectorClassHealth, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}
func (podReader) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (podReader) AppStateReader() bool    { return true }
func (podReader) AppStateNoisy() []string { return nil }
func (r podReader) PodReads() []podread.Query {
	return []podread.Query{{ID: r.queryID(), Target: podread.Target{Image: "synthetic-store"}, Args: []string{"store-cli", "get", r.id}, Interval: time.Minute}}
}
func (r podReader) AppStateRead(_ context.Context, _ sdk.DetectionContext, reads map[string]podread.Result) (carried.Values, error) {
	result, ok := reads[r.queryID()]
	if !ok {
		return carried.Values{}, fmt.Errorf("pod read %q unavailable", r.queryID())
	}
	if result.Error != "" || len(result.Readings) == 0 {
		return carried.Values{}, fmt.Errorf("pod read %q: %s", r.queryID(), result.Error)
	}
	for _, reading := range result.Readings {
		if reading.Error != "" {
			return carried.Values{}, fmt.Errorf("pod read %q Pod %s: %s", r.queryID(), reading.Pod, reading.Error)
		}
	}
	return valuesOf("v", strings.TrimSpace(result.Readings[0].Stdout)), nil
}

func podReaders(n int) []sdk.Detector {
	out := make([]sdk.Detector, n)
	for i := range out {
		out[i] = podReader{id: fmt.Sprintf("reader-%02d", i)}
	}
	return out
}

// designerReads declares podread.MaxQueries pod reads of the detection cycle.
type designerReads struct{}

func (designerReads) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "designer", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}
func (designerReads) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (designerReads) PodReads() []podread.Query {
	var qs []podread.Query
	for i := range podread.MaxQueries {
		qs = append(qs, podread.Query{ID: fmt.Sprintf("designer-%02d", i), Target: podread.Target{Image: "synthetic-store"}, Args: []string{"store-cli", "get", "x"}, Interval: time.Minute})
	}
	return qs
}

type stubSnapshots struct{ snapshot sdk.DetectionContext }

func (s stubSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return s.snapshot, nil
}

// Thirteen readers each declare a pod read while the designer fills the cycle's
// whole budget: every reader must still get its read within two intervals.
func TestEveryAppStateReaderIsReadWhateverTheDesignerDeclares(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
			time.Sleep(300 * time.Millisecond)
			_, err := io.WriteString(stdout, "value")
			return err
		}}
		pod := runningStorePod("store-0")
		observer := NewAppStateObserver(NewAppStateStore(DefaultStateSettle), lifecycleio.Clock(), lifecycleio.Clock().Now)
		observations := detectorObservations{Client: fake.NewSimpleClientset(&pod), Exec: exec, Clock: lifecycleio.Clock().Now, AppState: observer}
		detectors := append(podReaders(13), designerReads{})
		provider, _, err := observations.compose(stubSnapshots{snapshot: storeSnapshot(pod)}, detectors, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		if _, err := provider.Snapshot(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		cancel()
		observer.Stop()
		for i := range 13 {
			id := fmt.Sprintf("reader-%02d", i)
			result, _ := observer.Store().Result(id)
			latest, has := result.Latest()
			if !has || latest.Error != "" {
				t.Errorf("%s: latest=%+v has=%v, want a value", id, latest, has)
			}
		}
	})
}

func runReaderOnce(t *testing.T, observer *AppStateObserver) carried.Snapshot {
	t.Helper()
	var latest carried.Snapshot
	synctest.Test(t, func(t *testing.T) {
		pod := runningStorePod("store-0")
		if err := observer.Reconfigure(podReaders(1)); err != nil {
			t.Fatal(err)
		}
		observer.UseSnapshots(stubSnapshots{snapshot: storeSnapshot(pod)})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		cancel()
		observer.Stop()
		result, _ := observer.Store().Result("reader-00")
		latest, _ = result.Latest()
	})
	return latest
}

// A reader whose pod reads cannot run says why, never a bare "unavailable".
func TestAppStateReaderNamesWhyItCouldNotRun(t *testing.T) {
	t.Run("no Kubernetes client", func(t *testing.T) {
		observer := NewAppStateObserver(NewAppStateStore(0), lifecycleio.Clock(), lifecycleio.Clock().Now)
		if got := runReaderOnce(t, observer); !strings.Contains(got.Error, "skipped: no Kubernetes client") {
			t.Fatalf("latest = %+v", got)
		}
	})
	t.Run("exec gives no answer", func(t *testing.T) {
		pod := runningStorePod("store-0")
		hang := &scriptedPodExec{answer: func(ctx context.Context, _ sdk.ObjectRef, _ string, _ io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		observer := NewAppStateObserver(NewAppStateStore(0), lifecycleio.Clock(), lifecycleio.Clock().Now)
		observer.UsePodReads(fake.NewSimpleClientset(&pod), hang, "")
		if got := runReaderOnce(t, observer); !strings.Contains(got.Error, "command gave no answer within") {
			t.Fatalf("latest = %+v", got)
		}
	})
}

type flakySnapshots struct {
	stubSnapshots
	failures int
	calls    int
}

func (f *flakySnapshots) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	f.calls++
	if f.calls <= f.failures {
		return nil, fmt.Errorf("cache not synced")
	}
	return f.stubSnapshots.Snapshot(ctx)
}

// A reader whose first reads fail on a startup race is retried within seconds
// and reaches its first value, instead of waiting a whole interval per try.
func TestAppStateReaderRetriesAStartupRaceUntilItHasAResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pod := runningStorePod("store-0")
		observer := NewAppStateObserver(NewAppStateStore(0), lifecycleio.Clock(), lifecycleio.Clock().Now)
		observer.UsePodReads(fake.NewSimpleClientset(&pod), answering("value"), "")
		observer.UseSnapshots(&flakySnapshots{stubSnapshots: stubSnapshots{snapshot: storeSnapshot(pod)}, failures: 2})
		if err := observer.Reconfigure(podReaders(1)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(50 * time.Second) // well under the reader's interval
		synctest.Wait()
		cancel()
		observer.Stop()
		result, _ := observer.Store().Result("reader-00")
		latest, ok := result.Latest()
		if !ok || latest.Error != "" {
			t.Fatalf("latest = %+v, want a value after the retries", latest)
		}
	})
}

type captureHandler struct {
	mu    *sync.Mutex
	lines *[]string
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	line := r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.Key + "=" + a.Value.String(); return true })
	*h.lines = append(*h.lines, line)
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// A reader that records nothing for AppStateFirstResultBound is itself logged,
// with the reason.
func TestAppStateReaderStallIsLogged(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	previous := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &mu, lines: &lines}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		observer, _ := newTestObserver(t, fakeReader{id: "stuck", interval: time.Minute, read: func(context.Context) (carried.Values, error) {
			<-release // ignores its deadline: a read that never returns
			return carried.Values{}, nil
		}})
		ctx, cancel := context.WithCancel(context.Background())
		observer.Start(ctx)
		time.Sleep(AppStateFirstResultBound + 5*time.Second)
		synctest.Wait()
		close(release)
		cancel()
		observer.Stop()
	})
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, line := range lines {
		if strings.Contains(line, "app_state_reader_stalled") && strings.Contains(line, "reader=stuck") && strings.Contains(line, "reason=") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no stall line in %q", lines)
	}
}
