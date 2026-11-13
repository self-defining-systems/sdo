package runtime

import (
	"context"
	"fmt"
	"io"
	"strings"
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
func (r podReader) AppStateRead(_ context.Context, reads map[string]podread.Result) (carried.Values, error) {
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
		observer.observe(storeSnapshot(pod))
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
