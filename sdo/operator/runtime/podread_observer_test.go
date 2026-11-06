package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	utilexec "k8s.io/client-go/util/exec"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
)

const syntheticStoreImage = "example.invalid/synthetic-store:1"

func storePod(name string, containerState corev1.ContainerState) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("uid-" + name), Labels: map[string]string{"tier": "store"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "example.invalid/proxy:1"}, {Name: "store", Image: syntheticStoreImage}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "store", State: containerState}}},
	}
}

func runningStorePod(name string) corev1.Pod {
	return storePod(name, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}})
}

// scriptedPodExec answers each exec from a function and counts the commands.
type scriptedPodExec struct {
	calls  atomic.Int32
	answer func(ctx context.Context, pod sdk.ObjectRef, container string, stdout io.Writer) error
}

func (e *scriptedPodExec) Stream(ctx context.Context, pod sdk.ObjectRef, container string, _ []string, _ string, stdout io.Writer) error {
	e.calls.Add(1)
	return e.answer(ctx, pod, container, stdout)
}

func answering(text string) *scriptedPodExec {
	return &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
		_, err := io.WriteString(stdout, text)
		return err
	}}
}

// storeReadDetector fires on every store whose answer is not "good".
type storeReadDetector struct{}

const storeReadID = "store-policy"

func (storeReadDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "store-policy-detector", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}
func (storeReadDetector) PodReads() []podread.Query {
	return []podread.Query{{ID: storeReadID, Target: podread.Target{Image: "synthetic-store"}, Args: []string{"store-cli", "get", "policy"}, Interval: time.Minute}}
}
func (storeReadDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	result, err := podread.Require(view, storeReadID)
	if err != nil {
		return nil, err
	}
	var findings []sdk.Finding
	for _, reading := range result.Readings {
		if strings.TrimSpace(reading.Stdout) != "good" {
			findings = append(findings, sdk.Finding{RuleID: "store-policy-wrong", PrimaryResource: reading.Ref(), Summary: reading.Stdout})
		}
	}
	return findings, nil
}

func podReadObserver(t *testing.T, exec *scriptedPodExec, clock func() time.Time) *PodReadObserver {
	t.Helper()
	observer, err := NewPodReadObserver(fake.NewSimpleClientset(), exec, "", []sdk.Detector{storeReadDetector{}}, clock)
	if err != nil {
		t.Fatal(err)
	}
	observer.get = func(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetUID(types.UID("uid-" + ref.Name))
		return object, nil
	}
	return observer
}

func storeSnapshot(pods ...corev1.Pod) sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "app", PodList: pods}
}

func TestPodReadsRunOnTheirIntervalAndFreezeBetweenRuns(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	exec := answering("first")
	observer := podReadObserver(t, exec, func() time.Time { return now })
	snapshot := storeSnapshot(runningStorePod("a"), runningStorePod("b"))
	result := observer.collect(context.Background(), snapshot)[storeReadID]
	if result.Error != "" || len(result.Readings) != 2 || exec.calls.Load() != 2 {
		t.Fatalf("first cycle = %+v, %d execs", result, exec.calls.Load())
	}
	got := result.Readings[0]
	if got.Pod != "a" || got.Container != "store" || got.Stdout != "first" || got.Error != "" || !got.ObservedAt.Equal(now) {
		t.Fatalf("reading = %+v, want the image-matching container", got)
	}

	exec.answer = answering("second").answer
	now = now.Add(time.Minute - time.Second)
	retained := observer.collect(context.Background(), snapshot)[storeReadID]
	if exec.calls.Load() != 2 || retained.Readings[0].Stdout != "first" {
		t.Fatalf("within the interval: %d execs, %+v", exec.calls.Load(), retained)
	}
	now = now.Add(time.Second)
	if fresh := observer.collect(context.Background(), snapshot)[storeReadID]; fresh.Readings[0].Stdout != "second" || !fresh.Readings[0].ObservedAt.Equal(now) {
		t.Fatalf("after the interval: %+v", fresh)
	}
}

func TestPodReadsDropPodsThatLeftTheRole(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	exec := answering("value")
	observer := podReadObserver(t, exec, func() time.Time { return now })
	observer.collect(context.Background(), storeSnapshot(runningStorePod("a"), runningStorePod("b")))
	result := observer.collect(context.Background(), storeSnapshot(runningStorePod("b")))[storeReadID]
	if len(result.Readings) != 1 || result.Readings[0].Pod != "b" {
		t.Fatalf("readings = %+v", result.Readings)
	}
	if exec.calls.Load() != 2 {
		t.Fatalf("a retained Pod was read again within its interval: %d execs", exec.calls.Load())
	}
	// The whole role replaced: nothing retained applies, so read at once.
	result = observer.collect(context.Background(), storeSnapshot(runningStorePod("c")))[storeReadID]
	if len(result.Readings) != 1 || result.Readings[0].Pod != "c" || exec.calls.Load() != 3 {
		t.Fatalf("readings = %+v after %d execs", result.Readings, exec.calls.Load())
	}
}

func TestPodReadFailuresAreExplicitNeverAClear(t *testing.T) {
	refused := func(err error) *scriptedPodExec {
		return &scriptedPodExec{answer: func(context.Context, sdk.ObjectRef, string, io.Writer) error { return err }}
	}
	for name, test := range map[string]struct {
		exec *scriptedPodExec
		pod  corev1.Pod
		want string
	}{
		"transport failure":     {refused(errors.New("stream reset")), runningStorePod("a"), "exec transport failed"},
		"no exec credential":    {refused(kubereads.ErrExecUnavailable), runningStorePod("a"), "exec transport unavailable"},
		"executable missing":    {refused(utilexec.CodeExitError{Err: errors.New("exit"), Code: 127}), runningStorePod("a"), "could not start"},
		"container not running": {answering("good"), storePod("a", corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}), "container not running"},
	} {
		t.Run(name, func(t *testing.T) {
			observer := podReadObserver(t, test.exec, func() time.Time { return time.Unix(1000, 0) })
			view := storeSnapshot(test.pod)
			view.PodReads = observer.collect(context.Background(), view)
			reading := view.PodReads[storeReadID].Readings[0]
			if !strings.Contains(reading.Error, test.want) || reading.Stdout != "" {
				t.Fatalf("reading = %+v, want error %q", reading, test.want)
			}
			if findings, err := (storeReadDetector{}).Detect(context.Background(), view); err == nil || !strings.Contains(err.Error(), test.want) || findings != nil {
				t.Fatalf("Detect = %v, %v; want the failure as a detector error", findings, err)
			}
		})
	}
	t.Run("no Pod holds the role", func(t *testing.T) {
		exec := answering("good")
		observer := podReadObserver(t, exec, func() time.Time { return time.Unix(1000, 0) })
		other := runningStorePod("a")
		other.Spec.Containers = []corev1.Container{{Name: "web", Image: "example.invalid/web:1"}}
		result := observer.collect(context.Background(), storeSnapshot(other))[storeReadID]
		if !strings.Contains(result.Error, "no running Pod") || exec.calls.Load() != 0 {
			t.Fatalf("result = %+v after %d execs", result, exec.calls.Load())
		}
	})
	t.Run("too many Pods hold the role", func(t *testing.T) {
		exec := answering("good")
		observer := podReadObserver(t, exec, func() time.Time { return time.Unix(1000, 0) })
		var pods []corev1.Pod
		for i := range podread.MaxTargets + 1 {
			pods = append(pods, runningStorePod(fmt.Sprintf("s-%02d", i)))
		}
		result := observer.collect(context.Background(), storeSnapshot(pods...))[storeReadID]
		if !strings.Contains(result.Error, "narrow the target") || exec.calls.Load() != 0 {
			t.Fatalf("result = %+v after %d execs", result, exec.calls.Load())
		}
	})
	t.Run("Pod replaced under the same name", func(t *testing.T) {
		exec := answering("good")
		observer := podReadObserver(t, exec, func() time.Time { return time.Unix(1000, 0) })
		observer.get = func(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error) {
			object := &unstructured.Unstructured{}
			object.SetUID("replacement")
			return object, nil
		}
		result := observer.collect(context.Background(), storeSnapshot(runningStorePod("a")))[storeReadID]
		if !strings.Contains(result.Readings[0].Error, "identity changed") || exec.calls.Load() != 0 {
			t.Fatalf("result = %+v after %d execs", result, exec.calls.Load())
		}
	})
}

func TestPodReadKeepsANonzeroExitAsTheCommandsAnswer(t *testing.T) {
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
		_, _ = io.WriteString(stdout, "denied")
		return utilexec.CodeExitError{Err: errors.New("exit"), Code: 3}
	}}
	observer := podReadObserver(t, exec, func() time.Time { return time.Unix(1000, 0) })
	reading := observer.collect(context.Background(), storeSnapshot(runningStorePod("a")))[storeReadID].Readings[0]
	if reading.Error != "" || reading.ExitCode != 3 || reading.Stdout != "denied" {
		t.Fatalf("reading = %+v", reading)
	}
}

func TestPodReadOutputIsBoundedAndCrossesTheWire(t *testing.T) {
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
		for range 4 {
			if n, err := stdout.Write([]byte(strings.Repeat("\xff", podread.LimitBytes))); err != nil || n != podread.LimitBytes {
				return fmt.Errorf("the output must drain: %d, %v", n, err)
			}
		}
		return nil
	}}
	observer := podReadObserver(t, exec, func() time.Time { return time.Unix(1000, 0) })
	reading := observer.collect(context.Background(), storeSnapshot(runningStorePod("a")))[storeReadID].Readings[0]
	if !reading.Truncated || reading.Error != "" {
		t.Fatalf("reading = %+v", reading)
	}
	if _, err := wireStruct(reading); err != nil {
		t.Fatalf("binary output must not break the observation: %v", err)
	}
	if _, err := podread.Require(sdktest.Snapshot{PodReads: map[string]podread.Result{storeReadID: {Readings: []podread.Reading{reading}}}}, storeReadID); err == nil {
		t.Fatal("a truncated answer cannot establish a clear")
	}
}

// twoReadDetector declares a read of every store and one of a second role.
type twoReadDetector struct{ storeReadDetector }

func (twoReadDetector) PodReads() []podread.Query {
	return append(storeReadDetector{}.PodReads(), podread.Query{ID: "zz-other", Target: podread.Target{Image: "proxy"}, Args: []string{"proxy-cli"}, Interval: time.Minute})
}

func TestPodReadsAreBoundedInConcurrencyAndTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		running, peak := 0, 0
		// The store hangs, as a frozen process does; only the timeout ends it.
		exec := &scriptedPodExec{answer: func(ctx context.Context, _ sdk.ObjectRef, container string, stdout io.Writer) error {
			if container != "store" {
				_, err := io.WriteString(stdout, "other answer")
				return err
			}
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()
			<-ctx.Done()
			mu.Lock()
			running--
			mu.Unlock()
			return ctx.Err()
		}}
		observer := podReadObserver(t, exec, time.Now)
		observer.queries = twoReadDetector{}.PodReads()
		var pods []corev1.Pod
		for i := range podread.MaxTargets {
			pod := runningStorePod(fmt.Sprintf("s-%02d", i))
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
			pods = append(pods, pod)
		}
		start := time.Now()
		results := observer.collect(context.Background(), storeSnapshot(pods...))
		if elapsed := time.Since(start); elapsed > podread.CollectionBudget {
			t.Fatalf("collection stalled the cycle for %s", elapsed)
		}
		if peak != podread.MaxConcurrent {
			t.Fatalf("peak concurrency = %d", peak)
		}
		if got := int(exec.calls.Load()); got != 2*podread.MaxConcurrent {
			t.Fatalf("%d commands started; want two rounds of the concurrency limit", got)
		}
		for _, reading := range results[storeReadID].Readings {
			if reading.Error == "" {
				t.Fatalf("a hung command reported an answer: %+v", reading)
			}
		}
		if reading := results["zz-other"].Readings[0]; reading.Error != podReadNotStarted {
			t.Fatalf("unstarted reading = %+v", reading)
		}
		// The hung query is not due again; the one it crowded out runs at once.
		results = observer.collect(context.Background(), storeSnapshot(pods...))
		if reading := results["zz-other"].Readings[0]; reading.Error != "" || reading.Stdout != "other answer" {
			t.Fatalf("a hung store starved another query: %+v", reading)
		}
		if got := int(exec.calls.Load()); got != 2*podread.MaxConcurrent+podread.MaxTargets {
			t.Fatalf("%d commands started", got)
		}
	})
}

// A detector compiled into a memory host reads through the same chain the
// controller runs: declaration over the host boundary, collection by the
// controller's provider, the frozen result in the host's view, and capture.
func TestPodReadReachesAMemoryHostDetectorAndReplaysFromItsCapture(t *testing.T) {
	ctx := context.Background()
	server, err := memoryhost.NewServer("synthetic-commit", "synthetic-digest", []sdk.Detector{storeReadDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	rpc, stop := memoryTestRPC(t, server)
	host := &simActivationHost{testMemoryRPC: rpc, stop: stop}
	manager := memoryhost.NewManager(5 * time.Second)
	pod := runningStorePod("a")
	if err := manager.Publish(ctx, storeSnapshot(pod)); err != nil {
		t.Fatal(err)
	}
	if err := manager.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := manager.Activate(ctx, host, "synthetic-commit", "synthetic-digest"); err != nil {
		t.Fatal(err)
	}
	ready, err := host.Ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := NewRemoteMemory(ready, manager)
	if err != nil {
		t.Fatal(err)
	}
	detector := remote.Detectors[0]
	if got := podread.LongestInterval(detector); got != time.Minute {
		t.Fatalf("the declaration did not cross the host boundary: interval %s", got)
	}
	now := time.Unix(1000, 0).UTC()
	stored := "bad"
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
		_, err := io.WriteString(stdout, stored)
		return err
	}}
	client := fake.NewSimpleClientset(&pod)
	provider, _, err := detectorObservations{Client: client, Clock: func() time.Time { return now }, Exec: exec}.compose(staticProvider{snapshot: storeSnapshot(pod)}, []sdk.Detector{detector}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(ctx, view)
	if err != nil || len(findings) != 1 || findings[0].RuleID != "store-policy-wrong" || findings[0].PrimaryResource.Name != "a" {
		t.Fatalf("Detect on bad state = %+v, %v", findings, err)
	}

	captured, err := sdktest.CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var replay sdktest.Snapshot
	if err := json.Unmarshal(raw, &replay); err != nil {
		t.Fatal(err)
	}
	replayed, err := storeReadDetector{}.Detect(ctx, replay)
	if err != nil || len(replayed) != 1 || replayed[0].Summary != "bad" {
		t.Fatalf("replay of the capture = %+v, %v", replayed, err)
	}

	// A repair is visible once the read refreshes; the closure wait must cover it.
	if got := detectorWindow(detector, view); got != time.Minute {
		t.Fatalf("detector window = %s, want the read interval", got)
	}
	stored, now = "good", now.Add(time.Minute)
	view, err = provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if findings, err = detector.Detect(ctx, view); err != nil || len(findings) != 0 {
		t.Fatalf("Detect after repair = %+v, %v", findings, err)
	}

	// Failure crosses the host as a detector error, not a clear.
	exec.answer = func(context.Context, sdk.ObjectRef, string, io.Writer) error { return errors.New("stream reset") }
	now = now.Add(time.Minute)
	view, err = provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if findings, err = detector.Detect(ctx, view); err == nil || findings != nil {
		t.Fatalf("Detect on a failed read = %+v, %v", findings, err)
	}
}

func TestDetectorsWithoutPodReadsNeverExec(t *testing.T) {
	exec := answering("good")
	provider, _, err := detectorObservations{Client: fake.NewSimpleClientset(), Exec: exec}.compose(staticProvider{snapshot: storeSnapshot(runningStorePod("a"))}, []sdk.Detector{specOnly{sdk.DetectorSpec{ID: "plain"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := podread.ResultFrom(view, storeReadID); ok || exec.calls.Load() != 0 {
		t.Fatalf("idle observer ran: %d execs", exec.calls.Load())
	}
}

func TestPodReadDeclarationsAreRefusedAtComposition(t *testing.T) {
	_, _, err := detectorObservations{Client: fake.NewSimpleClientset()}.compose(staticProvider{}, []sdk.Detector{podReadOnlyDetector{specOnly{sdk.DetectorSpec{ID: "unwatched"}}}}, nil)
	if err == nil || !strings.Contains(err.Error(), "v1 Pod watch") {
		t.Fatalf("compose = %v", err)
	}
}
