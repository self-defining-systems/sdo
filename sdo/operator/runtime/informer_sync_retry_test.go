package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
)

func fastSyncPolicy() InformerSyncPolicy {
	return InformerSyncPolicy{Attempts: 3, Timeout: 100 * time.Millisecond, Backoff: time.Millisecond, Budget: 5 * time.Second}
}

func TestInformerSyncPolicyRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int32
	err := fastSyncPolicy().Do(context.Background(), 0, func(context.Context) error {
		if calls.Add(1) < 3 {
			return &InformerSyncError{Kinds: []string{"v1/Pod"}, Err: context.DeadlineExceeded}
		}
		return nil
	}, nil)
	if err != nil || calls.Load() != 3 {
		t.Fatalf("want success on the third attempt, got calls=%d err=%v", calls.Load(), err)
	}
}

func TestInformerSyncPolicyReturnsTypedErrorAfterBoundedAttempts(t *testing.T) {
	var calls atomic.Int32
	var retried []int
	err := fastSyncPolicy().Do(context.Background(), 0, func(context.Context) error {
		calls.Add(1)
		return &InformerSyncError{Kinds: []string{"v1/Pod"}, Err: context.DeadlineExceeded}
	}, func(attempt int, _ error) { retried = append(retried, attempt) })
	var syncErr *InformerSyncError
	if !errors.As(err, &syncErr) || len(syncErr.Kinds) != 1 || syncErr.Kinds[0] != "v1/Pod" {
		t.Fatalf("want the typed error naming the kind, got %v", err)
	}
	if calls.Load() != 3 || len(retried) != 2 {
		t.Fatalf("want 3 attempts and 2 retries, got calls=%d retries=%v", calls.Load(), retried)
	}
}

func TestInformerSyncPolicyStopsOnCancellationAndBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int32
	_ = fastSyncPolicy().Do(ctx, 0, func(context.Context) error { calls.Add(1); return errors.New("x") }, nil)
	if calls.Load() > 1 {
		t.Fatalf("a cancelled run must not retry, got %d attempts", calls.Load())
	}
	budget := InformerSyncPolicy{Attempts: 100, Timeout: time.Hour, Backoff: time.Millisecond, Budget: 150 * time.Millisecond}
	started := time.Now()
	err := budget.Do(context.Background(), 0, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, nil)
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("the total budget must bound retries, took %s err=%v", time.Since(started), err)
	}
}

func TestInformerSyncPolicyRejectsInvalidConfiguration(t *testing.T) {
	for _, policy := range []InformerSyncPolicy{
		{Attempts: -1}, {Timeout: -1}, {Backoff: -1}, {Budget: -1},
	} {
		if err := policy.Validate(); err == nil {
			t.Fatalf("%+v must be rejected", policy)
		}
	}
	if err := (InformerSyncPolicy{}).Validate(); err != nil {
		t.Fatalf("zero value means defaults: %v", err)
	}
	if policy := (InformerSyncPolicy{}).withDefaults(); policy.Attempts != 3 || policy.Budget > 3*time.Minute {
		t.Fatalf("defaults must stay within three attempts and three minutes: %+v", policy)
	}
}

// A persistent sync failure ends the process, so the Job's pod (and the lane
// tooling reading it) must see the named kinds without scraping stderr: the
// container termination message and a stdout record carry the typed error.
func TestRunReportsPersistentInformerSyncFailureInTerminationMessage(t *testing.T) {
	client := fake.NewSimpleClientset(hotelObjects()...)
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, errors.New("pods is forbidden")
	})
	termination := filepath.Join(t.TempDir(), "termination-log")
	lines := &lockedLines{}
	reader, writer := io.Pipe()
	go collectLines(reader, lines)
	defer writer.Close()
	err := RunWithOptions(context.Background(), []sdk.Detector{}, RuntimeOptions{
		Args: []string{"--namespace", hotel, "--control-namespace", hotel + "-sdo", "--app-root", gitRepository(t),
			"--application", "hotel", "--synthetic-traffic=false", "--lease-duration", "3s"},
		Client: client, Stdout: writer, Stderr: io.Discard, Dispatcher: capturingDispatcher{requests: make(chan IncidentRequest, 1)},
		InformerSync: fastSyncPolicy(), TerminationLogPath: termination,
	})
	var syncErr *InformerSyncError
	if !errors.As(err, &syncErr) {
		t.Fatalf("want InformerSyncError, got %v", err)
	}
	content, readErr := os.ReadFile(termination)
	if readErr != nil {
		t.Fatalf("termination message not written: %v", readErr)
	}
	var record struct {
		Error string   `json:"controller_error"`
		Kinds []string `json:"kinds"`
	}
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatalf("termination message is not one JSON record: %q: %v", content, err)
	}
	if record.Error != "informer_sync" || len(record.Kinds) == 0 || record.Kinds[0] != "v1/Pod" {
		t.Fatalf("termination message must name the error class and kinds: %+v", record)
	}
	eventually(t, "the stdout record", func() bool { return lines.has(`{"controller_error":"informer_sync"`) })
}

// One slow sync must not end the process: the start retries with a fresh
// informer generation and continues once the kind can be listed.
func TestRunRetriesTransientInformerSyncFailure(t *testing.T) {
	var healed atomic.Bool
	client := fake.NewSimpleClientset(hotelObjects()...)
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
		if healed.Load() {
			return false, nil, nil
		}
		return true, nil, errors.New("apiserver unavailable")
	})
	lines := &lockedLines{}
	reader, writer := io.Pipe()
	go collectLines(reader, lines)
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() {
		exited <- RunWithOptions(ctx, []sdk.Detector{}, RuntimeOptions{
			Args: []string{"--namespace", hotel, "--control-namespace", hotel + "-sdo", "--app-root", gitRepository(t),
				"--application", "hotel", "--synthetic-traffic=false", "--lease-duration", "3s"},
			Client: client, Stdout: writer, Stderr: io.Discard, Dispatcher: capturingDispatcher{requests: make(chan IncidentRequest, 1)},
			InformerSync: fastSyncPolicy(), TerminationLogPath: filepath.Join(t.TempDir(), "termination-log"),
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("controller did not stop after cancellation")
		}
		_ = writer.Close()
	})
	eventually(t, "the first sync to be retried", func() bool { return lines.has(`{"attempt":1,"error":`) })
	healed.Store(true)
	eventually(t, "the controller to start after the retry", func() bool { return lines.has(`{"controller_maintenance"`) })
	select {
	case err := <-exited:
		t.Fatalf("a transient sync failure ended the process: %v", err)
	default:
	}
}
