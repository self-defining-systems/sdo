package worker_registry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

func TestRWS2BindingPreservesHeldMutationFence(t *testing.T) {
	for _, held := range []string{"observation", "lease"} {
		t.Run(held, func(t *testing.T) {
			pool, e, task := standbyFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			readyStandby(t, pool)
			gate, err := apihistory.PrepareMutationGate(filepath.Join(e.RepositoryMountPath, "session/api-history"))
			if err != nil {
				t.Fatal(err)
			}
			var release func()
			if held == "observation" {
				observation, err := gate.TryObserve()
				if err != nil || observation == nil {
					t.Fatalf("observe: %v", err)
				}
				release = func() {
					if err := observation.Close(); err != nil {
						t.Error(err)
					}
				}
			} else {
				lease, err := gate.Begin()
				if err != nil {
					t.Fatal(err)
				}
				release = func() {
					if err := lease.Finish(false); err != nil {
						t.Error(err)
					}
				}
			}
			bound, err := pool.TryBind(ctx, e, task)
			release()
			if err != nil {
				t.Fatal(err)
			}
			if bound.WarmBinding != nil {
				t.Fatal("warm binding replaced mutation storage while its observation/lease was held")
			}
			bound, err = pool.TryBind(ctx, e, task)
			if err != nil || bound.WarmBinding == nil {
				t.Fatalf("binding after release failed: %v", err)
			}
			observation, err := gate.TryObserve()
			if err != nil || observation == nil {
				t.Fatalf("observe after bind: %v", err)
			}
			defer observation.Close()
			lock, err := os.Open(filepath.Join(gate.Directory, "mutation.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			err = syscall.Flock(int(lock.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
			if !errors.Is(err, syscall.EWOULDBLOCK) {
				t.Fatalf("bound mutation bypassed exclusive observation: %v", err)
			}
			if err := observation.Fence(time.Now()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRWS2RejoinKeepsActualStartupBoundaries(t *testing.T) {
	pool, e, task := standbyFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readyStandby(t, pool)
	bound, err := pool.TryBind(ctx, e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatalf("bind: %v", err)
	}
	claimed := bound.WarmBinding.ClaimedAt
	taskBound := claimed.Add(time.Millisecond)
	ready := taskBound.Add(time.Millisecond)
	firstRequest := ready.Add(time.Millisecond)
	// Simulate a crash after immutable publication and measured worker/session
	// startup, before the controller's deferred summary was written.
	if err := bound.publishWarmTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	session := filepath.Join(e.RepositoryMountPath, "session")
	if err := os.MkdirAll(filepath.Join(session, "startup"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeStartupOnce(filepath.Join(session, "startup/startup-dispatch.json"), DispatchStartup{Mode: "warm", ClaimedAt: &claimed, TaskBoundAt: taskBound}); err != nil {
		t.Fatal(err)
	}
	if err := RecordSessionStartup(e.RepositoryMountPath, "", "session", ready, firstRequest); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(session, "startup-workers", bound.WarmBinding.JobName), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeStartupOnce(filepath.Join(session, "startup-workers", bound.WarmBinding.JobName, "startup-worker.json"), WorkerStartup{WorkerReady: &ready, AgentProcessStarted: &ready, CodexProcessStarted: &ready, FirstModelRequestSent: &firstRequest}); err != nil {
		t.Fatal(err)
	}
	// The phase callback models leadership loss while rejoining the running Job.
	stopped := errors.New("leadership lost")
	e.OnPhase = func(context.Context, string) error { return stopped }
	if _, err := e.Execute(ctx, task); !errors.Is(err, stopped) {
		t.Fatalf("rejoin: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(session, "startup/attempts", bound.WarmBinding.JobName, "startup-controller.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record ResponderStartup
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.TaskBoundAt == nil || !record.TaskBoundAt.Equal(taskBound) || record.SessionOpenedAt == nil || !record.SessionOpenedAt.Equal(firstRequest) || record.FirstModelRequestSent == nil || !record.FirstModelRequestSent.Equal(firstRequest) || record.WorkerReady == nil || record.AgentProcessStarted == nil || record.CodexProcessStarted == nil {
		t.Fatalf("rejoin erased actual startup boundaries: %s", data)
	}
	if record.ClaimToFirstModelMilliseconds == nil || *record.ClaimToFirstModelMilliseconds != 3 {
		t.Fatalf("wrong derived duration: %s", data)
	}
}

func TestRWS2ContinuationRejoinKeepsItsOwnClock(t *testing.T) {
	_, e, task := standbyFixture(t)
	initial := time.Now().UTC().Add(-time.Hour)
	session := filepath.Join(e.RepositoryMountPath, "session")
	if err := os.MkdirAll(filepath.Join(session, "startup"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeStartupOnce(filepath.Join(session, "startup/startup-dispatch.json"), DispatchStartup{Mode: "warm", TaskBoundAt: initial, ClaimedAt: &initial}); err != nil {
		t.Fatal(err)
	}
	task.TaskId += "-continue-1"
	stopped := errors.New("leadership lost")
	e.OnPhase = func(context.Context, string) error { return stopped }
	if _, err := e.Execute(context.Background(), task); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	jobName, err := e.taskJobName(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(session, "startup/attempts", jobName, "startup-controller.json")
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	var first ResponderStartup
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	if first.TaskBoundAt == nil || !first.TaskBoundAt.After(initial) || first.Mode != "cold" || first.ClaimedAt != nil {
		t.Fatalf("continuation inherited initial clock: %s", data)
	}
	// No summary existed at the crash boundary; make the rejoin export anew.
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), task); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	var second ResponderStartup
	if err := json.Unmarshal(data, &second); err != nil {
		t.Fatal(err)
	}
	if second.TaskBoundAt == nil || !second.TaskBoundAt.Equal(*first.TaskBoundAt) {
		t.Fatalf("rejoin reset continuation clock: %s", data)
	}
}

func TestRWS2StartupDurationsRequireRetainedBoundaries(t *testing.T) {
	_, e, task := standbyFixture(t)
	now := time.Now().UTC()
	claimed := now.Add(-time.Second)
	request := claimed.Add(time.Millisecond)
	e.WarmBinding = &WarmBinding{ClaimedAt: claimed}
	session := filepath.Join(e.RepositoryMountPath, "session")
	if err := os.MkdirAll(filepath.Join(session, "startup"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeStartupOnce(filepath.Join(session, "startup-worker.json"), WorkerStartup{FirstModelRequestSent: &request}); err != nil {
		t.Fatal(err)
	}
	record := e.startupRecord(task, "job")
	record.TaskBoundAt = &now
	e.emitStartup(record)
	if record.FirstModelRequestSent != nil || record.ClaimToFirstModelMilliseconds != nil {
		t.Fatalf("derived duration survived discarded request: %+v", record)
	}
}
