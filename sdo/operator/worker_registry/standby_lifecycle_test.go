package worker_registry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func failJob(t *testing.T, e KubernetesExecutor, name string) {
	t.Helper()
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func publishedStandby(t *testing.T) (*StandbyPool, KubernetesExecutor, *WarmBinding) {
	t.Helper()
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal("bind", err)
	}
	if err := bound.publishWarmTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	return pool, e, slot
}

// A slot is given up through one function, so every exit path leaves neither a
// Job nor a directory behind.
func TestStandbySlotIsReleasedOnEveryExitPath(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T) (KubernetesExecutor, *WarmBinding)
	}{
		{"adoption fails after the claim", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, task := standbyFixture(t)
			slot := readyStandby(t, pool)
			pool.exchange = func(string, string) error { return errors.New("injected") }
			if bound, err := pool.TryBind(context.Background(), e, task); err != nil || bound.WarmBinding != nil {
				t.Fatal("failed adoption did not fall back cold", err)
			}
			return e, slot
		}},
		{"durable binding cannot be written", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, task := standbyFixture(t)
			slot := readyStandby(t, pool)
			bindings := filepath.Join(e.RepositoryMountPath, standbyRoot, "bindings")
			if err := os.MkdirAll(bindings, 0500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(bindings, 0700) })
			if bound, err := pool.TryBind(context.Background(), e, task); err != nil || bound.WarmBinding != nil {
				t.Fatal("unwritable binding did not fall back cold", err)
			}
			return e, slot
		}},
		{"unpublished claim whose Job died", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, task := standbyFixture(t)
			slot := readyStandby(t, pool)
			bound, err := pool.TryBind(context.Background(), e, task)
			if err != nil || bound.WarmBinding == nil {
				t.Fatal("bind", err)
			}
			failJob(t, e, slot.JobName)
			cold := jobExists(e, JobName(task.TaskId))
			released := func() bool {
				_, statErr := os.Stat(slot.Directory)
				_, getErr := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{})
				return cold() && os.IsNotExist(statErr) && apierrors.IsNotFound(getErr)
			}
			_, _ = executeUntil(t, released, func(ctx context.Context) (*contracts.AgentResult, error) { return bound.Execute(ctx, task) })
			return e, slot
		}},
		{"published slot whose Job is gone", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, slot := publishedStandby(t)
			if err := e.Client.BatchV1().Jobs(e.Namespace).Delete(context.Background(), slot.JobName, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := pool.reapGone(context.Background()); err != nil {
				t.Fatal(err)
			}
			return e, slot
		}},
		{"idle slot whose Job vanished", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, _ := standbyFixture(t)
			slot := readyStandby(t, pool)
			if err := e.Client.BatchV1().Jobs(e.Namespace).Delete(context.Background(), slot.JobName, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := pool.Ensure(context.Background()); err != nil {
				t.Fatal(err)
			}
			return e, slot
		}},
		{"controller shutdown", func(t *testing.T) (KubernetesExecutor, *WarmBinding) {
			pool, e, _ := standbyFixture(t)
			slot := readyStandby(t, pool)
			pool.retireIdle()
			return e, slot
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, slot := c.run(t)
			if _, err := os.Stat(slot.Directory); !os.IsNotExist(err) {
				t.Errorf("slot directory leaked: %v", err)
			}
			if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("standby Job leaked: %v", err)
			}
		})
	}
}

// A request that cannot be decoded is rejected before the claim, so the slot
// stays available for the next incident.
func TestStandbyUndecodableRequestDoesNotConsumeTheSlot(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	task.Request, _ = anypb.New(&contracts.ReflectionRequest{})
	if _, err := pool.TryBind(context.Background(), e, task); err == nil {
		t.Fatal("undecodable request was accepted")
	}
	if pool.current != slot {
		t.Fatal("slot was consumed")
	}
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", standbyClaimFile)); !os.IsNotExist(err) {
		t.Fatal("claim file left on an unbound slot")
	}
}

func TestStandbyPublishedSlotIsKeptWhileItsJobExists(t *testing.T) {
	pool, e, slot := publishedStandby(t)
	if err := pool.reapGone(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); err != nil {
		t.Fatal("running responder's inbox reaped", err)
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestStandbyProbeFailureDisablesWarmWithoutPayingTheCopy(t *testing.T) {
	pool, e, task := standbyFixture(t)
	pool.exchange = func(string, string) error { return errors.New("RENAME_EXCHANGE unsupported") }
	if err := pool.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "RENAME_EXCHANGE") {
		t.Fatal("probe failure did not disable warm", err)
	}
	jobs, err := e.Client.BatchV1().Jobs(e.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatal("a standby was created on a filesystem that cannot adopt", err)
	}
	if bound, err := pool.TryBind(context.Background(), e, task); err != nil || bound.WarmBinding != nil {
		t.Fatal("disabled pool bound a task", err)
	}
}

func TestStandbyProbeSucceedsOnASupportedFilesystem(t *testing.T) {
	pool, _, _ := standbyFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	pool.Wait()
}

func TestStandbyStaleClaimIsNeverPublished(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal("bind", err)
	}
	bound.WarmBinding.ClaimedAt = time.Now().Add(-standbyPublishDeadline - time.Second)
	if err := writeStandbyJSON(e.warmBindingPath(task), bound.WarmBinding); err != nil {
		t.Fatal(err)
	}
	_, _ = executeUntil(t, jobExists(e, JobName(task.TaskId)), func(ctx context.Context) (*contracts.AgentResult, error) { return bound.Execute(ctx, task) })
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); !os.IsNotExist(err) {
		t.Fatal("published to a pod about to expire")
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), JobName(task.TaskId), metav1.GetOptions{}); err != nil {
		t.Fatal("cold Job missing", err)
	}
}

func TestStandbyNotReadyWithinTheBoundIsReplaced(t *testing.T) {
	pool, e, _ := standbyFixture(t)
	if err := pool.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	stuck := pool.current
	if err := pool.Ensure(context.Background()); err != nil || pool.current != stuck {
		t.Fatal("a young pending standby was replaced", err)
	}
	stuck.CreatedAt = time.Now().Add(-standbyReadyDeadline - time.Second)
	if err := pool.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.current == stuck || pool.current == nil {
		t.Fatal("stuck standby was not replaced")
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), stuck.JobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("stuck standby Job leaked", err)
	}
	if _, err := os.Stat(stuck.Directory); !os.IsNotExist(err) {
		t.Fatal("stuck standby directory leaked")
	}
}

func TestStandbyReadyOldStandbyIsKept(t *testing.T) {
	pool, _, _ := standbyFixture(t)
	slot := readyStandby(t, pool)
	slot.CreatedAt = time.Now().Add(-10 * standbyReadyDeadline)
	if err := pool.Ensure(context.Background()); err != nil || pool.current != slot {
		t.Fatal("a ready idle standby was replaced", err)
	}
}

func TestLateFindingsProjectionErrorNeverAbortsExecute(t *testing.T) {
	_, e, task := standbyFixture(t)
	e.LateFindingsConfigMap = "late-findings-missing"
	_, err := executeUntil(t, jobExists(e, JobName(task.TaskId)), func(ctx context.Context) (*contracts.AgentResult, error) { return e.Execute(ctx, task) })
	if err != nil && strings.Contains(err.Error(), "late findings") {
		t.Fatalf("projection error aborted Execute: %v", err)
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), JobName(task.TaskId), metav1.GetOptions{}); err != nil {
		t.Fatal("worker was not launched", err)
	}
}
