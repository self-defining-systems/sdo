package broker

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestKubernetesValidatorRetriesTransientInspectionErrors(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{"unavailable", apierrors.NewServiceUnavailable("rpc error: code = Unavailable desc = etcd connection timed out")},
		{"internal", apierrors.NewInternalError(errors.New("etcd request timed out"))},
		{"timeout", apierrors.NewTimeoutError("API timeout", 1)},
		{"transport-timeout", os.ErrDeadlineExceeded},
		{"server-timeout", apierrors.NewServerTimeout(schema.GroupResource{Resource: "jobs"}, "get", 1)},
		{"throttled", apierrors.NewTooManyRequests("busy", 1)},
	} {
		t.Run(failure.name, func(t *testing.T) {
			finished := metav1.Now()
			client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "validator", Namespace: "sdo"}, Status: batchv1.JobStatus{Succeeded: 1, CompletionTime: &finished}})
			calls := 0
			client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls == 1 {
					return true, nil, failure.err
				}
				return false, nil, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", PollInterval: time.Millisecond}
			if err := launcher.waitJob(ctx, "validator"); err != nil || calls != 2 {
				t.Fatalf("transient error did not recover: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestKubernetesValidatorInspectionRetryRespectsDeadlineAndPermanentErrors(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		client := fake.NewSimpleClientset()
		calls := 0
		forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "jobs"}, "validator", errors.New("denied"))
		client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
			calls++
			if permanent {
				return true, nil, forbidden
			}
			return true, nil, apierrors.NewServiceUnavailable("etcd unavailable")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", PollInterval: time.Millisecond}
		err := launcher.waitJob(ctx, "validator")
		cancel()
		if permanent {
			if !apierrors.IsForbidden(err) || calls != 1 {
				t.Fatalf("permanent error was retried: calls=%d err=%v", calls, err)
			}
		} else if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("inspection retries ignored deadline: %v", err)
		}
	}
}
