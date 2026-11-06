package runtime

import (
	"context"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

// A previous terminated container whose log is gone means "no previous log".
// The API server reports it as 404 or as 400 "previous terminated container
// ... not found"; neither may fail the evaluation, while real read failures do.
func TestPreviousLogMissingIsNoPreviousLog(t *testing.T) {
	missing := map[string]error{
		"not found":   apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "synthetic-0"),
		"bad request": apierrors.NewBadRequest(`previous terminated container "wait" in pod "synthetic-0" not found`),
	}
	for name, readErr := range missing {
		for _, terminations := range []bool{false, true} {
			o := previousLogObserver(terminations, func(previous bool) error {
				if previous {
					return readErr
				}
				return nil
			})
			if err := requireQuery(o); err != nil {
				t.Fatalf("%s (terminations=%v): %v", name, terminations, err)
			}
		}
	}
	t.Run("other errors stay failures", func(t *testing.T) {
		o := previousLogObserver(false, func(previous bool) error {
			if previous {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "synthetic-0", io.ErrUnexpectedEOF)
			}
			return nil
		})
		if err := requireQuery(o); err == nil {
			t.Fatal("a forbidden previous read must still fail the evaluation")
		}
	})
}

func requireQuery(o *LogsObserver) error {
	base := previousLogPod()
	ctx := inputs.New(base).WithLogs(logResults(o.collect(context.Background(), base)))
	_, err := logs.Require(ctx, "q")
	return err
}

func previousLogObserver(terminations bool, fail func(previous bool) error) *LogsObserver {
	q := logs.Query{ID: "q", Previous: true, Terminations: terminations, Patterns: []logs.Pattern{{ID: "p", RE2: "boom"}}}
	o := &LogsObserver{queries: []logs.Query{q}, patterns: map[string]*regexp.Regexp{}, previous: true, terminations: terminations,
		clock:  func() time.Time { return time.Unix(1000, 0) },
		states: map[string]*logState{}}
	o.read = func(_ context.Context, _, _ string, opt *corev1.PodLogOptions) (io.ReadCloser, error) {
		if err := fail(opt.Previous); err != nil {
			return nil, err
		}
		return io.NopCloser(strings.NewReader("")), nil
	}
	return o
}

func previousLogPod() sdktest.Snapshot {
	finished := metav1.NewTime(time.Unix(900, 0))
	status := corev1.ContainerStatus{Name: "wait", RestartCount: 1, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: finished}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "synthetic-0", Namespace: "app", UID: "u1"},
		Spec:   corev1.PodSpec{InitContainers: []corev1.Container{{Name: "wait"}}, Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{status}}}
	return sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}
}
