package install

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestStopWithoutControllerIsIdempotentAndPreservesMemory(t *testing.T) {
	client := fake.NewSimpleClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "responder", Namespace: "demo-sdo"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker"}}}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "reflection-proxy", Namespace: "demo-sdo", Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent"}, {Name: "proxy"}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "accepted", Namespace: "demo-sdo"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "demo-sdo"}, Data: map[string]string{"state": "accepted"}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "demo-sdo"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "other", Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker"}}},
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := Stop(ctx, client, "demo-sdo")
	if err != nil || len(result.Removed) != 2 || len(result.Leftovers) != 0 {
		t.Fatalf("stop: %+v %v", result, err)
	}
	again, err := Stop(ctx, client, "demo-sdo")
	if err != nil || len(again.Removed) != 0 {
		t.Fatalf("repeat: %+v %v", again, err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("demo-sdo").Get(ctx, "accepted", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, "sdo-controller-state", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchV1().Jobs("demo-sdo").Get(ctx, "unrelated", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("other").Get(ctx, "unrelated", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestStopQuiescesControllerBeforeWorkersAndListsStuckPods(t *testing.T) {
	client := fake.NewSimpleClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: ControllerJobName, Namespace: "demo"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "demo", Labels: map[string]string{"app.kubernetes.io/name": "sdo-controller"}}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "demo"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker"}}}}},
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := Stop(ctx, client, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Removed) != 3 {
		t.Fatalf("removed: %v", result)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "jobs" {
			deletion := action.(ktesting.DeleteAction)
			if deletion.GetName() == "worker" {
				break
			}
			if deletion.GetName() != ControllerJobName {
				t.Fatalf("first Job deletion was %s", deletion.GetName())
			}
		}
	}
	stuck := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "demo", Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker"}}})
	stuck.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	short, cancelShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShort()
	remaining, err := Stop(short, stuck, "demo")
	if err == nil || !slices.Contains(remaining.Leftovers, "pod/demo/stuck") {
		t.Fatalf("stuck deletion reported success: %+v %v", remaining, err)
	}
}

// Seeded lifecycle cuts cover interrupted installs, dispatch, reflection and drain.
// The fake API exercises the production stop inventory and deletion path.
func TestStopSeededLifecycleCuts(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for cut := 0; cut < 5; cut++ {
			t.Run(fmt.Sprintf("seed-%d/cut-%d", seed, cut), func(t *testing.T) {
				random := rand.New(rand.NewSource(seed))
				client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "accepted", Namespace: "demo"}})
				jobs := []string{"sdo-controller", "sdo-worker", "sdo-worker", "sdo-worker", "sdo-worker"}
				for phase := 0; phase <= cut; phase++ {
					name := fmt.Sprintf("phase-%d-%d", phase, random.Intn(10000))
					labels := map[string]string{"app.kubernetes.io/name": jobs[phase]}
					job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}}
					if _, err := client.BatchV1().Jobs("demo").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "demo", Labels: labels}}
					if _, err := client.CoreV1().Pods("demo").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
					// A controller/Job disappearing before abort must not hide orphan Pods.
					if random.Intn(2) == 0 {
						_ = client.BatchV1().Jobs("demo").Delete(context.Background(), name, metav1.DeleteOptions{})
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result, err := Stop(ctx, client, "demo")
				if err != nil || len(result.Leftovers) != 0 {
					t.Fatalf("cut %d: %+v %v", cut, result, err)
				}
				jobsLeft, _ := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
				podsLeft, _ := client.CoreV1().Pods("demo").List(ctx, metav1.ListOptions{})
				if len(jobsLeft.Items)+len(podsLeft.Items) != 0 {
					t.Fatal("runtime survived abort")
				}
				if _, err := client.CoreV1().PersistentVolumeClaims("demo").Get(ctx, "accepted", metav1.GetOptions{}); err != nil {
					t.Fatal("accepted repository was removed")
				}
			})
		}
	}
}
