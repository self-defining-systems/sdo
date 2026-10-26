package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A Pod deleted with a grace period stays readable, with a deletion timestamp,
// until its containers stop. The API server has then accepted the deletion; the
// stale Pod must not turn into delete_not_confirmed. Other kinds that remain
// after DELETE (no timestamp) stay unconfirmed.
func TestGracefullyTerminatingPodDeleteIsConfirmed(t *testing.T) {
	for _, terminating := range []bool{true, false} {
		t.Run(fmt.Sprintf("terminating=%v", terminating), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"kind":"Status","status":"Success"}`)
			}))
			defer server.Close()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "stale"}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: "uid", ResourceVersion: "1"}}
			if terminating {
				now := metav1.NewTime(time.Now())
				pod.DeletionTimestamp = &now
			}
			client := fake.NewSimpleClientset(pod)
			client.Resources = []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: []string{"get", "delete"}}}}}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client, ExecConfig: &rest.Config{Host: server.URL}}, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
			action := program.KubernetesRequest{Ref: ref, Method: "DELETE", Path: "/api/v1/namespaces/app/pods/stale"}
			_, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{action}}, false)
			if terminating && err != nil {
				t.Fatalf("terminating Pod delete was not confirmed: %v", err)
			}
			if !terminating && err == nil {
				t.Fatal("a Pod that is not terminating was reported deleted")
			}
		})
	}
}

// terminatingPodClient keeps the Pod readable, with its UID and a deletion
// timestamp, after an accepted delete: a graceful termination.
type terminatingPodClient struct{ *runtimeActionClient }

func (c terminatingPodClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
	c.planFakeClient.objects[ref].SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	return nil
}

func TestDeletePodAcceptsGracefulTermination(t *testing.T) {
	e, c, ref := runtimeActionFixture()
	e.Client = terminatingPodClient{c}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeletePod{Ref: ref}}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("terminating Pod was not accepted as deleted: %+v %v", result, err)
	}
}

func TestKubectlDeleteReceiptAcceptsTerminatingPod(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "stale"}
	pod := planObject(ref, map[string]any{})
	pod.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	loop := &ResponderLoop{Executor: PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: pod}}}}
	receipts := loop.checkDeletionReceipts(context.Background(), []RepairActionReceipt{{Kind: "kubectl-delete", Success: true, Resources: []sdk.ObjectRef{ref}}})
	if !receipts[0].Success || receipts[0].FailureReason != "" {
		t.Fatalf("terminating Pod delete receipt: %+v", receipts[0])
	}
}
