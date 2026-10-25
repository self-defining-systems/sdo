package runtime

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sync/atomic"
	"testing"
)

func TestRawDeleteRequiresBoundTargetAndConfirmedAbsence(t *testing.T) {
	for _, mode := range []string{"deleted", "pending", "unreadable", "mismatch", "inferred", "collection", "subresource"} {
		t.Run(mode, func(t *testing.T) {
			var called atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called.Store(true)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"kind":"Status","status":"Success"}`)
			}))
			defer server.Close()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "target"}
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: "uid", ResourceVersion: "1"}})
			client.Resources = []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: []string{"get", "delete"}}}}}
			client.PrependReactor("get", "configmaps", func(a ktesting.Action) (bool, kruntime.Object, error) {
				if !called.Load() || mode == "pending" {
					return false, nil, nil
				}
				if mode == "unreadable" {
					return true, nil, fmt.Errorf("read unavailable")
				}
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, ref.Name)
			})
			action := program.KubernetesRequest{Ref: ref, Method: "DELETE", Path: "/api/v1/namespaces/app/configmaps/target"}
			if mode == "mismatch" {
				action.Ref.Name = "untouched"
			}
			if mode == "inferred" {
				action.Ref = sdk.ObjectRef{}
			}
			if mode == "collection" {
				action.Path = "/api/v1/namespaces/app/configmaps"
			}
			if mode == "subresource" {
				action.Path += "/status"
			}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client, ExecConfig: &rest.Config{Host: server.URL}}, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{action}}, false)
			success := mode == "deleted" || mode == "inferred"
			if success && err != nil {
				t.Fatal(err)
			}
			if !success && err == nil {
				t.Fatalf("unconfirmed/unsafe raw delete succeeded: %+v", result)
			}
			if (mode == "mismatch" || mode == "collection" || mode == "subresource") && called.Load() {
				t.Fatal("invalid target crossed write boundary")
			}
			errorText := ""
			if err != nil {
				errorText = err.Error()
			}
			receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "raw", Result: result, Error: errorText}}, nil)
			if success {
				if len(receipts) != 1 || !receipts[0].Success || len(receipts[0].Resources) != 1 || receipts[0].Resources[0] != ref {
					t.Fatalf("wrong receipt target: %+v", receipts)
				}
			}
			if mode == "pending" || mode == "unreadable" {
				if len(receipts) != 1 || receipts[0].Success || receipts[0].FailureReason != "delete_not_confirmed" {
					t.Fatalf("unconfirmed delete receipt: %+v", receipts)
				}
			}
		})
	}
}
