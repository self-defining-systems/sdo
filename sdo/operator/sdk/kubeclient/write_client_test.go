package kubeclient_test

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk/kubeclient"
)

func TestExecutionClientPreservesUpstreamWritesAndApplyManager(t *testing.T) {
	calls := 0
	c, err := kubeclient.NewExecutionClient(func(context.Context, string, string, url.Values) ([]byte, error) { return []byte(`{}`), nil }, func(_ context.Context, method, path string, q url.Values, content string, body []byte) ([]byte, error) {
		calls++
		if method != "PATCH" || path != "/apis/apps/v1/namespaces/app/deployments/api" {
			t.Fatalf("request %s %s", method, path)
		}
		if content == string(types.ApplyPatchType) && q.Get("fieldManager") != "sdo" {
			t.Fatalf("apply manager %v", q)
		}
		if !json.Valid(body) {
			t.Fatal("patch bytes lost")
		}
		return []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app","resourceVersion":"9"},"spec":{"replicas":3}}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []types.PatchType{types.JSONPatchType, types.MergePatchType, types.StrategicMergePatchType, types.ApplyPatchType} {
		result, err := c.AppsV1().Deployments("app").Patch(context.Background(), "api", kind, []byte(`{}`), metav1.PatchOptions{})
		if err != nil || result.ResourceVersion != "9" || *result.Spec.Replicas != 3 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if calls != 4 {
		t.Fatal(calls)
	}

}
