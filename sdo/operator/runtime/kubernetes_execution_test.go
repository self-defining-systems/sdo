package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk/sdktest"
)

func TestKubernetesExecutionReturnsMeasuredWritesAndUndo(t *testing.T) {
	value := "before"
	version := 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			fmt.Fprint(w, `{"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}`)
			return
		case "/apis":
			fmt.Fprint(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
			return
		case "/api/v1":
			fmt.Fprint(w, `{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["get","patch"]}]}`)
			return
		case "/api/v1/namespaces/app/configmaps/settings":
			if r.Method == "PATCH" {
				var body struct {
					Metadata struct {
						ResourceVersion string `json:"resourceVersion"`
					} `json:"metadata"`
					Data map[string]string `json:"data"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.Metadata.ResourceVersion != "" && body.Metadata.ResourceVersion != strconv.Itoa(version) {
					w.WriteHeader(409)
					return
				}
				value = body.Data["value"]
				version++
			}
			fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings","namespace":"app","uid":"original","resourceVersion":%q},"data":{"value":%q}}`, strconv.Itoa(version), value)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client, ExecConfig: config}}
	calls := []sdktest.PrimitiveCall{}
	executor.RecordPrimitive = func(call sdktest.PrimitiveCall) error { calls = append(calls, call); return nil }
	session, err := executor.NewKubernetesExecution(LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}, IncidentRequest{Namespace: "app"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := session.Client.CoreV1().ConfigMaps("app").Patch(context.Background(), "settings", types.MergePatchType, []byte(`{"data":{"value":"after"}}`), metav1.PatchOptions{})
	if err != nil || reply == nil || reply.Data["value"] != "after" || reply.ResourceVersion != "2" {
		t.Fatalf("actual response=%+v err=%v", reply, err)
	}
	receipt, err := session.Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Method != "KubernetesEffect" || len(receipt.Patches) != 1 || receipt.Patches[0].Before == nil || receipt.Patches[0].After == nil {
		t.Fatalf("recorded transition missing: %+v", receipt)
	}
	if err := executor.Rollback(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if value != "before" {
		t.Fatal("before-image not restored")
	}

}
