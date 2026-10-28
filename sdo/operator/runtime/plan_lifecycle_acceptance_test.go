package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The real typed/discovery clients must publish the same conditional,
// metadata-only acceptance write as adoption and feed the real diff consumer.
func TestCreatedAcceptanceUsesAdoptionWriterThroughAPI(t *testing.T) {
	for _, generic := range []bool{false, true} {
		name := "typed"
		if generic {
			name = "generic"
		}
		t.Run(name, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "restored"}
			source := planObject(ref, nil)
			delete(source.Object, "spec")
			source.Object["data"] = map[string]any{"key": "synthetic"}
			var mu sync.Mutex
			var object *unstructured.Unstructured
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if serveTestDiscovery(w, r) {
					return
				}
				switch r.Method {
				case http.MethodPost:
					object = &unstructured.Unstructured{}
					if err := json.NewDecoder(r.Body).Decode(&object.Object); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					object.SetUID("new-instance")
					object.SetResourceVersion("1")
					// Admission defaults belong to the instance baseline.
					annotations := object.GetAnnotations()
					if annotations == nil {
						annotations = map[string]string{}
					}
					annotations["example.test/default"] = "yes"
					object.SetAnnotations(annotations)
				case http.MethodPatch:
					applied := &unstructured.Unstructured{}
					if err := json.NewDecoder(r.Body).Decode(&applied.Object); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					if object == nil || r.URL.Query().Get("fieldManager") != acceptanceFieldManager || r.URL.Query().Get("force") != "true" || applied.GetUID() != object.GetUID() || applied.GetResourceVersion() != object.GetResourceVersion() || applied.Object["data"] != nil || applied.Object["spec"] != nil {
						t.Error("acceptance did not use adoption's conditional metadata writer")
						w.WriteHeader(409)
						return
					}
					annotations := object.GetAnnotations()
					annotations[acceptedPathsAnnotation] = applied.GetAnnotations()[acceptedPathsAnnotation]
					object.SetAnnotations(annotations)
					object.SetResourceVersion("2")
					writes++
				case http.MethodGet:
				default:
					t.Errorf("unexpected %s", r.Method)
					w.WriteHeader(400)
					return
				}
				if object == nil {
					w.WriteHeader(404)
					_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
					return
				}
				_ = json.NewEncoder(w).Encode(object.Object)
			}))
			defer server.Close()
			config := &rest.Config{Host: server.URL}
			kube, err := kubernetes.NewForConfig(config)
			if err != nil {
				t.Fatal(err)
			}
			var client PlanObjectClient = TypedPlanClient{Client: kube}
			if generic {
				objects, err := dynamic.NewForConfig(config)
				if err != nil {
					t.Fatal(err)
				}
				client = GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: kube}, Objects: &core.GenericCapture{Client: objects, Discovery: &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: []string{"get", "list", "patch", "create", "delete"}}}}}}}, Namespace: "app"}}
			}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: source}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("create: %+v %v", result, err)
			}
			live, err := client.Get(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := &sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}}
			view := LiveReadView{DetectionContext: snapshot, Source: e.Source, SourceCommit: "accepted"}
			diff, err := view.DiffAgainstSource(ref)
			if err != nil || diff.Changed {
				t.Fatalf("defaulted create not comparable: %+v %v", diff, err)
			}
			if writes != 1 {
				t.Fatalf("acceptance writes=%d", writes)
			}
			annotations := snapshot.Objects[0].GetAnnotations()
			annotations["example.test/late"] = "fault"
			snapshot.Objects[0].SetAnnotations(annotations)
			diff, err = view.DiffAgainstSource(ref)
			if err != nil || !diff.Changed {
				t.Fatalf("baseline absorbed later drift: %+v %v", diff, err)
			}
		})
	}
}
