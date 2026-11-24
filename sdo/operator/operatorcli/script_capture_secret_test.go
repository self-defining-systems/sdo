package operatorcli

import (
	"context"
	"encoding/json"
	"io"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"net/url"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestScriptCaptureDoesNotPersistSecretImages(t *testing.T) {
	ctx := context.Background()
	const path = "/api/v1/namespaces/app/secrets/credentials"
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	image := func(canary string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "annotations": map[string]any{"kubectl.kubernetes.io/last-applied-configuration": canary}},
			"data":     map[string]any{"key": canary}, "stringData": map[string]any{"key": canary},
		}}
	}
	before, after := image("synthetic-old-canary"), image("synthetic-new-canary")
	recorder := newScriptRecorder(ctx, nil, nil)
	recorder.retain(path, before, after)
	recorded, _ := json.Marshal(recorder.objects[path])
	maps := fake.NewSimpleClientset().CoreV1().ConfigMaps("control")
	store := scriptCaptureStore{maps: maps, namespace: "control", application: "test", jobUID: "job"}
	if _, err := store.save(ctx, path, program.ScriptObjectCapture{Ref: ref, Before: before, After: after}); err != nil {
		t.Fatal(err)
	}
	stored, err := maps.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Items) != 3 {
		t.Fatalf("want a manifest and two images, got %d ConfigMaps", len(stored.Items))
	}
	for _, data := range append([][]byte{recorded}, storedImages(stored.Items)...) {
		if strings.Contains(string(data), "-canary") || strings.Contains(string(data), "last-applied") {
			t.Fatal("script capture retained Secret material")
		}
	}
}

func storedImages(items []corev1.ConfigMap) (images [][]byte) {
	for _, item := range items {
		images = append(images, item.BinaryData["image"])
	}
	return images
}

type secretCaptureTransport func(*http.Request) (*http.Response, error)

func (f secretCaptureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestScriptCaptureSecretCollectionOmitsKind(t *testing.T) {
	const canary = "synthetic-collection-canary"
	const identity = `"metadata":{"namespace":"app","name":"credentials"},"data":{"key":"` + canary + `"}`
	upstream, _ := url.Parse("https://synthetic.invalid")
	transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"apiVersion":"v1","kind":"SecretList","items":[{` + identity + `}]}`
		if strings.HasSuffix(r.URL.Path, "/credentials") {
			body = `{"apiVersion":"v1","kind":"Secret",` + identity + `}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	ctx := context.Background()
	for _, sink := range []string{"checkpoint", "settle", "ConfigMap"} {
		t.Run(sink, func(t *testing.T) {
			recorder := newScriptRecorder(ctx, upstream, transport)
			maps := fake.NewSimpleClientset().CoreV1().ConfigMaps("control")
			if sink == "ConfigMap" {
				recorder.persistImage = (scriptCaptureStore{maps: maps, namespace: "control", application: "test", jobUID: "job"}).save
			}
			var checkpoint []program.ScriptObjectCapture
			recorder.checkpoint = func(objects []program.ScriptObjectCapture, _ []program.ScriptAPIReceipt) error {
				checkpoint = objects
				return nil
			}
			recorder.Record(apihistory.Event{Phase: "start", Verb: "deletecollection", Version: "v1", Namespace: "app", Resource: "secrets"}, nil)
			recorder.Record(apihistory.Event{Phase: "end", Verb: "list", Version: "v1", Namespace: "app", Resource: "secrets", Status: 200},
				[]byte(`{"apiVersion":"v1","kind":"SecretList","items":[{`+identity+`}]}`))
			var result program.ScriptExecutionResult
			recorder.settle(&result)
			if sink == "ConfigMap" {
				stored, err := maps.List(ctx, metav1.ListOptions{})
				if err != nil || len(stored.Items) != 3 {
					t.Fatalf("expected stored preimage and postimage: %v", err)
				}
				for _, data := range storedImages(stored.Items) {
					if strings.Contains(string(data), canary) {
						t.Fatal("ConfigMap retained Secret data")
					}
				}
				return
			}
			objects := checkpoint
			if sink == "settle" {
				objects = result.Objects
			}
			data, _ := json.Marshal(objects)
			if len(objects) != 1 || objects[0].Ref.Kind != "Secret" || strings.Contains(string(data), canary) {
				t.Fatal("Secret projection missing or retained credential material")
			}
		})
	}
}
