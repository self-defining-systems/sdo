package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestScriptCaptureStorePreservesChunkedFirstPreimageAndSettledAbsence(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	maps := client.CoreV1().ConfigMaps("app")
	store := scriptCaptureStore{maps: maps, namespace: "app", application: "sdo-script-application", jobUID: "original-job"}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	image := func(value string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "data": map[string]any{"value": value}}}
	}
	original := image(strings.Repeat("界", 300000))
	capture := program.ScriptObjectCapture{Ref: ref, Before: original, After: image("fixed")}
	location, err := store.save(ctx, "/api/v1/namespaces/app/configmaps/settings", capture)
	if err != nil {
		t.Fatal(err)
	}
	capture.Before = image("later write must not replace the first image")
	capture.After = nil
	capture.AfterAbsent = true
	if _, err = store.save(ctx, "/api/v1/namespaces/app/configmaps/settings", capture); err != nil {
		t.Fatal(err)
	}
	name := strings.TrimPrefix(location, "ConfigMap/app/")
	manifest, err := maps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	count, _ := strconv.Atoi(manifest.Data["before_chunks"])
	if count < 2 || manifest.Data["after_absent"] != "true" || manifest.Data["after_chunks"] != "0" {
		t.Fatalf("image manifest: %+v", manifest.Data)
	}
	var raw []byte
	for i := 0; i < count; i++ {
		chunk, err := maps.Get(ctx, fmt.Sprintf("%s-before-%d", name, i), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, chunk.BinaryData["image"]...)
	}
	expected, _ := json.Marshal(original)
	if !bytes.Equal(raw, expected) {
		t.Fatal("first preimage or UTF-8 chunk boundaries changed")
	}
	store.jobUID = "replacement-job"
	if _, err = store.save(ctx, "/api/v1/namespaces/app/configmaps/settings", capture); err == nil {
		t.Fatal("replacement Job overwrote private images")
	}
}

func TestScriptCapturesEveryObjectBeyondInlineImageBudget(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := scriptCaptureStore{maps: client.CoreV1().ConfigMaps("app"), namespace: "app", application: "sdo-script-many", jobUID: "job"}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":%q,"namespace":"app"},"data":{"value":"settled"}}`, r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	recorder := newScriptRecorder(ctx, target, http.DefaultTransport)
	recorder.persistImage = store.save
	recorder.persistReceipt = store.receipt
	for i := 0; i < 80; i++ {
		name := fmt.Sprintf("object-%d", i)
		raw := []byte(fmt.Sprintf(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":%q,"namespace":"app"},"data":{"value":%q}}`, name, strings.Repeat("x", 16<<10)))
		recorder.Record(apihistory.Event{Phase: "end", Verb: "get", Version: "v1", Resource: "configmaps", Namespace: "app", Name: name, Status: 200, Started: time.Unix(int64(i), 0)}, raw)
	}
	result := program.ScriptExecutionResult{}
	recorder.settle(&result)
	if len(result.Objects) != 80 || len(result.CaptureGaps) != 0 {
		t.Fatalf("objects=%d gaps=%v", len(result.Objects), result.CaptureGaps)
	}
	for _, object := range result.Objects {
		if object.ImagesRef == "" || object.Before != nil || object.After != nil {
			t.Fatal("complete object images must live at the private storage edge")
		}
	}
	manifests, err := client.CoreV1().ConfigMaps("app").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/script-capture=sdo-script-many"})
	if err != nil {
		t.Fatal(err)
	}
	receipts := 0
	for _, manifest := range manifests.Items {
		if manifest.Data["receipt.json"] != "" {
			receipts++
		}
	}
	if receipts != 80 {
		t.Fatalf("private request receipts lost: %d", receipts)
	}
}

func TestScriptSecretDataNeverReadOrPersisted(t *testing.T) {
	ctx := context.Background()
	reads := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"synthetic","namespace":"app"},"data":{"password":"secret-canary"}}`)
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	recorder := newScriptRecorder(ctx, target, http.DefaultTransport)
	var persisted []byte
	recorder.persistImage = func(_ context.Context, _ string, c program.ScriptObjectCapture) (string, error) {
		raw, _ := json.Marshal(c)
		persisted = append(persisted, raw...)
		return "private-ref", nil
	}
	event := apihistory.Event{RequestID: "secret-write", Phase: "start", Verb: "patch", Version: "v1", Resource: "secrets", Namespace: "app", Name: "synthetic"}
	recorder.Record(event, nil)
	event.Phase = "end"
	event.Status = 200
	recorder.Record(event, []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"synthetic","namespace":"app"},"data":{"password":"secret-canary"}}`))
	result := program.ScriptExecutionResult{}
	recorder.settle(&result)
	public, _ := json.Marshal(result)
	if reads != 0 || len(persisted) != 0 || bytes.Contains(public, []byte("secret-canary")) {
		t.Fatalf("Secret crossed capture boundary: reads=%d images=%s result=%s", reads, persisted, public)
	}
	if len(result.Receipts) != 1 {
		t.Fatal("metadata receipt missing")
	}
}

func TestScriptCapturesEachWriteBeforeImage(t *testing.T) {
	value := "one"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings","namespace":"app","uid":"uid","resourceVersion":%q},"data":{"value":%q}}`, value, value)
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	recorder := newScriptRecorder(context.Background(), target, http.DefaultTransport)
	images := map[string][]string{}
	recorder.persistImage = func(_ context.Context, key string, c program.ScriptObjectCapture) (string, error) {
		if c.Before != nil {
			v, _, _ := unstructured.NestedString(c.Before.Object, "data", "value")
			images[key] = append(images[key], v)
		}
		return key, nil
	}
	for _, id := range []string{"write-one", "write-two"} {
		e := apihistory.Event{RequestID: id, Phase: "start", Verb: "patch", Version: "v1", Resource: "configmaps", Namespace: "app", Name: "settings"}
		recorder.Record(e, nil)
		if value == "one" {
			value = "two"
		} else {
			value = "three"
		}
		e.Phase = "end"
		e.Status = 200
		recorder.Record(e, nil)
	}
	distinct := map[string]bool{}
	for _, values := range images {
		for _, v := range values {
			distinct[v] = true
		}
	}
	if !distinct["one"] || !distinct["two"] {
		t.Fatalf("not every write has its immediate preimage: %+v", images)
	}
}

func TestSecretShapedResponsesNeverEnterReceiptsWhenImageStorageFails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"synthetic","namespace":"app"},"data":{"value":"secret-shaped-canary"}}`)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	recorder := newScriptRecorder(context.Background(), u, http.DefaultTransport)
	recorder.persistImage = func(context.Context, string, program.ScriptObjectCapture) (string, error) {
		return "", fmt.Errorf("unavailable")
	}
	event := apihistory.Event{RequestID: "bad-response", Phase: "start", Verb: "patch", Version: "v1", Resource: "configmaps", Namespace: "app", Name: "synthetic"}
	recorder.Record(event, nil)
	event.Phase = "end"
	event.Status = 200
	recorder.Record(event, []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"synthetic","namespace":"app"},"data":{"value":"secret-shaped-canary"}}`))
	result := program.ScriptExecutionResult{}
	recorder.settle(&result)
	raw, _ := json.Marshal(result)
	if bytes.Contains(raw, []byte("secret-shaped-canary")) {
		t.Fatal("Secret response escaped through fallback receipt")
	}
}
