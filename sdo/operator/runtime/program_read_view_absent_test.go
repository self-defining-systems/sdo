package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var configMapKind = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}

func cmRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: name}
}

// declaringSource accepts exactly the named ConfigMaps; everything else is
// absent from accepted source.
type declaringSource map[string]bool

func (s declaringSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind != "ConfigMap" || !s[ref.Name] {
		return nil, ErrAcceptedManifestAbsent
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace},
		"data":     map[string]any{"init.sh": "echo hi"},
	}}, nil
}
func (declaringSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, ErrAcceptedManifestAbsent
}

func absentWorld(complete ...schema.GroupVersionKind) sdktest.Snapshot {
	return sdktest.Snapshot{
		NamespaceName:    "app",
		CompleteKindList: complete,
		ConfigMapList:    []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}}},
	}
}

func absentView(base sdk.DetectionContext, client kubernetes.Interface, source declaringSource) program.ReadView {
	return LiveReadView{Carrier: inputs.New(base), Context: context.Background(), Client: client, Source: source, SourceCommit: liveViewCommit}
}

// A declared object that is gone from a complete list is a diff, never an error.
// The same must hold while an incident is open (held keys decorate the snapshot).
func TestDiffAgainstSourceDeclaredButDeletedIsAMissingDiff(t *testing.T) {
	for _, incident := range []bool{false, true} {
		name := "no incident"
		if incident {
			name = "incident open"
		}
		t.Run(name, func(t *testing.T) {
			base := absentWorld(configMapKind)
			provider := ReadSnapshotProvider{Base: fixedProvider{base}, View: func(s sdk.DetectionContext) program.ReadView {
				return absentView(s, nil, declaringSource{"cm": true})
			}}
			c, err := NewController(testControllerConfig(), nil, provider, &recordingDispatcher{}, liveViewNow)
			if err != nil {
				t.Fatal(err)
			}
			if incident {
				c.incidentOpen = true
				c.currentIncidentRequest = &IncidentRequest{OpeningKeys: []string{FindingStateKey("request-metrics", "requestmetrics/errors/op")}}
				c.publishHeldLocked()
			}
			snapshot, err := c.provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			view := snapshot.(program.ReadView)
			diff, err := view.DiffAgainstSource(cmRef("cm"))
			if err != nil {
				t.Fatalf("declared but deleted object errored: %v", err)
			}
			if !diff.Changed || !diff.LiveAbsent || diff.SourceAbsent || !diff.MissingButInSource() || diff.PresentButNotInSource() {
				t.Fatalf("diff = %+v, want changed, live absent, in source", diff)
			}
			if !strings.Contains(diff.Text, "init.sh") {
				t.Fatalf("diff text does not show the declared object: %q", diff.Text)
			}
			if _, err := view.Object(cmRef("cm")); !errors.Is(err, sdk.ErrObjectAbsent) {
				t.Fatalf("Object = %v, want ErrObjectAbsent", err)
			}
			// Neither declared nor live: absent on both sides, not changed.
			diff, err = view.DiffAgainstSource(cmRef("nowhere"))
			if err != nil || diff.Changed || !diff.SourceAbsent || !diff.LiveAbsent || diff.MissingButInSource() {
				t.Fatalf("undeclared absent diff = %+v, %v", diff, err)
			}
			// Present and declared: not absent, so MissingButInSource is false.
			present := absentWorld(configMapKind)
			present.ConfigMapList = append(present.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "app"}, Data: map[string]string{"init.sh": "echo hi"}})
			diff, err = absentView(present, nil, declaringSource{"cm": true}).DiffAgainstSource(cmRef("cm"))
			if err != nil && !errors.Is(err, ErrAcceptanceRecordUnavailable) {
				t.Fatal(err)
			}
			if diff.LiveAbsent || diff.MissingButInSource() {
				t.Fatalf("present object reported absent: %+v", diff)
			}
		})
	}
}

// A kind the snapshot did not list completely proves nothing: it stays an
// error, and is not mistaken for absence.
func TestObjectOfUncapturedKindIsNotAbsent(t *testing.T) {
	view := absentView(absentWorld(), nil, declaringSource{"cm": true})
	_, err := view.Object(cmRef("cm"))
	if !errors.Is(err, sdk.ErrObjectNotCaptured) || errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("Object = %v, want ErrObjectNotCaptured and not ErrObjectAbsent", err)
	}
	if _, err := view.DiffAgainstSource(cmRef("cm")); !errors.Is(err, sdk.ErrObjectNotCaptured) {
		t.Fatalf("DiffAgainstSource = %v, want the not-captured error", err)
	}
	// A complete list of another kind does not vouch for this one.
	other := absentView(absentWorld(schema.GroupVersionKind{Version: "v1", Kind: "Service"}), nil, declaringSource{"cm": true})
	if _, err := other.Object(cmRef("cm")); errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("a Service list vouched for a ConfigMap: %v", err)
	}
	// Nor does a complete list vouch for a different namespace.
	if _, err := absentView(absentWorld(configMapKind), nil, nil).Object(sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "x"}); errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("a list of namespace app vouched for kube-system: %v", err)
	}
}

// When the list is not complete but the API answers NotFound, the object is absent.
func TestObjectNotFoundOnGetIsAbsent(t *testing.T) {
	respond := func(code int, body any) (*http.Response, error) {
		encoded, _ := json.Marshal(body)
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(encoded))}, nil
	}
	notFound := metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound}
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "http://api.invalid", Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v1":
			return respond(http.StatusOK, metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true}}})
		case "/api/v1/namespaces/app/configmaps/boom":
			return respond(http.StatusInternalServerError, metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusInternalServerError})
		default:
			return respond(http.StatusNotFound, notFound)
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	view := absentView(absentWorld(), client, declaringSource{"cm": true})
	if _, err := view.Object(cmRef("cm")); !errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("Object = %v, want ErrObjectAbsent after NotFound", err)
	}
	if _, err := view.Object(cmRef("boom")); errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("a server error was read as absence: %v", err)
	}
	diff, err := absentView(absentWorld(), nil, declaringSource{"cm": true}).DiffAgainstSource(cmRef("cm"))
	if err == nil {
		t.Fatalf("no client and no complete list must stay an error, got %+v", diff)
	}
}

// A deleted declared object is drift: it is reported, not turned into a
// comparison error that re-queues the change.
func TestClassifyDeletedDeclaredObjectDoesNotError(t *testing.T) {
	owned := func(manager string) []metav1.ManagedFieldsEntry {
		return []metav1.ManagedFieldsEntry{{Manager: manager, Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:init.sh":{}}}`)}}}
	}
	deleted := func(name string, fields []metav1.ManagedFieldsEntry) ObjectChange {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", ManagedFields: fields}, Data: map[string]string{"init.sh": "echo hi"}}
		return objectChange(t, cm, nil, "v1", "ConfigMap")
	}
	view := absentView(absentWorld(configMapKind), nil, declaringSource{"cm": true})
	for name, change := range map[string]ObjectChange{
		"deployer owned fields": deleted("cm", owned(sdk.DeployerFieldManager)),
		"no owned fields":       deleted("cm", owned("someone-else")),
	} {
		t.Run(name, func(t *testing.T) {
			event, err := DriftClassifier{}.Classify(context.Background(), change, view, liveViewNow)
			if err != nil {
				t.Fatalf("deleted declared object errored: %v", err)
			}
			if event == nil || event.Classification != sdk.DriftUnexplained || event.Ref.Name != "cm" {
				t.Fatalf("event = %+v, want unexplained drift for cm", event)
			}
		})
	}
	// An object accepted source never declared leaves nothing to report.
	event, err := DriftClassifier{}.Classify(context.Background(), deleted("gone", owned(sdk.DeployerFieldManager)), view, liveViewNow)
	if err != nil || event != nil {
		t.Fatalf("deleted undeclared object = %+v, %v; want no event, no error", event, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
