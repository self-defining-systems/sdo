package sdktest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type genericRepair struct {
	ref  sdk.ObjectRef
	path string
}

func (p genericRepair) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "generic", Params: []program.ParamSpec{{Name: "repair", ValueKind: program.StringValue, Required: true}}}
}
func (p genericRepair) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	object, err := v.Object(p.ref)
	if err != nil {
		return program.Verdict{}, err
	}
	value, err := program.FieldValue(object.Object, p.path)
	if err != nil {
		return program.Verdict{}, err
	}
	result := program.Match
	if value == "fixed" {
		result = program.NoMatch
	}
	return program.Verdict{Result: result}, nil
}
func (p genericRepair) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: p.ref, Path: "/" + strings.ReplaceAll(p.path, ".", "/"), Value: program.FromParam("repair")}}}, nil
}

func TestGenericCaptureAndMitigationReplay(t *testing.T) {
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "app", Name: "claim"},
		{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "policy-system", Name: "policy"},
		{APIVersion: "example.test/v7", Kind: "UnlistedResource", Namespace: "other", Name: "cr"},
	} {
		t.Run(ref.Kind+ref.Namespace, func(t *testing.T) {
			object := unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"mode": "broken"}}}
			original := Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}}
			captured, err := CaptureSnapshot(original, "app")
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(captured)
			var fixture Snapshot
			if err = json.Unmarshal(encoded, &fixture); err != nil {
				t.Fatal(err)
			}
			got, err := fixture.Object(ref)
			if err != nil || got.Object["spec"].(map[string]any)["mode"] != "broken" {
				t.Fatalf("lost object: %v %v", got, err)
			}
			p := genericRepair{ref: ref, path: "spec.mode"}
			result, err := CheckMitigation(p, p, CorpusCase{Open: fixture, Bindings: program.Bindings{Values: map[string]any{"repair": "fixed"}}})
			if err != nil || result.Status != MitigationReplayPassed {
				t.Fatalf("replay: %+v %v", result, err)
			}
			got.Object["spec"].(map[string]any)["mode"] = "changed"
			again, _ := fixture.Object(ref)
			if again.Object["spec"].(map[string]any)["mode"] != "broken" {
				t.Fatal("reader aliased fixture")
			}
		})
	}
}
func TestGenericProjectionPreservesUnknownFieldsAndExcludesSecretValues(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "UnlistedResource", "Deployment"} {
		raw := `{"apiVersion":"example.test/v1","kind":"` + kind + `","metadata":{"name":"x","annotations":{"kubectl.kubernetes.io/last-applied-configuration":"CANARY"}},"novel":{"preserved":true},"spec":{"containers":[{"env":[{"name":"API_TOKEN","value":"CANARY","valueFrom":{"secretKeyRef":{"name":"CANARY"}}}]}]}}`
		projected, err := ProjectCapturedObject([]byte(raw))
		if err != nil || strings.Contains(string(projected), "CANARY") || !strings.Contains(string(projected), `"preserved":true`) {
			t.Fatalf("projection: %s %v", projected, err)
		}
	}
	raw := `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"x"},"data":{"token":"CANARY"},"stringData":{"key":"CANARY"},"status":{"token":"CANARY"}}`
	projected, err := ProjectCapturedObject([]byte(raw))
	if err != nil || strings.Contains(string(projected), "CANARY") {
		t.Fatalf("secret projection: %s %v", projected, err)
	}
}

func TestCaptureConfigMapStructuredFlagRetainsNonsecretState(t *testing.T) {
	raw := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"flags","namespace":"app"},"data":{"flags.json":"{\"flags\":{\"queue\":{\"defaultVariant\":\"on\",\"variants\":{\"on\":true,\"off\":false}}},\"apiToken\":\"CANARY\"}","password":"CANARY"}}`)
	projected, err := ProjectCapturedObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(projected), "CANARY") || !strings.Contains(string(projected), `defaultVariant`) {
		t.Fatalf("flag capture: %s", projected)
	}
}

func TestGenericCaptureRenameKeepsTypedAndObjectViewsConsistent(t *testing.T) {
	c := renameFixture(t)
	var err error
	for _, snapshot := range []*Snapshot{&c.Open, &c.Fixed, &c.Healthy} {
		*snapshot, err = CaptureSnapshot(*snapshot, snapshot.NamespaceName)
		if err != nil {
			t.Fatal(err)
		}
	}
	renamed, err := renameCorpusCase(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range renamed.Open.Services() {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
		object, err := sdk.FindObject(renamed.Open.Objects, ref)
		if err != nil {
			t.Fatal(err)
		}
		value, err := program.FieldValue(object.Object, "spec.ports[0].targetPort")
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(value)
		want, _ := json.Marshal(service.Spec.Ports[0].TargetPort)
		if string(got) != string(want) {
			t.Fatalf("generic/typed rename disagrees: %s %s", got, want)
		}
	}
}

func TestGenericSnapshotPreservesAllocatedFieldsForRestore(t *testing.T) {
	for _, tc := range []struct{ raw, path string }{
		{`{"apiVersion":"v1","kind":"Service","metadata":{"name":"x","namespace":"app"},"spec":{"clusterIP":"10.0.0.1"}}`, "spec.clusterIP"},
		{`{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"x","namespace":"app"},"spec":{"volumeName":"allocated-pv"}}`, "spec.volumeName"},
		{`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"x","namespace":"app"},"spec":{"nodeName":"allocated-node","containers":[{"name":"app","image":"synthetic:app"}]}}`, "spec.nodeName"},
		{`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x","namespace":"app"},"spec":{"selector":{"matchLabels":{"batch.kubernetes.io/controller-uid":"allocated-controller"}}}}`, "spec.selector.matchLabels[batch.kubernetes.io/controller-uid]"},
	} {
		var object unstructured.Unstructured
		if err := json.Unmarshal([]byte(tc.raw), &object); err != nil {
			t.Fatal(err)
		}
		captured, err := CaptureSnapshot(Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}}, "app")
		if err != nil {
			t.Fatal(err)
		}
		got, err := captured.Object(sdk.ObjectIdentity(object))
		if err != nil {
			t.Fatal(err)
		}
		before, err := program.FieldValue(object.Object, tc.path)
		if err != nil {
			t.Fatal(err)
		}
		after, err := program.FieldValue(got.Object, tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Fatalf("allocated %s changed: %v != %v", tc.path, before, after)
		}
	}
}
