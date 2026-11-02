package program

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
)

func TestBindingsEqualUsesCanonicalObjectIdentity(t *testing.T) {
	forms := []string{
		`"Service/default/reservation"`, `"svc/default/reservation"`,
		`"default/services/reservation"`, `"v1@Service/default/reservation"`,
		`{"kind":"Service","namespace":"default","name":"reservation"}`,
		`{"kind":"svc","namespace":"default","name":"reservation"}`,
		`{"api_version":"v1","kind":"Service","namespace":"default","name":"reservation"}`,
		`{"apiVersion":"v1","kind":"Service","namespace":"default","name":"reservation"}`,
	}
	legacy := Bindings{Objects: ObjectBindings{"service": {Kind: "Service", Namespace: "default", Name: "reservation"}}}
	for _, left := range forms {
		for _, right := range forms {
			t.Run(left+"/"+right, func(t *testing.T) {
				var a, b sdk.ObjectRef
				if err := json.Unmarshal([]byte(left), &a); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(right), &b); err != nil {
					t.Fatal(err)
				}
				one, two := Bindings{Objects: ObjectBindings{"service": a}}, Bindings{Objects: ObjectBindings{"service": b}}
				if !one.Equal(two) || !two.Equal(one) || !one.Equal(legacy) || !legacy.Equal(one) {
					t.Fatalf("same object has unequal bindings: %+v / %+v / %+v", one, two, legacy)
				}
			})
		}
	}
	for _, change := range []struct {
		name string
		ref  sdk.ObjectRef
	}{
		{"name", sdk.ObjectRef{Kind: "Service", Namespace: "default", Name: "peer"}},
		{"namespace", sdk.ObjectRef{Kind: "Service", Namespace: "peer", Name: "reservation"}},
		{"unspecified namespace", sdk.ObjectRef{Kind: "Service", Name: "reservation"}},
		{"kind", sdk.ObjectRef{Kind: "Pod", Namespace: "default", Name: "reservation"}},
		{"version", sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Service", Namespace: "default", Name: "reservation"}},
	} {
		t.Run(change.name, func(t *testing.T) {
			other := Bindings{Objects: ObjectBindings{"service": change.ref}}
			if legacy.Equal(other) || other.Equal(legacy) {
				t.Fatalf("different objects compare equal: %+v / %+v", legacy, other)
			}
		})
	}
	if legacy.Equal(Bindings{Objects: ObjectBindings{"peer": legacy.Objects["service"]}}) {
		t.Fatal("different binding names compare equal")
	}
	withValue := legacy.Clone()
	withValue.Values = map[string]any{"port": 80}
	differentValue := legacy.Clone()
	differentValue.Values = map[string]any{"port": 81}
	if withValue.Equal(differentValue) || withValue.Equal(legacy) {
		t.Fatal("different scalar values compare equal")
	}
}

func FuzzBindingsCanonicalObjectIdentity(f *testing.F) {
	for _, seed := range []string{`svc/default/reservation`, `Service/reservation`, `deploy/app/api`, `ClusterRole/reader`, `example.dev/v2@Widget/shop/worker`, `{"kind":"Service","name":"api"}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		ref, err := sdk.ParseObjectRef(raw)
		if err != nil {
			return
		}
		text, err := sdk.FormatObjectRef(ref)
		if err != nil {
			t.Fatal(err)
		}
		var encoded sdk.ObjectRef
		data, err := json.Marshal(text)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &encoded); err != nil {
			t.Fatal(err)
		}
		left, right := Bindings{Objects: ObjectBindings{"object": ref}}, Bindings{Objects: ObjectBindings{"object": encoded}}
		if !left.Equal(right) || !right.Equal(left) {
			t.Fatalf("canonical identity changed: %+v / %+v", left, right)
		}
		encoded.Name += "-different"
		different := Bindings{Objects: ObjectBindings{"object": encoded}}
		if left.Equal(different) || different.Equal(left) {
			t.Fatal("different objects compare equal")
		}
	})
}
