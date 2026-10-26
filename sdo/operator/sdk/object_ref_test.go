package sdk

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestObjectRefTextBoundary(t *testing.T) {
	for _, raw := range []string{"Deployment/api", "Deployment/shop/api", "deploy/shop/api", "shop/deployments.apps/api"} {
		got, err := ParseObjectRef(raw)
		if err != nil || got.Kind != "Deployment" || got.Name != "api" || got.APIVersion != "apps/v1" {
			t.Fatalf("ParseObjectRef(%q) = %+v, %v", raw, got, err)
		}
		canonical, err := FormatObjectRef(got)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseObjectRef(canonical)
		if err != nil || again != got {
			t.Fatalf("round trip %q: %+v / %+v: %v", raw, got, again, err)
		}
	}
	for _, raw := range []string{"Service/a; Service/b", "Service/a,Service/b", "Service/a Service/b", "Service//a", "Service/ns/", "/Service/a", "Service/a/b/c", "Service/a\x00"} {
		_, err := ParseObjectRef(raw)
		var rejection *ObjectRefError
		if !errors.As(err, &rejection) {
			t.Errorf("%q lacks typed rejection: %v", raw, err)
		}
	}
}

func TestObjectRefJSONPassesRawAgentTextToGo(t *testing.T) {
	for _, raw := range []string{`"sts/shop/db"`, `{"api_version":"apps/v1","kind":"StatefulSet","namespace":"shop","name":"db"}`} {
		var got ObjectRef
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatal(err)
		}
		if got != (ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "shop", Name: "db"}) {
			t.Fatalf("unexpected ref: %+v", got)
		}
	}
}

func FuzzObjectRefParseFormat(f *testing.F) {
	for _, seed := range []string{"Deployment/api", "deploy/shop/api", "shop/deployments.apps/api", "Service/a; Service/b", "Service//api", "example.dev/v2@Widget/shop/example", "\xff\x00"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseObjectRef(raw)
		if err != nil {
			return
		}
		canonical, err := FormatObjectRef(got)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseObjectRef(canonical)
		if err != nil || !reflect.DeepEqual(again, got) {
			t.Fatalf("%q -> %q: %+v != %+v: %v", raw, canonical, got, again, err)
		}
		second, err := FormatObjectRef(again)
		if err != nil || second != canonical {
			t.Fatalf("unstable canonicalization: %q / %q", canonical, second)
		}
	})
}

func FuzzObjectRefTypedRoundTrip(f *testing.F) {
	f.Add("Service", "shop", "api", "v1")
	f.Add("Deployment", "", "api", "")
	f.Add("widget", "shop", "api", "example.dev/v2")
	f.Fuzz(func(t *testing.T, kind, namespace, name, version string) {
		ref := ObjectRef{Kind: kind, Namespace: namespace, Name: name, APIVersion: version}
		text, err := FormatObjectRef(ref)
		if err != nil {
			return
		}
		got, err := ParseObjectRef(text)
		if err != nil || got != ref {
			t.Fatalf("typed round trip %+v -> %q -> %+v: %v", ref, text, got, err)
		}
	})
}

// TestObjectRefGrammarCorpus pins the shared corpus
// tests/fixtures/sdo/contracts/object_ref_grammar.json. The Python copy of the
// grammar (sdo/agents/responder/recorded_actions.py) is checked against the same
// file, so the two parsers cannot drift.
func TestObjectRefGrammarCorpus(t *testing.T) {
	raw, err := os.ReadFile("../../../tests/fixtures/sdo/contracts/object_ref_grammar.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Input string `json:"input"`
		Go    *struct {
			APIVersion string `json:"api_version"`
			Kind       string `json:"kind"`
			Namespace  string `json:"namespace"`
			Name       string `json:"name"`
		} `json:"go"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		got, err := ParseObjectRef(c.Input)
		if c.Go == nil {
			if err == nil {
				t.Errorf("ParseObjectRef(%q) accepted %+v", c.Input, got)
			}
			continue
		}
		want := ObjectRef{APIVersion: c.Go.APIVersion, Kind: c.Go.Kind, Namespace: c.Go.Namespace, Name: c.Go.Name}
		if err != nil || got != want {
			t.Errorf("ParseObjectRef(%q) = %+v, %v; want %+v", c.Input, got, err, want)
		}
	}
}

func TestClusterRBACReferencesKeepTheirKindVersionAndScope(t *testing.T) {
	for _, kind := range []string{"ClusterRole", "ClusterRoleBinding"} {
		ref, err := ParseObjectRef(kind + "/app-cleanup")
		if err != nil || ref.Kind != kind || ref.APIVersion != "rbac.authorization.k8s.io/v1" || ref.Namespace != "" {
			t.Fatalf("cluster RBAC reference: %+v %v", ref, err)
		}
	}
}
