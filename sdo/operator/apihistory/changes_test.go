package apihistory

import (
	"reflect"
	"strings"
	"testing"
)

func TestChangedPathsKeepKeysAndDropValues(t *testing.T) {
	body := `{"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"api","image":"registry.invalid/api:secretvalue"}]}}}}`
	paths, ok := ChangedPaths("deployments", "application/strategic-merge-patch+json", []byte(body))
	want := []string{"spec.replicas", "spec.template.spec.containers[api].image"}
	if !ok || !reflect.DeepEqual(paths, want) {
		t.Fatalf("paths = %v, %v; want %v", paths, ok, want)
	}
	for _, path := range paths {
		if strings.Contains(path, "secretvalue") || strings.Contains(path, "registry") {
			t.Fatalf("value leaked into path %q", path)
		}
	}
}

func TestChangedPathsNeverDescendBelowSecretData(t *testing.T) {
	body := `{"data":{"db-password-canary":"c2VjcmV0"},"stringData":{"token-canary":"x"},"metadata":{"name":"db"}}`
	paths, ok := ChangedPaths("secrets", "application/merge-patch+json", []byte(body))
	if !ok {
		t.Fatal("paths unknown")
	}
	for _, path := range paths {
		if strings.Contains(path, "canary") {
			t.Fatalf("secret key leaked: %q", path)
		}
	}
	if !reflect.DeepEqual(paths, []string{"data", "metadata.name", "stringData"}) {
		t.Fatalf("paths = %v", paths)
	}
}

func TestChangedPathsJSONPatchHidesSecretDataPaths(t *testing.T) {
	body := `[{"op":"replace","path":"/data/password-canary","value":"x"},{"op":"replace","path":"/spec/replicas","value":2}]`
	paths, ok := ChangedPaths("secrets", "application/json-patch+json", []byte(body))
	if !ok {
		t.Fatal("paths unknown")
	}
	for _, path := range paths {
		if strings.Contains(path, "canary") {
			t.Fatalf("secret key leaked: %q", path)
		}
	}
}

func TestChangedPathsOpaqueUnrecognizedSegmentsAndBound(t *testing.T) {
	paths, ok := ChangedPaths("pods", "application/merge-patch+json", []byte(`{"metadata":{"annotations":{"has space and password=hunter2":"v"}}}`))
	if !ok || len(paths) != 1 || strings.Contains(paths[0], "hunter2") || !strings.Contains(paths[0], Opaque) {
		t.Fatalf("paths = %v, %v", paths, ok)
	}
	var many strings.Builder
	many.WriteString(`{"spec":{`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			many.WriteString(",")
		}
		many.WriteString(`"f` + strings.Repeat("x", 5) + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `":1`)
	}
	many.WriteString(`}}`)
	paths, _ = ChangedPaths("pods", "application/merge-patch+json", []byte(many.String()))
	if len(paths) > MaxChangedPaths {
		t.Fatalf("recorded %d paths, cap is %d", len(paths), MaxChangedPaths)
	}
	if _, ok := ChangedPaths("pods", "application/merge-patch+json", []byte("not json")); ok {
		t.Fatal("unparseable body must report unknown paths")
	}
}

func TestRedactCommandKeepsOnlySafeArguments(t *testing.T) {
	for name, tc := range map[string]struct {
		argv []string
		want []string
	}{
		"clean command stays readable": {[]string{"nginx", "-s", "reload"}, []string{"nginx", "-s", "reload"}},
		"program is its base name":     {[]string{"/usr/bin/pg_isready"}, []string{"pg_isready"}},
		"attached short-flag password": {[]string{"mysql", "-pSECRET", "mydb"}, []string{"mysql", Withheld, "mydb"}},
		"value after a short flag":     {[]string{"redis-cli", "-a", "SECRET", "ping"}, []string{"redis-cli", "-a", Withheld, "ping"}},
		"url with userinfo":            {[]string{"psql", "postgresql://u:p@h/db"}, []string{"psql", Withheld}},
		"curl basic auth":              {[]string{"curl", "-u", "u:p", "http://h"}, []string{"curl", "-u", Withheld, Withheld}},
		"mongo url":                    {[]string{"mongosh", "mongodb://u:p@h"}, []string{"mongosh", Withheld}},
		"key=value":                    {[]string{"env", "TOKEN=abc", "run"}, []string{"env", Withheld, "run"}},
		"long flag with value":         {[]string{"tool", "--password=abc"}, []string{"tool", Withheld}},
		"shell payload":                {[]string{"sh", "-c", "echo hi", "x"}, []string{"sh", "-c", Withheld, Withheld}},
		"token-length argument":        {[]string{"tool", strings.Repeat("a", 80)}, []string{"tool", Withheld}},
	} {
		if got := RedactCommand(tc.argv); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: RedactCommand(%q) = %q, want %q", name, tc.argv, got, tc.want)
		}
	}
}
