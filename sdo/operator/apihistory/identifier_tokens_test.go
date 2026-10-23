package apihistory

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryScopedOpaqueIdentityTokens(t *testing.T) {
	const secret = "Authorization: Bearer kp3c-canary"
	record := func(dir string, list bool) Event {
		t.Helper()
		s, err := Open(dir, "incident", Limits{})
		if err != nil {
			t.Fatal(err)
		}
		obj := map[string]any{"kind": "Pod", "apiVersion": "v1", "metadata": map[string]any{"name": "pod", "namespace": "app", "resourceVersion": secret}}
		if list {
			obj = map[string]any{"kind": "PodList", "apiVersion": "v1", "metadata": map[string]any{"resourceVersion": secret}, "items": []any{obj}}
		}
		raw, _ := json.Marshal(obj)
		s.Record(Event{Phase: "end", Name: secret, Subresource: secret, Resource: "pods", Namespace: "app", ResourceVersion: secret}, raw)
		s.Record(Event{Phase: "end", Name: secret + "-different", Subresource: "status", ResourceVersion: secret + "-different"}, nil)
		s.Close()
		h, err := Read(dir)
		if err != nil {
			t.Fatal(err)
		}
		e := h.Events[len(h.Events)-2]
		other := h.Events[len(h.Events)-1]
		if e.Name == secret || !strings.HasPrefix(e.Name, "opaque:") || !strings.HasPrefix(e.Subresource, "opaque:") || !strings.HasPrefix(e.ResourceVersion, "opaque:") {
			t.Fatal("invalid identity was not tokenized")
		}
		if e.Name == other.Name || e.ResourceVersion == other.ResourceVersion || other.Subresource != "status" {
			t.Fatal("identity equality or known subresource lost")
		}
		var body map[string]any
		if err := json.Unmarshal(h.Body(e), &body); err != nil {
			t.Fatal(err)
		}
		if body["metadata"].(map[string]any)["resourceVersion"] != e.ResourceVersion {
			t.Fatal("envelope/blob versions disagree")
		}
		if list {
			if body["items"].([]any)[0].(map[string]any)["metadata"].(map[string]any)["resourceVersion"] != e.ResourceVersion {
				t.Fatal("list/item versions disagree")
			}
		}
		err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				b, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Contains(b, []byte("kp3c-canary")) {
					t.Error("canary persisted")
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	dir := t.TempDir()
	a := record(dir, false)
	b := record(dir, true)
	c := record(t.TempDir(), true)
	if a.Name != b.Name || a.Subresource != b.Subresource || a.ResourceVersion != b.ResourceVersion {
		t.Fatal("tokens changed after reopening same history")
	}
	if a.Name == c.Name || a.Subresource == c.Subresource || a.ResourceVersion == c.ResourceVersion {
		t.Fatal("tokens correlate across histories")
	}
}

func TestIdentityEnvelopeGrammarControls(t *testing.T) {
	s, err := Open(t.TempDir(), "incident", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s.Record(Event{Phase: "end", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app-ns", Name: "web.worker", Subresource: "scale", ResourceVersion: "12345"}, nil)
	s.Record(Event{Phase: "end", Group: "Authorization: Bearer kp3c-canary", Version: "bad version", Resource: "bad resource", Namespace: "bad namespace", Name: "UPPER", Subresource: "bad subresource"}, nil)
	s.Close()
	h, err := Read(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	a, b := h.Events[0], h.Events[1]
	if a.Group != "apps" || a.Version != "v1" || a.Resource != "deployments" || a.Namespace != "app-ns" || a.Name != "web.worker" || a.Subresource != "scale" || a.ResourceVersion != "12345" {
		t.Fatal("accepted envelope identifiers lost")
	}
	if b.Group != "" || b.Version != "" || b.Resource != "" || b.Namespace != "" || !b.Partial || !strings.HasPrefix(b.Name, "opaque:") || !strings.HasPrefix(b.Subresource, "opaque:") {
		t.Fatal("invalid envelope identity retained or broadened")
	}
}
