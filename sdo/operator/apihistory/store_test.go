package apihistory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreProjectsBeforeHashingAndBoundsIncident(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "incident", Limits{Bytes: 4096, Events: 2, Queue: 16})
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db","namespace":"app","resourceVersion":"123"},"data":{"password":"cHJpdmF0ZQ=="},"stringData":{"token":"private"}}`)
	for range 4 {
		s.Record(Event{Started: time.Now(), Phase: "end", Verb: "get", Resource: "secrets", Namespace: "app", Name: "db", Status: 200}, raw)
	}
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Events) != 2 || h.Manifest.Complete || h.Manifest.Dropped != 2 {
		t.Fatalf("history: %+v", h)
	}
	if h.Events[0].BodyHash != h.Events[1].BodyHash || h.Events[0].ResourceVersion != "123" {
		t.Fatal("missing hash or resourceVersion")
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "blobs"))
	if len(entries) != 1 {
		t.Fatal("bodies not deduplicated")
	}
	filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
		if e == nil && !d.IsDir() {
			b, _ := os.ReadFile(p)
			if strings.Contains(string(b), "private") || strings.Contains(string(b), "cHJpdmF0ZQ==") {
				t.Errorf("secret stored in %s", p)
			}
		}
		return e
	})
	var body map[string]any
	if err := json.Unmarshal(h.Body(h.Events[0]), &body); err != nil {
		t.Fatal(err)
	}
	if body["data"].(map[string]any)["password"] != "" {
		t.Fatalf("secret projection: %v", body)
	}
}

func TestStoreFailsOpenAndPersistsGap(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "incident", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(filepath.Join(dir, "blobs"))
	s.Record(Event{Phase: "end", Verb: "get"}, []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x"}}`))
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Dropped != 1 {
		t.Fatalf("unmarked disk failure: %+v", h.Manifest)
	}
}

func TestStoreRestartAndMissingHistoryAreUnknown(t *testing.T) {
	dir := t.TempDir()
	if _, err := Read(dir); err == nil {
		t.Fatal("missing history accepted")
	}
	s, _ := Open(dir, "incident", Limits{})
	s.Record(Event{Phase: "start", Verb: "patch", Started: time.Now()}, nil)
	s.Close()
	s, err := Open(dir, "incident", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Epoch != 2 || len(h.Events) != 1 {
		t.Fatalf("restart: %+v", h)
	}
	if _, err := Open(dir, "other", Limits{}); err == nil {
		t.Fatal("cross incident append accepted")
	}
}

func TestStoreByteCapAndQueueGap(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "incident", Limits{Bytes: 1, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		s.Record(Event{Phase: "start", Verb: "get"}, nil)
	}
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Events) != 0 || h.Manifest.Dropped != 100 || h.Manifest.Complete || h.Manifest.Bytes > 1 {
		t.Fatalf("caps: %+v", h)
	}
}

func TestUnfinishedRequestNeverClaimsCompleteHistory(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, "incident", Limits{})
	s.Record(Event{RequestID: "unfinished", Phase: "start", Verb: "patch", Started: time.Now()}, nil)
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete {
		t.Fatal("unfinished mutation claims complete history")
	}
}

func TestDefaultQueueRetainsSmallRequestBursts(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "incident", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		s.Record(Event{Phase: "end", Verb: "get"}, nil)
	}
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Events) != 100 || h.Manifest.Dropped != 0 {
		t.Fatalf("small burst exceeded queue budget: events=%d dropped=%d", len(h.Events), h.Manifest.Dropped)
	}
}

func TestRestartRecoversCapAccountingAfterManifestLag(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, "incident", Limits{})
	s.Record(Event{Phase: "end", Verb: "get"}, nil)
	s.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	h.Manifest.Events = 0
	h.Manifest.Bytes = 0
	data, _ := json.Marshal(h.Manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, "incident", Limits{Events: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.Record(Event{Phase: "end", Verb: "get"}, nil)
	s.Close()
	h, err = Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Events) != 1 || h.Manifest.Dropped != 1 {
		t.Fatalf("restart exceeded incident cap: %+v", h.Manifest)
	}
}
