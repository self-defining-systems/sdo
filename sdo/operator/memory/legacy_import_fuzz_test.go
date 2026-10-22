package memory

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// FuzzLegacyOutcomeStream attacks the retired JSONL outcome stream, the one
// outcome input that is hand-edited text. Whatever the bytes are, readers must
// not panic, must show each incident once, and importing must preserve exactly
// what readers saw.
func FuzzLegacyOutcomeStream(f *testing.F) {
	for _, seed := range []string{
		legacyPhysical + "\n",
		legacyPhysical + "\n" + legacyPhysical + "\n",
		legacyPhysical + "\n\n   \n",
		legacyPhysical, // no trailing newline
		`{"schema_version":1,"incident_id":"a","classification":"success"}` + "\n" + `{"schema_version":1,"incident_id":"a","classification":"failure"}` + "\n",
		`{"schema_version":1,"incident_id":"b","classification":"success","applied_playbooks":["../x","a/b"]}` + "\n",
		"[]\n", "null\n", "{}\n", "\xff\xfe\n", `{"incident_id":"` + string(bytes.Repeat([]byte("x"), 300)) + `"}` + "\n",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, stream []byte) {
		ctx := context.Background()
		root := t.TempDir()
		path := filepath.Join(root, ".sdo", "outcomes.jsonl")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, stream, 0o644); err != nil {
			t.Fatal(err)
		}
		store := NewJSONStore(root)
		seen, err := store.List(ctx)
		if err != nil {
			if importErr := store.ImportLegacy(ctx); importErr == nil {
				t.Fatalf("import accepted a stream the readers reject (%v)", err)
			}
			return
		}
		ids := map[string]bool{}
		for _, outcome := range seen {
			if ids[outcome.IncidentID] {
				t.Fatalf("incident %q listed twice", outcome.IncidentID)
			}
			ids[outcome.IncidentID] = true
		}
		if err := store.ImportLegacy(ctx); err != nil {
			return // conflicting duplicate records are rejected, keeping the stream for repair
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("legacy stream remains after a successful import: %v", err)
		}
		after, err := store.List(ctx)
		if err != nil || len(after) != len(seen) {
			t.Fatalf("import changed the history: %d outcomes became %d (%v)", len(seen), len(after), err)
		}
		for i := range seen {
			if !bytes.Equal(seen[i].document, after[i].document) {
				t.Fatalf("outcome %q changed in import:\n%s\n%s", seen[i].IncidentID, seen[i].document, after[i].document)
			}
			if got, err := store.Get(ctx, seen[i].IncidentID); err != nil || !bytes.Equal(got.document, seen[i].document) {
				t.Fatalf("Get(%q) disagrees with List: %v", seen[i].IncidentID, err)
			}
		}
		if err := store.ImportLegacy(ctx); err != nil {
			t.Fatalf("second import is not a no-op: %v", err)
		}
	})
}
