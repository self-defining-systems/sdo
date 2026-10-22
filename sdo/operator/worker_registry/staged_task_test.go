package worker_registry

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedTaskStoreStagesResolvesAndVerifiesDigest(t *testing.T) {
	store := PreparedTaskStore{MountPath: t.TempDir()}
	payload := []byte(`{"task":"frozen"}`)
	reference, err := store.Stage("job-1", payload)
	if err != nil || !IsPreparedTaskReference(reference) {
		t.Fatalf("stage: %q, %v", reference, err)
	}
	if again, err := store.Stage("job-1", payload); err != nil || again != reference {
		t.Fatalf("a retry did not reuse the staged bytes: %q, %v", again, err)
	}
	got, err := store.Resolve("job-1", reference)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("resolve: %q, %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(store.MountPath, ".sdo-runtime/requests/job-1", preparedTaskFile), []byte("altered"), 0o644); err != nil {
		t.Fatal(err)
	}
	var unavailable *PreparedTaskUnavailableError
	if _, err := store.Resolve("job-1", reference); !errors.As(err, &unavailable) {
		t.Fatalf("altered bytes resolved: %v", err)
	}
	store.Release("job-1")
	if _, err := store.Resolve("job-1", reference); !errors.As(err, &unavailable) {
		t.Fatalf("released bytes resolved: %v", err)
	}
}
