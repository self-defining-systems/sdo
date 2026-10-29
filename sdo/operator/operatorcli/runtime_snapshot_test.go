package operatorcli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// localPod runs the Pod command on this host, as the sync and export Pods
// would run it against the repository PVC.
func localPod(calls *int) podExecutor {
	return func(ctx context.Context, _, _ string, command []string, stdout, stderr io.Writer) error {
		if calls != nil {
			*calls++
		}
		cmd := exec.CommandContext(ctx, command[0], command[1:]...)
		cmd.Stdout, cmd.Stderr = stdout, stderr
		return cmd.Run()
	}
}

type snapshotLane struct{ runtime, snapshots, results string }

func newSnapshotLane(t *testing.T) snapshotLane {
	t.Helper()
	root := t.TempDir()
	lane := snapshotLane{filepath.Join(root, "workspace/.sdo-runtime"), filepath.Join(root, "workspace/.sdo-runtime-snapshots"), filepath.Join(root, "results")}
	writeFile(t, filepath.Join(lane.runtime, "sessions/first/session.jsonl"), "first-closed\n")
	writeFile(t, filepath.Join(lane.runtime, "usage/responder-turns.jsonl"), "turn-1\n")
	writeFile(t, filepath.Join(lane.runtime, "sessions/first/codex/auth.json"), "credential-canary")
	return lane
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

func (l snapshotLane) seal(t *testing.T, id string) RuntimeArtifactsResult {
	t.Helper()
	result, err := sealRuntimeSnapshot(context.Background(), localPod(nil), "control", "sync", l.runtime, l.snapshots, id)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func readDelivered(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "sdo_runtime", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestRuntimeSnapshotDeliversClosedIncidentNotNextRowState(t *testing.T) {
	lane := newSnapshotLane(t)
	sealed := lane.seal(t, "incident-one")
	if !sealed.Created || sealed.Bytes == 0 {
		t.Fatalf("snapshot was not created: %+v", sealed)
	}
	// The next row starts writing before the earlier row's export is delivered.
	appendFile(t, filepath.Join(lane.runtime, "sessions/first/session.jsonl"), "next-row\n")
	appendFile(t, filepath.Join(lane.runtime, "usage/responder-turns.jsonl"), "turn-2\n")
	writeFile(t, filepath.Join(lane.runtime, "sessions/second/session.jsonl"), "second-open\n")
	row := filepath.Join(lane.results, "001")
	delivered, err := deliverRuntimeSnapshot(context.Background(), localPod(nil), "control", "export", lane.snapshots, "incident-one", sealed.SHA256, row)
	if err != nil {
		t.Fatal(err)
	}
	if got := readDelivered(t, row, "sessions/first/session.jsonl"); got != "first-closed\n" {
		t.Fatalf("earlier row evidence includes later state: %q", got)
	}
	if got := readDelivered(t, row, "usage/responder-turns.jsonl"); got != "turn-1\n" {
		t.Fatalf("earlier row usage includes later state: %q", got)
	}
	if _, err := os.Stat(filepath.Join(row, "sdo_runtime/sessions/second")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("next row session leaked into the earlier row: %v", err)
	}
	if _, err := os.Stat(filepath.Join(row, "sdo_runtime/sessions/first/codex/auth.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential canary exported: %v", err)
	}
	if !delivered.Retired {
		t.Fatal("delivered archive was not retired from the PVC")
	}
	if _, err := os.Stat(filepath.Join(lane.snapshots, "incident-one.tar")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired archive still present: %v", err)
	}
}

func TestRuntimeSnapshotRetryNeverRecapturesLiveFiles(t *testing.T) {
	lane := newSnapshotLane(t)
	first := lane.seal(t, "incident-one")
	writeFile(t, filepath.Join(lane.runtime, "sessions/second/session.jsonl"), "second-open\n")
	again := lane.seal(t, "incident-one")
	if again.Created || again.SHA256 != first.SHA256 || again.Bytes != first.Bytes {
		t.Fatalf("a repeated seal recaptured live files: first %+v again %+v", first, again)
	}
	entries, err := os.ReadDir(lane.snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("snapshot directory retains partial archives: %v", entries)
	}
}

func TestRuntimeSnapshotRejectsDigestMismatchWithoutPartialTree(t *testing.T) {
	lane := newSnapshotLane(t)
	lane.seal(t, "incident-one")
	row := filepath.Join(lane.results, "001")
	_, err := deliverRuntimeSnapshot(context.Background(), localPod(nil), "control", "export", lane.snapshots, "incident-one", strings.Repeat("0", 64), row)
	if err == nil || !strings.Contains(err.Error(), "does not match sealed digest") {
		t.Fatalf("digest mismatch accepted: %v", err)
	}
	entries, _ := os.ReadDir(row)
	if len(entries) != 0 {
		t.Fatalf("failed delivery left files: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(lane.snapshots, "incident-one.tar")); err != nil {
		t.Fatalf("failed delivery retired its archive: %v", err)
	}
}

func TestRuntimeSnapshotTransportFailureRetriesSameArchive(t *testing.T) {
	lane := newSnapshotLane(t)
	sealed := lane.seal(t, "incident-one")
	row := filepath.Join(lane.results, "001")
	writeFile(t, filepath.Join(row, "sdo_runtime/stage-end.jsonl"), "stage-end")
	broken := func(_ context.Context, _, _ string, _ []string, stdout, _ io.Writer) error {
		_, _ = stdout.Write([]byte("partial"))
		return errors.New("stream reset")
	}
	if _, err := deliverRuntimeSnapshot(context.Background(), broken, "control", "export", lane.snapshots, "incident-one", sealed.SHA256, row); err == nil {
		t.Fatal("broken transport reported delivery")
	}
	if got := readDelivered(t, row, "stage-end.jsonl"); got != "stage-end" {
		t.Fatal("failed delivery replaced the earlier export")
	}
	writeFile(t, filepath.Join(lane.runtime, "sessions/second/session.jsonl"), "second-open\n")
	if _, err := deliverRuntimeSnapshot(context.Background(), localPod(nil), "control", "export", lane.snapshots, "incident-one", sealed.SHA256, row); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(row, "sdo_runtime/sessions/second")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retry recaptured live files")
	}
	if _, err := os.Stat(filepath.Join(row, "sdo_runtime/stage-end.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("sealed delivery kept stale stage-end files")
	}
}

func TestRuntimeSnapshotRedeliveryAfterRetirementIsIdempotent(t *testing.T) {
	lane := newSnapshotLane(t)
	sealed := lane.seal(t, "incident-one")
	row := filepath.Join(lane.results, "001")
	if _, err := deliverRuntimeSnapshot(context.Background(), localPod(nil), "control", "export", lane.snapshots, "incident-one", sealed.SHA256, row); err != nil {
		t.Fatal(err)
	}
	// A crash after publication but before the queue recorded completion retries.
	calls := 0
	if _, err := deliverRuntimeSnapshot(context.Background(), localPod(&calls), "control", "export", lane.snapshots, "incident-one", sealed.SHA256, row); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("redelivery of a published snapshot touched the PVC %d times", calls)
	}
	if delivered, err := runtimeSnapshotDelivered(row, "incident-one", strings.Repeat("1", 64)); err != nil || delivered {
		t.Fatalf("a different digest counted as delivered: %v %v", delivered, err)
	}
}

func TestParseRuntimeArtifactsModes(t *testing.T) {
	base := []string{"runtime-artifacts", "--control-namespace", "control", "--repository-pvc", "repo", "--controller-image", "image"}
	digest := strings.Repeat("a", 64)
	for _, test := range []struct {
		args []string
		ok   bool
	}{
		{[]string{"--output-dir", "/out"}, true},
		{[]string{"--mode", "snapshot", "--snapshot-id", "incident-one"}, true},
		{[]string{"--mode", "snapshot", "--snapshot-id", "../escape"}, false},
		{[]string{"--mode", "snapshot", "--snapshot-id", "incident-one", "--output-dir", "/out"}, false},
		{[]string{"--mode", "deliver", "--snapshot-id", "incident-one", "--sha256", digest, "--output-dir", "/out"}, true},
		{[]string{"--mode", "deliver", "--snapshot-id", "incident-one", "--output-dir", "/out"}, false},
		{[]string{"--mode", "deliver", "--snapshot-id", "incident-one", "--sha256", digest}, false},
		{[]string{"--snapshot-id", "incident-one", "--output-dir", "/out"}, false},
		{[]string{"--mode", "other", "--output-dir", "/out"}, false},
	} {
		_, err := ParseRuntimeArtifactsLaunch(append(append([]string{}, base...), test.args...))
		if (err == nil) != test.ok {
			t.Fatalf("%v: ok=%v err=%v", test.args, test.ok, err)
		}
	}
}
