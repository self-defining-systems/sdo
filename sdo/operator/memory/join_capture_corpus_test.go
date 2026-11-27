package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A fault that started while the incident was open shows only in the capture
// taken when its alert joined. A playbook for that fault takes its case from
// that capture, the case passes provenance, and reads recorded there append.
func TestACaseFromAJoinCaptureIsBuiltFromTheStateTheAlertJoinedIn(t *testing.T) {
	sessions := t.TempDir()
	captures, err := IncidentCorpusDir(sessions, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	opening, _ := json.Marshal(sdktest.Snapshot{NamespaceName: "opening"})
	joined, _ := json.Marshal(sdktest.Snapshot{NamespaceName: "joined"})
	writeCaptureFile(t, filepath.Join(captures, "fixed.json"), "{}")
	writeCaptureFile(t, filepath.Join(captures, "open@detect.json"), string(opening))
	writeCaptureFile(t, filepath.Join(captures, "open@join-1.json"), string(joined))
	for _, phase := range []string{"open@detect", "open@join-1"} {
		writeCaptureFile(t, filepath.Join(captures, phase+"-recordings.json"), "[]")
	}
	if err := AppendCapturedRecordings(sessions, "incident-1", "open@join-1", []sdktest.PrimitiveCall{{Method: "Log", Arguments: json.RawMessage(`{}`)}}); err != nil {
		t.Fatalf("join capture recordings: %v", err)
	}

	root := t.TempDir()
	origins := map[string]string{CapturedInstanceKey("later", program.Bindings{}): "join-1"}
	paths, err := RecordCorpus(root, captures, "incident-1", map[string][]program.Bindings{"later": {{}}}, origins, CaseContext{})
	if err != nil || len(paths) == 0 {
		t.Fatalf("RecordCorpus = %v, %v", paths, err)
	}
	var caseDir string
	for _, path := range paths {
		if filepath.Base(path) == "labels.json" {
			caseDir = filepath.Dir(filepath.Join(root, path))
		}
	}
	loaded, err := sdktest.LoadCorpusMetadata(caseDir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Provenance.OpenAt != "join-1" {
		t.Fatalf("open_at = %q, want join-1", loaded.Provenance.OpenAt)
	}
	open, err := os.ReadFile(sdktest.CorpusSnapshotFile(caseDir, "open.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !sdktest.SnapshotMatchesDigest(open, loaded.Provenance.OpenSHA256) || string(open) == string(opening) {
		t.Fatalf("case open state is not the joined capture: %s", open)
	}
	if err := ValidateCapturedCorpusDirectory(captures, root, "incident-1", paths); err != nil {
		t.Fatalf("a join-capture case failed provenance: %v", err)
	}
}

// Only a well-formed join phase is a capture; anything else stays refused.
func TestOnlyWellFormedJoinPhasesAreCaptures(t *testing.T) {
	for phase, want := range map[string]bool{"join-1": true, "join-12": true, "join-0": false, "join-": false, "join-1x": false, "settle": false} {
		if got := sdktest.IsJoinCapturePhase(phase); got != want {
			t.Errorf("IsJoinCapturePhase(%q) = %v, want %v", phase, got, want)
		}
	}
	sessions := t.TempDir()
	if err := AppendCapturedRecordings(sessions, "incident-1", "open@join-0", nil); err == nil {
		t.Fatal("open@join-0 accepted as a recording phase")
	}
}
