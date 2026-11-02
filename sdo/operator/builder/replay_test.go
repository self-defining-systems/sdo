package builder

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func TestBuildReplayUsesControllerRegistrations(t *testing.T) {
	testutil.OfflineGo(t, "..")
	root := programFixture(t)
	if err := copyTree(context.Background(), filepath.Join("testdata", "replay-detector"), filepath.Join(root, ".sdo", "diagnostics", "detectors", "incidents", "replay-targetport")); err != nil {
		t.Fatal(err)
	}
	workspace, binary, err := BuildReplay(context.Background(), Config{ApplicationRoot: root, DiagnosticsRoot: filepath.Join(root, ".sdo", "diagnostics"), OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	accepted, err := filepath.Abs(filepath.Join("testdata", "replay-accepted.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-fixtures", filepath.Join(root, ".sdo", "corpus"), "-source", accepted, "-repeat", "50")
	var summary strings.Builder
	command.Stderr = &summary
	output, err := command.Output()
	if err != nil {
		t.Fatalf("%v\n%s\n%s", err, summary.String(), output)
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		var row struct {
			ProgramID string `json:"program_id"`
			Status    string `json:"status"`
		}
		if err = json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.ProgramID == "service-targetport-mismatch" {
			found = true
			if row.Status != "replayed" {
				t.Fatalf("%s", line)
			}
		}
	}
	if !found {
		t.Fatal("production program registry omitted authored program")
	}
	t.Log(summary.String())
}
