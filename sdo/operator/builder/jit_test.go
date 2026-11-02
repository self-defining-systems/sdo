package builder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
	"time"
)

func TestBuildWorkingProgramWithoutCorpusOrDetectors(t *testing.T) {
	testutil.OfflineGo(t, "..")
	root := programFixture(t)
	for _, part := range []string{"diagnostics/detectors", "corpus"} {
		if err := os.RemoveAll(filepath.Join(root, ".sdo", part)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	started := time.Now()
	w, binary, err := BuildWorkingProgram(ctx, Config{ApplicationRoot: root, OperatorRoot: ".."}, "service-targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	t.Logf("warm compile: %s", time.Since(started))
	command := exec.CommandContext(ctx, binary)
	command.Stdin = strings.NewReader("{\"command\":\"spec\"}\n")
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "service-targetport-mismatch") {
		t.Fatalf("runner: %s: %v", output, err)
	}
}
