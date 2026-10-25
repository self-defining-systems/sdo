package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
)

func TestMemoryImportPrepareAndPublishRequirements(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.name", "T"}, {"config", "user.email", "t@example.com"}, {"commit", "--allow-empty", "-qm", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	work := filepath.Join(t.TempDir(), "broker")
	args := []string{"import", "prepare", "--root", root, "--worktree-root", work, "--id", "operator-1"}
	var out, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	if code := RunMemory(context.Background(), args, getenv, &out, &stderr); code != 0 {
		t.Fatalf("prepare: %d %s", code, stderr.String())
	}
	var proposal broker.Proposal
	if err := json.Unmarshal(out.Bytes(), &proposal); err != nil {
		t.Fatal(err)
	}
	if proposal.Path == root || proposal.BaseCommit == "" || proposal.IncidentID != "operator-1" {
		t.Fatalf("not an isolated base-bound proposal: %+v", proposal)
	}
	out.Reset()
	stderr.Reset()
	args[1] = "publish"
	if code := RunMemory(context.Background(), args, getenv, &out, &stderr); code != exitInfra {
		t.Fatalf("publish without isolated validator configuration: %d %s", code, stderr.String())
	}
}
