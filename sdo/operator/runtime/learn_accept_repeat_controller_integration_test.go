//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/builder"
)

// Execute the accepted bundle in its own module so generated All/Programs are
// the authored, validated code, rather than an in-process detector substitute.
func runCompiledLearnAcceptRepeatController(t *testing.T, ctx context.Context, built *builder.Workspace, accepted string, request IncidentRequest) {
	t.Helper()
	acceptedCommit := nativeGit(t, accepted, "rev-parse", "HEAD")
	if acceptedCommit == request.SourceCommit {
		t.Fatal("cycle did not accept new learned memory after the first incident")
	}
	request.SourceCommit = acceptedCommit
	request.RepositoryWorktree = accepted
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "accepted-request.json")
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := format.Source([]byte(compiledLearnAcceptRepeatController))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(built.Directory, "generated/learn_accept_repeat_test.go")
	if err := os.WriteFile(path, source, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "test", "-race", "-v", "-count=1", "-timeout=45s", "-run", "^TestAcceptedLearnedControllerRepeat$", "./generated")
	command.Dir = built.Directory
	command.Env = append(os.Environ(), "LE_CYCLE_REQUEST="+input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("accepted learned controller repeat: %v: %s", err, output)
	}
	t.Logf("dynamic accepted-memory controller cycle: %s", output)
}

