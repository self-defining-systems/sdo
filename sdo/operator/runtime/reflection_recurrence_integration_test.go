//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/builder"
)

// Compile the accepted programs and detector into the recurrence driver. This
// prevents a hand-written test double from standing in for the learned code.
func runCompiledLearningRecurrence(t *testing.T, ctx context.Context, built *builder.Workspace, accepted string, request IncidentRequest) {
	t.Helper()
	request.IncidentID = "identical-recurrence"
	request.SourceCommit = nativeGit(t, accepted, "rev-parse", "HEAD")
	request.RepositoryBaseCommit = request.SourceCommit
	request.DeployedCommit = request.SourceCommit
	request.RepositoryWorktree = accepted
	request.PreparedAgentTask = ""
	request.InitialProgramRuns = nil
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(built.Directory, "generated/recurrence_test.go")
	if err := os.WriteFile(path, []byte(compiledLearningRecurrence), 0600); err != nil {
		t.Fatal(err)
	}
	registrations, err := filepath.Abs("../../agents/registrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-count=1", "-run", "^(TestCompiledIdenticalRecurrence|TestCompiledStaleStepStaysBroken)$", "./generated")
	cmd.Dir = built.Directory
	cmd.Env = append(os.Environ(), "RL1_REQUEST="+input, "RL1_REGISTRATIONS="+registrations)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compiled detector/program recurrence: %v: %s", err, output)
	} else {
		t.Logf("compiled recurrence: %s", output)
	}
}
