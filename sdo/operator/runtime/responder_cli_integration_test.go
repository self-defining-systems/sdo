//go:build integration

package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
)

// The real Python CLI talks to the real controller-side responder loop over the
// session file transport: diagnose a playbook by name, then mitigate it by name
// with the command diagnose printed. No hash identifier appears anywhere.
func TestCLIDiagnoseThenMitigateByName(t *testing.T) {
	_, file, _, _ := goruntime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	python := os.Getenv("SDO_E2E_PYTHON")
	if python == "" {
		python = filepath.Join(root, ".venv", "bin", "python")
	}
	if _, err := os.Stat(python); err != nil {
		t.Skipf("python for the CLI is unavailable: %v", err)
	}
	world, loop := targetLoop(t)
	session := filepath.Join(t.TempDir(), "tools")
	server := &SessionToolServer{Directory: session, Handle: func(ctx context.Context, request ToolRequest) ToolResponse { return loop.Handle(ctx, request) }}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ctx) }()
	defer func() { cancel(); <-done }()

	sdo := func(args ...string) (string, error) {
		command := exec.Command(python, append([]string{"-m", "sdo.agents.responder.tools_cli"}, args...)...)
		command.Dir = root
		command.Env = append(os.Environ(), "SDO_TOOLS_DIR="+session, "SDO_SESSION_SCRATCH="+t.TempDir())
		output, err := command.CombinedOutput()
		return string(output), err
	}

	wrong, err := sdo("diagnose", "aware-restart", "--bind", "redis="+apiBinding)
	if err == nil || !strings.Contains(wrong, "aware-restart has no parameter 'redis'; it takes deployment=Deployment/NAMESPACE/NAME") {
		t.Fatalf("wrong role must fail with usage (err=%v):\n%s", err, wrong)
	}
	diagnosed, err := sdo("diagnose", "aware-restart", "--bind", "deployment="+apiBinding)
	fix := "sdo mitigate aware-restart --bind deployment=Deployment/app/api"
	if err != nil || !strings.Contains(diagnosed, "To fix: "+fix) {
		t.Fatalf("diagnose must print the fix (err=%v):\n%s", err, diagnosed)
	}
	applied, _ := sdo(strings.Fields(strings.TrimPrefix(fix, "sdo "))...)
	if world.restarts.Load() != 1 {
		t.Fatalf("the fix from diagnose did not apply once:\n%s", applied)
	}
	if strings.Contains(applied+diagnosed, "program-") {
		t.Fatalf("hash identifiers leaked to the responder:\n%s\n%s", diagnosed, applied)
	}
}
