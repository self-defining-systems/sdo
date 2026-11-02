//go:build integration

package builder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	programvalidation "sdo.dev/operator/memory/validate/program"
)

func TestBuildWorkspaceRegistersProgramAndSharedLibrary(t *testing.T) {
	testutil.OfflineGo(t, "..")
	t.Setenv("GOFLAGS", os.Getenv("GOFLAGS")+" -mod=mod -p=2")
	root := programFixture(t)
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ValidateAndBuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"playbooks/service-targetport-mismatch/diagnose.go", "lib/ports/ports.go", "generated/programs.go"} {
		if _, err := os.Stat(filepath.Join(w.Directory, path)); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

// A peer reflection on the shared hotel memory (PAR3) turned the learned
// program's one required Deployment role into two optional aliases. Unnamed
// health findings then bound neither, and the repeat's 70 diagnoses all ran
// unbound. The save-time declaration gate refuses the edit.
func TestProgramAliasRolesRefusedAtSave(t *testing.T) {
	testutil.OfflineGo(t, "..")
	t.Setenv("GOFLAGS", os.Getenv("GOFLAGS")+" -mod=mod -p=2")
	root := programFixture(t)
	path := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/diagnose.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(string(data), `{{Name: "service", Kind: "Service", Required: true}}`, `{{Name: "resource", Kind: "Service"}, {Name: "service", Kind: "Service"}}`, 1)
	if source == string(data) {
		t.Fatal("missing declaration anchor")
	}
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.ValidateAndBuild(context.Background())
	if err == nil || !strings.Contains(err.Error(), "program.finding-bindings") || !strings.Contains(err.Error(), "[resource service]") {
		t.Fatalf("alias roles reached accepted memory: %v", err)
	}
}

// Declaration rules judge only the playbooks a candidate edits, so accepted
// memory never blocks an outcome publication that does not touch it.
func TestChangedProgramsAreTheCandidateEditedPlaybooks(t *testing.T) {
	root := t.TempDir()
	programs := []programvalidation.Spec{{ID: "edited"}, {ID: "accepted"}, {ID: "added"}}
	if got := changedPrograms(context.Background(), root, programs); !slices.Equal(got, []string{"edited", "accepted", "added"}) {
		t.Fatalf("without Git every program is checked: %v", got)
	}
	git := func(args ...string) {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", root, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, output)
		}
	}
	write := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q")
	write(".sdo/playbooks/edited/diagnose.go", "package edited\n")
	write(".sdo/playbooks/accepted/diagnose.go", "package accepted\n")
	git("add", ".")
	git("commit", "-q", "-m", "accepted")
	if got := changedPrograms(context.Background(), root, programs); len(got) != 0 {
		t.Fatalf("accepted programs re-judged: %v", got)
	}
	write(".sdo/playbooks/edited/README.md", "# edited\n")
	write(".sdo/playbooks/added/diagnose.go", "package added\n")
	write(".sdo/outcomes/x.json", "{}")
	if got := changedPrograms(context.Background(), root, programs); !slices.Equal(got, []string{"edited", "added"}) {
		t.Fatalf("edited playbooks: %v", got)
	}
}
