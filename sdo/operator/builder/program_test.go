package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func programFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := copyTree(context.Background(), filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app"), root); err != nil {
		t.Fatal(err)
	}
	return root
}

func buildProgramFixture(t *testing.T, root string) (*Workspace, error) {
	t.Helper()
	return BuildWorkspace(context.Background(), Config{ApplicationRoot: root, DiagnosticsRoot: filepath.Join(root, ".sdo", "diagnostics"), OperatorRoot: ".."})
}

func TestBuildWorkspaceRegistersProgramAndSharedLibrary(t *testing.T) {
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

func TestProgramValidationRejectsUnsafeMemory(t *testing.T) {
	for _, tc := range []struct{ name, path, old, replacement, want string }{
		{"network import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"net/http\"", "may not import"},
		{"detector network import", ".sdo/diagnostics/detectors/missing_configmap/detector.go", "import (", "import (\n\t\"net/http\"", "may not import"},
		{"net import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"net\"", "may not import"},
		{"filesystem import", ".sdo/playbooks/service-targetport-mismatch/diagnose.go", "import (", "import (\n\t\"os\"", "may not import"},
		{"exec import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"os/exec\"", "may not import"},
		{"unsafe import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"unsafe\"", "may not import"},
		{"syscall import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"syscall\"", "may not import"},
		{"plugin import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"plugin\"", "may not import"},
		{"missing diagnoser", ".sdo/playbooks/service-targetport-mismatch/diagnose.go", "func (Program) Diagnose", "func (Program) Inspect", "exactly one Diagnoser"},
		{"bad binding", ".sdo/corpus/targetport-mismatch/labels.json", `"kind":"Service"`, `"kind":"Pod"`, "binding"},
		{"incident literal", ".sdo/lib/ports/ports.go", "package ports", "package ports\n// front", "incident literal"},
		{"incident port", ".sdo/playbooks/service-targetport-mismatch/diagnose.go", "package targetport", "package targetport\n// 9999", "incident literal"},
		{"incident image", ".sdo/lib/ports/ports.go", "package ports", "package ports\n// registry.example/demo:v1", "incident literal"},
		{"shell guidance", ".sdo/playbooks/service-targetport-mismatch/README.md", "Check the", "kubectl get services\nCheck the", "shell command"},
		{"shell fence", ".sdo/playbooks/service-targetport-mismatch/README.md", "Check the", "```bash\n```\nCheck the", "shell command"},
		{"spaced shell fence", ".sdo/playbooks/service-targetport-mismatch/README.md", "Check the", "``` bash\n```\nCheck the", "shell command"},
		{"tilde shell fence", ".sdo/playbooks/service-targetport-mismatch/README.md", "Check the", "~~~sh\n~~~\nCheck the", "shell command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := programFixture(t)
			path := filepath.Join(root, tc.path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(data), tc.old, tc.replacement, 1)), 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := buildProgramFixture(t, root)
			if w != nil {
				defer w.Close()
			}
			if err == nil {
				_, err = w.ValidateAndBuild(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProgramWithTwoMitigatorsIsRejected(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "mitigate.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	extra := `
type ExtraMitigator struct{}
func (ExtraMitigator) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "service-targetport-mismatch"} }
func (ExtraMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) { return program.Plan{}, nil }
`
	if err := os.WriteFile(path, append(data, []byte(extra)...), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "at most one Mitigator") {
		t.Fatalf("two mitigators: %v", err)
	}
}

func TestProgramWithTwoDiagnosersIsRejected(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "mitigate.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	extra := `
type ExtraDiagnoser struct{}
func (ExtraDiagnoser) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "service-targetport-mismatch"} }
func (ExtraDiagnoser) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) { return program.Verdict{Result: program.NoMatch}, nil }
`
	if err := os.WriteFile(path, append(data, []byte(extra)...), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "exactly one Diagnoser") {
		t.Fatalf("two diagnosers: %v", err)
	}
}

func TestProgramMustCompile(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "diagnose.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "ports.Matches(target, exposed)", "missing.Matches(target, exposed)", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ValidateAndBuild(context.Background()); err == nil || !strings.Contains(err.Error(), "go test") {
		t.Fatalf("compile failure: %v", err)
	}
}

func TestSharedLibraryChecksItsOriginatingOutcomeWithoutProgram(t *testing.T) {
	root := programFixture(t)
	if err := os.RemoveAll(filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".sdo", "lib", "ports", "ports.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte("\n// front\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "incident literal") {
		t.Fatalf("library literal accepted: %v", err)
	}
}

func TestProgramFaultClassCannotContainIncidentName(t *testing.T) {
	root := programFixture(t)
	old := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch")
	newPath := filepath.Join(root, ".sdo", "playbooks", "service-front-targetport")
	if err := os.Rename(old, newPath); err != nil {
		t.Fatal(err)
	}
	readme := filepath.Join(newPath, "README.md")
	data, err := os.ReadFile(readme)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readme, []byte(strings.Replace(string(data), "fault_class: service-targetport-mismatch", "fault_class: service-front-targetport", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "fault class") || !strings.Contains(err.Error(), "incident literal") {
		t.Fatalf("literal fault class accepted: %v", err)
	}
}

func TestProgramRejectsImageFromFindingMetadata(t *testing.T) {
	root := programFixture(t)
	outcomes, err := filepath.Glob(filepath.Join(root, ".sdo", "outcomes", "*.json"))
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("fixture outcome: %v", err)
	}
	data, err := os.ReadFile(outcomes[0])
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(data), `"findings": [{`, `"findings": [{"metadata":{"image":"registry.example/metadata:v2"},`, 1)
	if err := os.WriteFile(outcomes[0], []byte(updated), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "diagnose.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(source, []byte("\n// registry.example/metadata:v2\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "incident literal") {
		t.Fatalf("metadata image accepted: %v", err)
	}
}

func TestCorpusCaseCannotNameUnknownProgram(t *testing.T) {
	root := programFixture(t)
	source := filepath.Join(root, ".sdo", "corpus", "targetport-mismatch")
	other := filepath.Join(root, ".sdo", "corpus", "unknown-program")
	if err := copyTree(context.Background(), source, other); err != nil {
		t.Fatal(err)
	}
	labels := filepath.Join(other, "labels.json")
	data, err := os.ReadFile(labels)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(labels, []byte(strings.Replace(string(data), "service-targetport-mismatch", "missing-program", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if w != nil {
		defer w.Close()
	}
	if err == nil {
		_, err = w.ValidateAndBuild(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "unknown program") {
		t.Fatalf("unknown corpus label accepted: %v", err)
	}
}

func TestProgramAndLibraryRejectPersistentShellFiles(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{".sdo/playbooks/service-targetport-mismatch/scripts/repair.sh", "shell scripts"},
		{".sdo/lib/ports/repair.sh", "unexpected shared library file"},
		{".sdo/corpus/targetport-mismatch/repair.sh", "unexpected corpus file"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			root := programFixture(t)
			path := filepath.Join(root, tc.path)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("echo repair\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := buildProgramFixture(t, root)
			if w != nil {
				defer w.Close()
			}
			if err == nil {
				_, err = w.ValidateAndBuild(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("shell file accepted: %v", err)
			}
		})
	}
}

func TestProgramGeneralizationIsRequired(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo", "corpus", "targetport-mismatch", "fixed.json")
	data, err := os.ReadFile(filepath.Join(root, ".sdo", "corpus", "targetport-mismatch", "open.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ValidateAndBuild(context.Background()); err == nil || !strings.Contains(err.Error(), "fixed") {
		t.Fatalf("generalization failure: %v", err)
	}
}
