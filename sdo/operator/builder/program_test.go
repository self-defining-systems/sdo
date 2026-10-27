package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	programvalidation "sdo.dev/operator/memory/validate/program"
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

func TestProgramAcceptsRecordedSyntheticNoMatchCase(t *testing.T) {
	testutil.OfflineGo(t, "..")
	root := programFixture(t)
	source := filepath.Join(root, ".sdo", "corpus", "targetport-mismatch")
	dir := filepath.Join(root, ".sdo", "corpus", "near-miss")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	fixed, err := os.ReadFile(filepath.Join(source, "fixed.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"open.json", "fixed.json", "healthy.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), fixed, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "recordings.json"), []byte("[]"), 0o644); err != nil {
		t.Fatal(err)
	}
	labels := `{"no_match_program_ids":["service-targetport-mismatch"],"bindings":{"service":{"kind":"Service","namespace":"demo","name":"front"}},"synthetic_no_match_reason":"healthy port near miss"}`
	if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(labels), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ValidateAndBuild(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestProgramSaveReplayDoesNotRequireRenameInvariance(t *testing.T) {
	for _, tc := range []struct {
		name, old, replacement string
		port                   bool
	}{
		{"service name", "ref := b.Objects[\"service\"]", "ref := b.Objects[\"service\"]\nif ref.Name != \"front\" { return program.Verdict{Result: program.NoMatch}, nil }", false},
		{"port 8087", "target := port.TargetPort", "target := port.TargetPort\nif target.Type == intstr.Int && target.IntVal != 8087 { return program.Verdict{Result: program.NoMatch}, nil }", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := programFixture(t)
			path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "diagnose.go")
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(source), tc.old) {
				t.Fatalf("mutation anchor missing: %s", tc.old)
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(source), tc.old, tc.replacement, 1)), 0o644); err != nil {
				t.Fatal(err)
			}
			if tc.port {
				open := filepath.Join(root, ".sdo", "corpus", "targetport-mismatch", "open.json")
				data, err := os.ReadFile(open)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(open, []byte(strings.Replace(string(data), `"targetPort":9999`, `"targetPort":8087`, 1)), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			w, err := buildProgramFixture(t, root)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			_, err = w.ValidateAndBuild(context.Background())
			if err != nil {
				t.Fatalf("repair replay should not require renamed fixtures (%s): %v", tc.name, err)
			}
		})
	}
}

func TestProgramValidationRejectsUnsafeMemory(t *testing.T) {
	for _, tc := range []struct{ name, path, old, replacement, want string }{
		{"network import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"net/http\"", "may not import"},
		{"detector network import", ".sdo/diagnostics/detectors/incidents/missing-configmap/detector.go", "import (", "import (\n\t\"net/http\"", "may not import"},
		{"net import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"net\"", "may not import"},
		{"filesystem import", ".sdo/playbooks/service-targetport-mismatch/diagnose.go", "import (", "import (\n\t\"os\"", "may not import"},
		{"exec import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"os/exec\"", "may not import"},
		{"unsafe import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"unsafe\"", "may not import"},
		{"syscall import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"syscall\"", "may not import"},
		{"plugin import", ".sdo/lib/ports/ports.go", "import (", "import (\n\t\"plugin\"", "may not import"},
		{"missing diagnoser", ".sdo/playbooks/service-targetport-mismatch/diagnose.go", "func (Program) Diagnose", "func (Program) Inspect", "exactly one Diagnoser"},
		{"invalid detector finding", ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go", `Summary: "Service targetPort has no matching container port"`, `Summary: ""`, "detector.invalid-finding"},
		{"detector binds an object Diagnose rejects", ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go", `ref := sdk.ObjectRefFrom("Service", "v1", &service)`, `ref := sdk.ObjectRefFrom("Service", "v1", &service); ref.Name = "not-observed"`, "program.diagnose-no-match"},
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

func TestProgramRepeatUnreachableIsRecordedAsGap(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The detector never fires, so no finding selects the playbook.
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "if faulty {", `if faulty && service.Name == "" {`, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("unreachable repeat must not reject memory: %v", err)
	}
	gaps, err := w.CoverageGaps()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(gaps, "\n"), "program.repeat-unreachable") {
		t.Fatalf("coverage gaps = %v, want program.repeat-unreachable", gaps)
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

func TestProgramSaveReplayProvesEffectsWithoutFixedSnapshotGate(t *testing.T) {
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
	if _, err := w.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("repair replay unnecessarily depended on fixed snapshot: %v", err)
	}
}

// Declaration validity is independent of corpus/replay availability and must
// remain a save-time check when replay results are informational.
func TestProgramDeclarationsCheckedWithoutReplay(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/diagnose.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(string(data), "[]program.ParamSpec{", "[]program.ParamSpec{{Name: \"recorded-only\", ValueKind: program.IntValue, Required: true},", 1)
	if source == string(data) {
		t.Fatal("missing synthetic declaration anchor")
	}
	if err := os.WriteFile(path, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := generatePrograms(w.Directory, w.ModulePath, []programvalidation.Spec{{ID: "service-targetport-mismatch", Diagnoser: "Program", Mitigator: "Program"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.runGo(context.Background(), "mod", "tidy"); err != nil {
		t.Fatal(err)
	}
	err = w.runGo(context.Background(), "test", "./generated", "-run", "^TestProgramRegistrationContracts$")
	if err == nil || !strings.Contains(err.Error(), "required scalar parameter") {
		t.Fatalf("declaration bypassed save-time validation without replay: %v", err)
	}
}
