package validate

import (
<<<<<<< HEAD
=======
	"context"
>>>>>>> origin/vic-lsh/dev/sdo-improve
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// compileTree is a repository whose only memory is the committed example
// detector package, which builds against the SDK and passes its own test.
func compileTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo")
	copyTree(t, fixture, filepath.Join(root, ".sdo"))
	return root
}

func compileRequest(root string) Request {
	return Request{Root: root, Layers: Compile, OperatorRoot: filepath.Join("..", "..")}
}

const detectorSource = ".sdo/diagnostics/detectors/incidents/missing-configmap/detector.go"

func TestCompileCachedVetLocationsSurviveWorkspaceRelocation(t *testing.T) {
	t.Setenv("GOFLAGS", os.Getenv("GOFLAGS")+" -trimpath")
	var first []Diagnostic
	for attempt := 0; attempt < 2; attempt++ {
		root := compileTree(t)
		edit(t, root, detectorSource, "return Detector{}", `_ = fmt.Sprintf("%d", "not a number")
	return Detector{}`)
		diagnostics := diagnose(t, compileRequest(root))
		if len(diagnostics) != 1 || diagnostics[0].Rule != RuleDetectorVet || diagnostics[0].Path != detectorSource || diagnostics[0].Line == 0 {
			t.Fatalf("attempt %d: unlocated vet diagnostic: %+v", attempt, diagnostics)
		}
		if attempt == 0 {
			first = diagnostics
		} else if !reflect.DeepEqual(first, diagnostics) {
			t.Fatalf("same source in isolated workspaces changed diagnostics: first %+v, second %+v", first, diagnostics)
		}
	}
}

func TestCompileAcceptsDetectorsThatBuildVetAndPass(t *testing.T) {
	if got := diagnose(t, compileRequest(compileTree(t))); len(got) != 0 {
		t.Fatalf("valid detector rejected: %v", got)
	}
}

func TestCompileReportsTheFailingStage(t *testing.T) {
	for _, test := range []struct {
		name   string
		rule   string
		mutate func(t *testing.T, root string)
		path   string
	}{
		{"build", RuleDetectorCompile, func(t *testing.T, root string) {
			edit(t, root, detectorSource, "return Detector{}", "return undefinedValue")
		}, detectorSource},
		{"vet", RuleDetectorVet, func(t *testing.T, root string) {
			edit(t, root, detectorSource, "return Detector{}", `_ = fmt.Sprintf("%d", "not a number")
	return Detector{}`)
		}, detectorSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := compileTree(t)
			if test.name == "vet" {
				// This contract asserts fresh source positions, not cached vet
				// stderr from an identical fixture in an older temp directory.
				// Keep dependencies cacheable while making this source unique.
				path := filepath.Join(root, detectorSource)
				source, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				source = append(source, []byte("\n// diagnostic fixture: "+root+"\n")...)
				if err := os.WriteFile(path, source, 0644); err != nil {
					t.Fatal(err)
				}
			}
			test.mutate(t, root)
			diagnostics := diagnose(t, compileRequest(root))
			if got := ruleSet(diagnostics); !same(got, []string{test.rule}) {
				t.Fatalf("rules = %v, want %s\n%v", got, test.rule, diagnostics)
			}
			if test.path != "" && (diagnostics[0].Path != test.path || diagnostics[0].Line == 0) {
				t.Fatalf("compiler error is not located: %+v", diagnostics[0])
			}
		})
	}
}

func TestCompileRunsOnlyAfterStaticLayersPass(t *testing.T) {
	root := compileTree(t)
	write(t, root, ".sdo/notes.txt", "stray\n")
	edit(t, root, detectorSource, "return Detector{}", "return undefinedValue")
	got := ruleSet(diagnose(t, Request{Root: root, Layers: Structural | Compile, OperatorRoot: filepath.Join("..", "..")}))
	for _, rule := range got {
		if strings.HasPrefix(rule, "detector.") {
			t.Fatalf("rules = %v; compile must not pile onto static findings", got)
		}
	}
	if len(got) == 0 {
		t.Fatal("the stray file was not reported")
	}
}

func TestCompileSkipsRepositoriesWithoutDetectors(t *testing.T) {
	if got := diagnose(t, compileRequest(t.TempDir())); len(got) != 0 {
		t.Fatalf("unbootstrapped repository rejected: %v", got)
	}
}

<<<<<<< HEAD
// Reporting does not need to rebuild Kubernetes/controller dependencies for
// each Go failure stage. Real compiler fixtures still run in nightly CI.
func TestCompileStageDiagnostics(t *testing.T) {
	for _, stage := range []struct{ name, rule string }{
		{"build", RuleDetectorCompile}, {"vet", RuleDetectorVet}, {"test", RuleDetectorTest},
	} {
		t.Run(stage.name, func(t *testing.T) {
			bin := t.TempDir()
			script := "#!/bin/sh\nif [ \"$1\" = \"" + stage.name + "\" ]; then\n printf '%s\\n' 'detectors/incidents/missing-configmap/detector.go:17: synthetic stage failure'\n exit 1\nfi\nexit 0\n"
			if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin)
			got := diagnose(t, compileRequest(compileTree(t)))
			if len(got) != 1 || got[0].Rule != stage.rule || got[0].Path != detectorSource || got[0].Line != 17 {
				t.Fatalf("%s diagnostic: %+v", stage.name, got)
=======
func TestMissingHealthyReplayInputIsInformational(t *testing.T) {
	for _, mode := range []string{"missing", "empty", "file"} {
		t.Run(mode, func(t *testing.T) {
			root := compileTree(t)
			baseline := filepath.Join(t.TempDir(), "baseline")
			if mode == "empty" {
				if err := os.MkdirAll(baseline, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "file" {
				if err := os.WriteFile(baseline, []byte("no snapshot directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var observations []Diagnostic
			request := compileRequest(root)
			request.HealthyBaseline = baseline
			request.CoverageGaps = &observations
			if ds, err := CompileCandidate(context.Background(), request); err != nil || len(ds) != 0 {
				t.Fatalf("missing replay input blocked saving: %v %v", ds, err)
			}
			found := false
			for _, note := range observations {
				found = found || note.Rule == RuleReplayObservation && strings.Contains(note.Message, "healthy replay unverified")
			}
			if !found {
				t.Fatalf("missing replay input was hidden: %v", observations)
>>>>>>> origin/vic-lsh/dev/sdo-improve
			}
		})
	}
}
