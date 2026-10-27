package validate

import (
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
