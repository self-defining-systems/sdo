package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestGoodTreePassesEveryStaticLayer(t *testing.T) {
	root := goodTree(t)
	if diagnostics := diagnose(t, Request{Root: root, Layers: Structural | Semantic}); len(diagnostics) != 0 {
		t.Fatalf("good tree rejected: %v", diagnostics)
	}
	commit(t, root, "base")
	if diagnostics := diagnose(t, Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Layers: Static}); len(diagnostics) != 0 {
		t.Fatalf("unchanged good tree rejected: %v", diagnostics)
	}
}

// Each case overlays testdata/bad/<name> on the good tree and must produce
// exactly the listed rules, so a rule that stops firing or starts cascading is
// noticed.
func TestStructuralAndSemanticRules(t *testing.T) {
	for _, test := range []struct {
		name string
		want []string
		path string
		line int
	}{
		{"unknown-path", []string{RuleUnknownPath}, ".sdo/notes.txt", 0},
		{"invalid-id", []string{RuleInvalidID}, ".sdo/playbooks/Bad_ID/README.md", 0},
		{"secret-path", []string{RuleSecretPath}, ".sdo/diagnostics/traffic/generators/secret-token.go", 0},
		{"legacy-manifest", []string{RuleLegacyManifest}, ".sdo/diagnostics/manifest.yaml", 0},
		{"schema-version", []string{RuleSchemaVersion}, ".sdo/schema-version", 1},
		{"yaml-parse", []string{RuleYAMLParse}, ".sdo/diagnostics/traffic/workloads/health.yaml", 2},
		{"go-syntax", []string{RuleGoSyntax}, ".sdo/diagnostics/detectors/incidents/cache/detector.go", 2},
		{"document-parse", []string{RuleDocumentParse}, ".sdo/goal.md", 0},
		{"manifest-parse", []string{RuleManifestParse}, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", 0},
		{"manifest-invalid", []string{RuleManifestInvalid}, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", 0},
		{"manifest-location", []string{RuleManifestLocation}, ".sdo/diagnostics/detectors/health/health/manifest.yaml", 0},
		{"goal-missing", []string{RuleGoalMissing}, ".sdo/goal.md", 0},
		{"architecture-missing", []string{RuleArchitectureMissing}, ".sdo/arch.md", 0},
		{"goal-owner", []string{RuleGoalOwner}, ".sdo/goal.md", 0},
		{"manifest-missing", []string{RuleManifestMissing}, ".sdo/diagnostics/detectors/incidents/orphan", 0},
		{"detector-none", []string{RuleDetectorNone}, ".sdo/diagnostics/detectors", 0},
		{"detector-no-source", []string{RuleDetectorNoSource}, ".sdo/diagnostics/detectors/incidents/cache", 0},
		{"duplicate-id", []string{RuleDuplicateID}, ".sdo/diagnostics/detectors/incidents/health/manifest.yaml", 0},
		{"unknown-playbook", []string{RuleUnknownPlaybook}, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", 20},
		{"playbook-none", []string{RuleDetectorNone, RulePlaybookNone}, ".sdo/playbooks", 0},
		{"playbook-missing-readme", []string{RulePlaybookNoReadme}, ".sdo/playbooks/extra", 0},
		{"playbook-owner", []string{RulePlaybookOwner}, ".sdo/playbooks/base/README.md", 0},
		{"playbook-originating-incident", []string{RulePlaybookIncident}, ".sdo/playbooks/base/README.md", 0},
		{"index-missing", []string{RuleIndexMissing}, ".sdo/playbooks/README.md", 0},
		{"index-link", []string{RuleIndexLink}, ".sdo/playbooks/README.md", 4},
		{"script-unreferenced", []string{RuleScriptUnreferenced}, ".sdo/playbooks/base/scripts/other.sh", 0},
		{"script-syntax", []string{RuleScriptSyntax}, ".sdo/playbooks/base/scripts/fix.sh", 0},
		{"physical-path", []string{RulePhysicalPath}, ".sdo/playbooks/base/README.md", 7},
		{"detector-physical-path", []string{RulePhysicalPath}, ".sdo/diagnostics/detectors/incidents/cache/detector.go", 3},
		{"manifest-playbook-path", []string{RuleManifestInvalid}, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := goodTree(t)
			applyCase(t, root, test.name)
			diagnostics := diagnose(t, Request{Root: root, Layers: Structural | Semantic})
			if got := ruleSet(diagnostics); !same(got, test.want) {
				t.Fatalf("rules = %v, want %v\n%v", got, test.want, diagnostics)
			}
			found := false
			for _, d := range diagnostics {
				if d.Rule == test.want[len(test.want)-1] && d.Path == test.path && (test.line == 0 || d.Line == test.line) {
					found = true
				}
				if d.Message == "" {
					t.Errorf("diagnostic without a message: %+v", d)
				}
			}
			if !found {
				t.Fatalf("no %s diagnostic at %s:%d: %v", test.want[len(test.want)-1], test.path, test.line, diagnostics)
			}
		})
	}
}

func TestFileSystemShapeRules(t *testing.T) {
	for _, test := range []struct {
		name   string
		rule   string
		mutate func(t *testing.T, root string)
	}{
		{"symlink", RuleSymlink, func(t *testing.T, root string) {
			if err := os.Symlink("/etc/passwd", filepath.Join(root, ".sdo", "link.md")); err != nil {
				t.Fatal(err)
			}
		}},
		{"not-regular", RuleNotRegular, func(t *testing.T, root string) {
			if err := syscall.Mkfifo(filepath.Join(root, ".sdo", "goal.pipe"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"too-large", RuleTooLarge, func(t *testing.T, root string) {
			write(t, root, ".sdo/playbooks/base/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: i\n---\n<RESOURCE_NAME> "+strings.Repeat("x", 300*1024)+"\n")
		}},
		{"script-not-executable", RuleScriptNotExecutable, func(t *testing.T, root string) {
			if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := goodTree(t)
			test.mutate(t, root)
			got := ruleSet(diagnose(t, Request{Root: root, Layers: Structural | Semantic}))
			if test.rule == RuleSymlink || test.rule == RuleNotRegular {
				// The extra path is also outside the layout.
				got = removeRule(got, RuleUnknownPath)
			}
			if !same(got, []string{test.rule}) {
				t.Fatalf("rules = %v, want only %s", got, test.rule)
			}
		})
	}
}

func removeRule(rules []string, drop string) []string {
	kept := []string{}
	for _, rule := range rules {
		if rule != drop {
			kept = append(kept, rule)
		}
	}
	return kept
}

func TestPartialMemoryIsAcceptedOnlyDuringBootstrapStages(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\nGoal\n")
	commit(t, root, "base")
	for _, test := range []struct {
		actor Actor
		phase Phase
		want  []string
	}{
		{Human, InitialPhase, nil},
		{Deployer, InitialPhase, nil},
		{HealthJudge, InitialPhase, []string{RuleArchitectureMissing, RuleDetectorNone, RulePlaybookNone, RuleSchemaVersion}},
		{Responder, ReflectionPhase, []string{RuleArchitectureMissing, RuleDetectorNone, RulePlaybookNone, RuleSchemaVersion}},
	} {
		got := ruleSet(diagnose(t, Request{Root: root, Actor: test.actor, Phase: test.phase, Layers: Structural | Semantic}))
		if !same(got, test.want) {
			t.Errorf("%s/%s: rules = %v, want %v", test.actor, test.phase, got, test.want)
		}
	}
}

func TestMemoryIsValidatedOnlyWhenTheDiffTouchesIt(t *testing.T) {
	root := goodTree(t)
	remove(t, root, ".sdo/goal.md")
	commit(t, root, "broken memory already accepted")
	write(t, root, "app.txt", "repair\n")
	got := diagnose(t, Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase, Layers: Static})
	if len(got) != 0 {
		t.Fatalf("a source-only change was rejected for pre-existing memory: %v", got)
	}
}

func TestChangedSourceFilesAreSyntaxChecked(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, "service.yaml", "kind: Service\nmetadata:\n  name: a\n  name: b\n")
	write(t, root, "repair.go", "package repair\nfunc Broken( {\n")
	write(t, root, "chart.yaml", "name: {{ .Values.name }}\nname: again\n")
	diagnostics := diagnose(t, Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase, Layers: Structural})
	if got := ruleSet(diagnostics); !same(got, []string{RuleYAMLParse, RuleGoSyntax}) {
		t.Fatalf("rules = %v\n%v", got, diagnostics)
	}
	for _, d := range diagnostics {
		if d.Path == "chart.yaml" {
			t.Fatalf("templated YAML must be skipped: %v", d)
		}
	}
}

func TestSecretPathsInTheDiffAreRejected(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, "config/.env", "TOKEN=x\n")
	got := ruleSet(diagnose(t, Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase, Layers: Structural}))
	if !same(got, []string{RuleSecretPath}) {
		t.Fatalf("rules = %v", got)
	}
}

func TestInfrastructureFailuresAreErrorsNotDiagnostics(t *testing.T) {
	root := goodTree(t)
	for name, request := range map[string]Request{
		"missing root":             {},
		"ownership without base":   {Root: root, Actor: Responder, Phase: ReflectionPhase, Layers: Ownership},
		"ownership without actor":  {Root: root, Base: "HEAD", Layers: Ownership},
		"compile without operator": {Root: root, Layers: Compile},
		"unknown layer":            {Root: root, Layers: 1 << 7},
		"base outside repository":  {Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Layers: Ownership},
	} {
		if diagnostics, err := Validate(context.Background(), request); err == nil {
			t.Errorf("%s: want an error, got diagnostics %v", name, diagnostics)
		}
	}
}

func TestDiagnosticsAreSortedAndFormatted(t *testing.T) {
	root := goodTree(t)
	applyCase(t, root, "unknown-path")
	applyCase(t, root, "go-syntax")
	diagnostics := diagnose(t, Request{Root: root, Layers: Structural})
	if len(diagnostics) != 2 || diagnostics[0].Path > diagnostics[1].Path {
		t.Fatalf("diagnostics are not sorted by path: %v", diagnostics)
	}
	err := Err(diagnostics)
	failure, ok := err.(*Failure)
	if !ok || !strings.Contains(failure.Error(), "[go.syntax]") || !strings.Contains(failure.Error(), "detector.go:2") {
		t.Fatalf("failure text = %v", err)
	}
	if Err(nil) != nil {
		t.Fatal("no diagnostics must be no error")
	}
}
