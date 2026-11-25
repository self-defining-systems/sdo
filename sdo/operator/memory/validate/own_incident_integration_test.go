//go:build integration

package validate

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/sdktest"
)

const ownReadme = "---\nschema_version: 1\nowner: responder\nfault_class: synthetic\noriginating_incident: synthetic-incident\ndescription: A synthetic playbook.\n---\n\nUse when the synthetic fault is present.\n"

func ownDiagnose(result string) string {
	return "package fresh\n\nimport (\n\t\"context\"\n\n\t\"sdo.dev/operator/sdk/program\"\n)\n\ntype Program struct{}\n\nfunc (Program) Spec() program.ProgramSpec { return program.ProgramSpec{ID: \"fresh\"} }\n\nfunc (Program) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {\n\treturn program.Verdict{Result: program." + result + "}, nil\n}\n"
}

func ownIncidentRoot(t *testing.T, diagnose string) (root, capture string) {
	t.Helper()
	root = compileTree(t)
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	dir := filepath.Join(root, ".sdo", "playbooks", "fresh")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"README.md": ownReadme, "diagnose.go": diagnose} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	capture = t.TempDir()
	snapshot, err := json.Marshal(sdktest.Snapshot{NamespaceName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"open@detect.json": snapshot, "open@detect-recordings.json": []byte("[]")} {
		if err := os.WriteFile(filepath.Join(capture, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, capture
}

// A new playbook whose Diagnose returns NoMatch on its own incident is refused
// with a diagnostic naming it; one that matches passes and is recorded as a
// match. Both run through the same compile the broker's subprocess runs.
func TestANewPlaybookThatDoesNotMatchItsOwnIncidentIsRefused(t *testing.T) {
	root, capture := ownIncidentRoot(t, ownDiagnose("NoMatch"))
	request := compileRequest(root)
	request.OwnIncidentDirectory = capture
	diagnostics := diagnose(t, request)
	found := false
	for _, d := range diagnostics {
		found = found || d.Rule == RuleOwnIncidentReplay && d.Path == ".sdo/playbooks/fresh" && strings.Contains(d.Message, "NoMatch")
	}
	if !found {
		t.Fatalf("diagnostics = %v, want %s on the playbook", diagnostics, RuleOwnIncidentReplay)
	}
}

func TestANewPlaybookThatMatchesItsOwnIncidentIsRecordedAsAMatch(t *testing.T) {
	root, capture := ownIncidentRoot(t, ownDiagnose("Match"))
	request := compileRequest(root)
	request.OwnIncidentDirectory = capture
	var gaps []Diagnostic
	request.CoverageGaps = &gaps
	if diagnostics := diagnose(t, request); len(diagnostics) != 0 {
		t.Fatalf("a matching playbook was refused: %v", diagnostics)
	}
	found := false
	for _, gap := range gaps {
		found = found || gap.Rule == RuleOwnIncidentReplay && strings.HasPrefix(gap.Message, "match:")
	}
	if !found {
		t.Fatalf("gaps = %v, want a match note", gaps)
	}
}

func TestRecoveredDriftIsAnInformationalNote(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		want         bool
	}{
		{"false drift matches healthy object", "", true},
		{"correct diagnose ignores healthy object", "fault-", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.Replace(ownDiagnose("Match"), `"sdo.dev/operator/sdk/program"`, `"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"`, 1)
			source = strings.Replace(source, "context.Context, program.ReadView, program.Bindings", "_ context.Context, view program.ReadView, _ program.Bindings", 1)
			source = strings.Replace(source, "return program.Verdict{Result: program.Match}, nil", `
	for _, cm := range view.Pods() {
		if strings.HasPrefix(cm.Name, "`+tc.prefix+`") {
			ref := sdk.ObjectRef{Kind: "Pod", Namespace: cm.Namespace, Name: cm.Name}
			return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "drift"}}}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil`, 1)
			root, capture := ownIncidentRoot(t, source)
			for file, name := range map[string]string{"open@detect.json": "fault-config", "fixed.json": "healthy-config"} {
				raw := `{"namespace":"app","pods":[{"metadata":{"name":"` + name + `","namespace":"app"}}]}`
				if err := os.WriteFile(filepath.Join(capture, file), []byte(raw), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			request := compileRequest(root)
			request.OwnIncidentDirectory = capture
			var gaps []Diagnostic
			request.CoverageGaps = &gaps
			if diagnostics := diagnose(t, request); len(diagnostics) != 0 {
				t.Fatalf("recovered check refused the playbook: %v", diagnostics)
			}
			var count int
			for _, gap := range gaps {
				if gap.Rule == RuleMatchesRecovered {
					count++
					if !strings.Contains(gap.Message, "Pod/app/healthy-config") {
						t.Fatalf("recovered note does not name the object: %v", gap)
					}
				}
			}
			if (count == 1) != tc.want || count > 1 {
				t.Fatalf("recovered notes = %d, want one: %v. All notes: %v", count, tc.want, gaps)
			}
		})
	}
}
