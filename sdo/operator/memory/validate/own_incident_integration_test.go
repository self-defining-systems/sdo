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

// A program whose plan the host would refuse is refused by the compile the broker's subprocess runs, on a capture that
// did not record the application map: the map is the repository's. The statement is computed, so only the plan says it.
func TestANewPlaybookWhosePlanTheHostRefusesIsRefused(t *testing.T) {
	root, capture := ownIncidentRoot(t, ownDiagnose("Match"))
	write(t, root, ".sdo/arch.md", mapInventory+"\n"+mapRegion(mapYAML))
	write(t, root, ".sdo/playbooks/fresh/mitigate.go", `package fresh

import (
	"context"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	if _, err := v.AppMap(); err != nil {
		return program.Plan{}, err
	}
	target := program.StoreTarget{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"}
	return program.Plan{Actions: []program.Action{program.RepairData{Target: target, Scope: program.DataScope{Collection: "items"}, Statement: "a = 1 " + strings.Repeat("-", 2), ExpectMin: 1, ExpectMax: 1}}}, nil
}
`)
	request := compileRequest(root)
	request.OwnIncidentDirectory = capture
	diagnostics := diagnose(t, request)
	for _, d := range diagnostics {
		if strings.Contains(d.Message, "program.plan-invalid") {
			return
		}
	}
	t.Fatalf("diagnostics = %v, want a refused plan", diagnostics)
}
