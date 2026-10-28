package validate

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// An index authored by several reflections (id links plus titled duplicates)
// is regenerated to one line per active playbook, and the result validates.
func TestNormalizeAuthoredPlaybooksRegeneratesDuplicatedIndex(t *testing.T) {
	root := goodTree(t)
	write(t, root, ".sdo/playbooks/second/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: second\noriginating_incident: synthetic\n---\n# Second fault\n\nRepair <RESOURCE_NAME>.\n")
	write(t, root, layout.PlaybookIndexFile, "# Playbooks\n\n- [base](base/README.md)\n- [second](second/README.md)\n- [Base repair](base/README.md)\n- [Second fault](second/README.md)\n")
	if got := ruleSet(diagnose(t, Request{Root: root, Layers: Structural | Semantic})); !same(got, []string{RuleIndexGenerated}) {
		t.Fatalf("duplicated index rules = %v", got)
	}
	changed, err := NormalizeAuthoredPlaybooks(root, []string{".sdo/playbooks/second/README.md"})
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0] != layout.PlaybookIndexFile {
		t.Fatalf("changed = %v", changed)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(layout.PlaybookIndexFile)))
	if err != nil {
		t.Fatal(err)
	}
	if want := "# Playbooks\n\n- [base](base/README.md)\n- [Second fault](second/README.md)\n"; string(data) != want {
		t.Fatalf("index = %q, want %q", data, want)
	}
	if diagnostics := diagnose(t, Request{Root: root, Layers: Structural | Semantic}); len(diagnostics) != 0 {
		t.Fatalf("regenerated index does not validate: %v", diagnostics)
	}
}

// Agent edits to the index alone are replaced by the generated form.
func TestNormalizeAuthoredPlaybooksDropsIndexEdits(t *testing.T) {
	root := goodTree(t)
	write(t, root, layout.PlaybookIndexFile, "# My playbooks\n\nSee [base](base/README.md) first.\n")
	if _, err := NormalizeAuthoredPlaybooks(root, []string{layout.PlaybookIndexFile}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(layout.PlaybookIndexFile)))
	if err != nil {
		t.Fatal(err)
	}
	if want := "# Playbooks\n\n- [base](base/README.md)\n"; string(data) != want {
		t.Fatalf("index = %q, want %q", data, want)
	}
}
