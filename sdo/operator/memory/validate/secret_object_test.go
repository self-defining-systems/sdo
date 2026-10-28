package validate

import (
	"context"
	"testing"
)

func secretDataRules(t *testing.T, root string) bool {
	t.Helper()
	found := []bool{false, false}
	for i, check := range []func() ([]Diagnostic, error){
		func() ([]Diagnostic, error) {
			return Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
		},
		func() ([]Diagnostic, error) {
			return CheckShell(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase, CodePath: "shell"})
		},
	} {
		ds, err := check()
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range ds {
			found[i] = found[i] || d.Rule == RuleSecretData
		}
	}
	if found[0] != found[1] {
		t.Fatalf("Go and shell checks disagree on Secret data: %v", found)
	}
	return found[0]
}

// Memory is pushed to a shared remote, so no path or format may carry a
// Secret's data, including captured corpus cases. References stay allowed.
func TestCheckRejectsSecretDataAnywhereInMemory(t *testing.T) {
	documents := map[string]string{
		"object.json":     `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db"},"data":{"password":"c3ludGhldGlj"}}` + "\n",
		"list.yaml":       "items:\n  - kind: Secret\n    metadata:\n      name: db\n    stringData:\n      password: synthetic\n",
		"collection.json": `{"namespace":"demo","secrets":[{"metadata":{"name":"db"},"data":{"token":"c3ludGhldGlj"}}]}` + "\n",
	}
	for _, dir := range []string{".sdo/corpus/synthetic-case/", ".sdo/diagnostics/detectors/incidents/cache/", ".sdo/playbooks/base/"} {
		for name, content := range documents {
			t.Run(dir+name, func(t *testing.T) {
				root := goodTree(t)
				commit(t, root, "base")
				write(t, root, dir+name, content)
				if !secretDataRules(t, root) {
					t.Fatalf("Secret data in %s%s accepted", dir, name)
				}
			})
		}
	}
	for name, content := range map[string]string{
		"reference.json": `{"related":[{"apiVersion":"v1","kind":"Secret","name":"db","namespace":"demo"}]}` + "\n",
		"empty.json":     `{"namespace":"demo","secrets":[]}` + "\n",
	} {
		t.Run("allowed/"+name, func(t *testing.T) {
			root := goodTree(t)
			commit(t, root, "base")
			write(t, root, ".sdo/diagnostics/detectors/incidents/cache/"+name, content)
			if secretDataRules(t, root) {
				t.Fatalf("Secret reference in %s rejected", name)
			}
		})
	}
}
