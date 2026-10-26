package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckRejectsPublicationContentGaps(t *testing.T) {
	for _, test := range []struct {
		name, rule string
		edit       func(*testing.T, string)
	}{
		{"whitespace", "diff.whitespace", func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/base/README.md", readGap(t, r, ".sdo/playbooks/base/README.md")+"\nRecovery guidance. \n")
		}},
		{"aggregate snapshot budget", "memory.snapshot-budget", func(t *testing.T, r string) {
			for _, id := range []string{"large-a", "large-b", "large-c"} {
				write(t, r, ".sdo/playbooks/"+id+"/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: incident\n---\nRepair <RESOURCE_NAME>.\n"+strings.Repeat("Verified guidance paragraph.\n", 7000))
			}
			write(t, r, ".sdo/playbooks/README.md", readGap(t, r, ".sdo/playbooks/README.md")+"\n- [large-a](large-a/README.md)\n- [large-b](large-b/README.md)\n- [large-c](large-c/README.md)\n")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := goodTree(t)
			commit(t, root, "base")
			test.edit(t, root)
			ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range ds {
				if d.Rule == test.rule {
					return
				}
			}
			t.Fatalf("missing %s: %v", test.rule, ds)
		})
	}
}

func readGap(t *testing.T, root, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCheckReportsLifecycleGates(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/playbooks/base/README.md", readGap(t, root, ".sdo/playbooks/base/README.md")+"\nNew guidance.\n")
	for _, test := range []struct {
		phase Phase
		rule  string
	}{
		{ReflectionPhase, "lifecycle.accepted-outcome"},
		{InitialPhase, "lifecycle.bootstrap-closed"},
	} {
		ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: test.phase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, d := range ds {
			found = found || d.Rule == test.rule
		}
		if !found {
			t.Errorf("missing %s: %v", test.rule, ds)
		}
	}
}

// App manifests carry data the agent did not author; whitespace inside them
// (an embedded script in an accepted ConfigMap) is bytes to keep as-is.
func TestWhitespaceRuleCoversOnlyAuthoredMemory(t *testing.T) {
	script := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: mongo-geo-script\ndata:\n  init.sh: |\n    mongo --eval 'db.x.insert({})'   \n    \n"
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, "kubernetes/geo/mongo-geo-script-configmap.yaml", script)
	ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 {
		t.Fatalf("accepted app bytes were judged: %+v", ds)
	}
}
