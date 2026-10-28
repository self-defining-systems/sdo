package validate

import (
	"context"
	"testing"

	"sdo.dev/operator/memory"
)

func clusterLocalRules(t *testing.T, root string, actor Actor) bool {
	t.Helper()
	for _, check := range []func() ([]Diagnostic, error){
		func() ([]Diagnostic, error) {
			return Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: actor, Phase: ProposalPhase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
		},
		func() ([]Diagnostic, error) {
			return CheckShell(context.Background(), Request{Root: root, Base: "HEAD", Actor: actor, Phase: ProposalPhase, CodePath: "shell"})
		},
	} {
		ds, err := check()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, d := range ds {
			found = found || d.Rule == RuleClusterLocal
		}
		if !found {
			return false
		}
	}
	return true
}

// Cluster-local values are recomputed by each cluster's controller and never
// written into shared memory. The sweep covers every field kind the inventory
// found (incarnation identity, allocated addresses, live probe times) in both
// data formats; a new kind belongs in clusterLocalKeys and in this sweep.
func TestCheckRejectsNewClusterLocalValues(t *testing.T) {
	const fixture = ".sdo/diagnostics/detectors/incidents/cache/"
	for name, edit := range map[string]func(*testing.T, string){
		"legacy reachability capture": func(t *testing.T, r string) {
			write(t, r, memory.LegacyReachabilityBaselinePath, `{"version":1,"exclusions":[]}`+"\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := goodTree(t)
			commit(t, root, "base")
			edit(t, root)
			if !clusterLocalRules(t, root, HealthJudge) {
				t.Fatal("cluster-local write accepted")
			}
		})
	}
	for _, key := range clusterLocalKeys {
		for format, content := range map[string]string{
			"capture.json": `{"service":{"metadata":{"name":"geo","` + key + `":"10.96.4.7"}}}` + "\n",
			"capture.yaml": "service:\n  items:\n    - name: geo\n      " + key + ": 10.96.4.7\n",
		} {
			t.Run(key+"/"+format, func(t *testing.T) {
				root := goodTree(t)
				commit(t, root, "base")
				write(t, root, fixture+format, content)
				if !clusterLocalRules(t, root, Responder) {
					t.Fatalf("%s in %s accepted", key, format)
				}
			})
		}
	}
}

func TestCheckKeepsSourceDeclaredAndLegacyValues(t *testing.T) {
	for name, edit := range map[string]func(*testing.T, string){
		"headless service": func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/cache/svc.yaml", "clusterIP: None\n")
		},
		"null timestamp": func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/cache/obj.yaml", "creationTimestamp: null\n")
		},
		"test fixture": func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/cache/testdata/pod.json", `{"uid":"synthetic"}`+"\n")
		},
		"legacy unchanged": func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/sh\necho fixed\n")
		},
		"legacy stripped":    func(t *testing.T, r string) { remove(t, r, memory.LegacyReachabilityBaselinePath) },
		"plain notes change": func(t *testing.T, r string) { write(t, r, ".sdo/goal.md", "# Goal\n\nServe hotel searches.\n") },
	} {
		t.Run(name, func(t *testing.T) {
			root := goodTree(t)
			write(t, root, memory.LegacyReachabilityBaselinePath, `{"version":1,"capturedAt":"2026-10-28T16:19:23Z","exclusions":[]}`+"\n")
			commit(t, root, "base with a pre-move capture")
			edit(t, root)
			for _, check := range []func() ([]Diagnostic, error){
				func() ([]Diagnostic, error) {
					return Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: ProposalPhase}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
				},
				func() ([]Diagnostic, error) {
					return CheckShell(context.Background(), Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: ProposalPhase, CodePath: "shell"})
				},
			} {
				ds, err := check()
				if err != nil {
					t.Fatal(err)
				}
				for _, d := range ds {
					if d.Rule == RuleClusterLocal {
						t.Fatalf("rejected %s: %v", name, d)
					}
				}
			}
		})
	}
}
