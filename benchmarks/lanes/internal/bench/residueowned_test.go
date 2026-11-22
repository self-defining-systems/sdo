//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A workload that installs its own operator at every row start (and removes it at the next) leaves that operator's
// cluster-scoped objects behind at the end of every row. They are not residue: the workload's reset removes them. It
// says so through `app-reset owned` (docs/contract.md), and nothing else is excused.

// residueLane is a runner whose lane baseline holds baseline and whose cluster now holds now.
func residueLane(t *testing.T, baseline, now []string, wl string) *stepRunner {
	t.Helper()
	r := newRunner(t, presentCluster)
	state := t.TempDir()
	t.Setenv("FAKE_STATE", state)
	t.Setenv("PATH", testdata("fakebin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(state, "cluster-objects"), []byte(strings.Join(now, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(r.s.LaneDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.s.LaneDir, "cluster-objects.txt"), []byte(strings.Join(baseline, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r.wl = adapter(t, wl)
	return r
}

const (
	opNamespace = "namespace/app-sdo"
	opRole      = "clusterrole.rbac.authorization.k8s.io/sdo-admin-app-sdo"
	opBinding   = "clusterrolebinding.rbac.authorization.k8s.io/sdo-admin-app-sdo"
	webhook     = "validatingwebhookconfiguration.admissionregistration.k8s.io/blocker"
)

func TestResidueExcusesWhatTheWorkloadSaysItRecreatesEveryRow(t *testing.T) {
	r := residueLane(t, []string{"namespace/kube-system"}, []string{"namespace/kube-system", opNamespace, opRole, opBinding},
		`[ "$WORKLOAD_VERB/$WORKLOAD_ACTION" = app-reset/owned ] || exit 64
echo '{"objects":["`+opNamespace+`","`+opRole+`","`+opBinding+`"]}'`)
	extra, err := r.residue(context.Background(), &Prepared{Namespace: "app"})
	if err != nil || len(extra) != 0 {
		t.Fatalf("the workload's own objects are residue: %v, %v", extra, err)
	}
}

func TestResidueStillReportsAFaultsObjectBesideTheWorkloadsOwn(t *testing.T) {
	r := residueLane(t, []string{"namespace/kube-system"}, []string{"namespace/kube-system", opNamespace, opRole, webhook},
		`echo '{"objects":["`+opNamespace+`","`+opRole+`"]}'`)
	extra, err := r.residue(context.Background(), &Prepared{Namespace: "app"})
	if err != nil || len(extra) != 1 || extra[0] != webhook {
		t.Fatalf("want only the webhook, got %v, %v", extra, err)
	}
}

// A workload that answers nothing about it (unsupported, the built-in one) is judged as before: every addition counts.
func TestResidueWithAnUnsupportedOwnedVerbCountsEveryAddition(t *testing.T) {
	r := residueLane(t, []string{"namespace/kube-system"}, []string{"namespace/kube-system", opNamespace}, `exit 64`)
	extra, err := r.residue(context.Background(), &Prepared{Namespace: "app"})
	if err != nil || len(extra) != 1 || extra[0] != opNamespace {
		t.Fatalf("want the namespace, got %v, %v", extra, err)
	}
}

// An answer that cannot be read leaves the check unable to say: the row's residue is "unchecked", never a pass.
func TestResidueWithABrokenOwnedAnswerIsUnchecked(t *testing.T) {
	for _, script := range []string{`echo not-json`, `echo boom >&2; exit 1`} {
		r := residueLane(t, []string{"namespace/kube-system"}, []string{"namespace/kube-system", opNamespace}, script)
		r.checkResidue(context.Background(), &Prepared{Namespace: "app"})
		if len(r.res.Residue) != 1 || !strings.HasPrefix(r.res.Residue[0], "unchecked:") {
			t.Fatalf("%q: residue %v", script, r.res.Residue)
		}
	}
}
