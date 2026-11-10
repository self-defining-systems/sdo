//go:build integration

package memory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/patchset"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestCaptureSourceProposalsWritesAResponderPatch(t *testing.T) {
	root := t.TempDir()
	acceptedGit(t, root, "init", "-q")
	acceptedGit(t, root, "config", "user.email", "test@example.invalid")
	acceptedGit(t, root, "config", "user.name", "test")
	for name, text := range map[string]string{
		"k8s/web.yaml": proposalManifest,
		".sdo/arch.md": "# Manifest inventory\n\n- Deployment/shop/web (source: k8s/web.yaml)\n- Deployment/shop/chart (source: helm)\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	acceptedGit(t, root, "add", "-A")
	acceptedGit(t, root, "commit", "-q", "-m", "accepted")
	reason := "web saturates at one replica under the observed load; keep the replicas the playbook scaled to"
	created, skipped, err := CaptureSourceProposals(context.Background(), root, "inc-1", []program.SourceChangeProposal{
		{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "web"}, Path: "spec.replicas", Value: float64(3), Reason: reason},
		{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "chart"}, Path: "spec.replicas", Value: float64(3), Reason: reason},
	})
	if err != nil || len(created) != 1 || len(skipped) != 1 {
		t.Fatalf("created %v skipped %v: %v", created, skipped, err)
	}
	patch, err := os.ReadFile(filepath.Join(root, patchset.Directory, created[0]))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Owner: responder", "Incident: inc-1", "+  replicas: 3"} {
		if !strings.Contains(string(patch), want) {
			t.Fatalf("patch lacks %q:\n%s", want, patch)
		}
	}
	report, err := patchset.Check(context.Background(), root)
	if err != nil || len(report.Uncaptured) != 0 {
		t.Fatalf("check = %+v, %v", report, err)
	}
}
