package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// patchWeb commits a patch that moves the web image tag, authored from a real
// diff and then restored, as a capture tool would write it.
func patchWeb(t *testing.T, root string) {
	t.Helper()
	file := filepath.Join(root, "k8s", "web.yaml")
	original, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(original), "web:1", "web:2")), 0o644); err != nil {
		t.Fatal(err)
	}
	diff := testutil.Git(t, root, "diff", "--no-color", "--full-index", "--no-ext-diff") + "\n"
	testutil.Git(t, root, "checkout", "--", "k8s/web.yaml")
	text := "SDO-Patch: 1\nOwner: deployer\nReason: |\n  The vendored workload must use the web:2 tag to start against the local registry.\n\n" + diff
	target := filepath.Join(root, patchset.Directory, "0001-web-tag.patch")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "patch")
}

// Deploy and restart verification render the effective tree: a patch changes
// what is resolved and applied, and the digest recorded with it re-verifies.
func TestDeployAndVerifyRenderTheEffectiveTree(t *testing.T) {
	root, source := depRawFixture(t)
	patchWeb(t, root)
	deployer, fake := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if calls := strings.Join(fake.Calls(), ","); calls != "busybox:1,web:2" {
		t.Fatalf("images resolved from the patched source: %s", calls)
	}
	if _, err := deployer.VerifyAccepted(ctx, depHead(t, root), depNamespace, receipt); err != nil {
		t.Fatalf("the recorded digests must re-verify through the same effective tree: %v", err)
	}
}

// A receipt made without the patch does not verify a tree that carries one:
// the render digest moves, which is a RendererChangedError and not drift.
func TestVerifyRenderedSeesAPatchTheReceiptDidNotHave(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	patchWeb(t, root)
	tree := testutil.Git(t, root, "rev-parse", "HEAD^{tree}")
	store := rendered.ObjectStore{Dir: root}
	err = verifyRendered(ctx, store, tree, renderenv.For(nil, depNamespace, render.SecretsOmit), deployer.Renderers, receipt)
	var changed *RendererChangedError
	if !errors.As(err, &changed) || changed.What != "render" {
		t.Fatalf("err = %v, want a render RendererChangedError", err)
	}
}
