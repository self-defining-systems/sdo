package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc"
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
// the patch series digest moves, a *PatchSetChangedError and not a renderer
// change or drift.
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
	err = verifyRendered(ctx, rendered.ObjectStore{Dir: root}, tree, renderenv.For(nil, depNamespace, render.SecretsOmit), deployer.Renderers, receipt, nil)
	var changed *PatchSetChangedError
	if !errors.As(err, &changed) || changed.What != "patch series digest" || changed.Want != receipt.PatchesDigest {
		t.Fatalf("err = %v, want a patch series PatchSetChangedError", err)
	}
	var renderer *RendererChangedError
	if errors.As(err, &renderer) {
		t.Fatal("a changed patch set is not a renderer change")
	}
}

func TestVerifyAcceptedRequiresTheRecordedEffectiveTree(t *testing.T) {
	root, source := depRawFixture(t)
	patchWeb(t, root)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.EffectiveTree == receipt.BaseTree || receipt.ObservedRevision != receipt.EffectiveTree {
		t.Fatalf("a patched tree deploys under the effective tree ID: %+v", receipt)
	}
	commit := depHead(t, root)
	t.Run("another effective tree with the same patch digest", func(t *testing.T) {
		changed := receipt
		changed.EffectiveTree = strings.Repeat("3", 40)
		changed.ObservedRevision = changed.EffectiveTree
		_, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed)
		var patchErr *PatchSetChangedError
		if !errors.As(err, &patchErr) || patchErr.What != "effective tree" {
			t.Fatalf("err = %v, want an effective tree PatchSetChangedError", err)
		}
	})
	t.Run("a revision that is not the effective tree", func(t *testing.T) {
		changed := receipt
		changed.ObservedRevision = receipt.BaseTree
		if _, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed); err == nil {
			t.Fatal("evidence observed at the base tree stamp was accepted")
		}
	})
	t.Run("patches that no longer apply", func(t *testing.T) {
		// A later upstream-style change moves the patched lines.
		file := filepath.Join(root, "k8s", "web.yaml")
		body, _ := os.ReadFile(file)
		if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(body), "web:1", "web:9")), 0o644); err != nil {
			t.Fatal(err)
		}
		testutil.Git(t, root, "commit", "-qam", "upstream moves")
		tree := testutil.Git(t, root, "rev-parse", "HEAD^{tree}")
		err := verifyRendered(ctx, rendered.ObjectStore{Dir: root}, tree, renderenv.For(nil, depNamespace, render.SecretsOmit), deployer.Renderers, receipt, nil)
		var patchErr *PatchSetChangedError
		if !errors.As(err, &patchErr) || patchErr.What != "patch application" {
			t.Fatalf("err = %v, want a patch application PatchSetChangedError", err)
		}
	})
}

// The pod-template stamp is the effective tree ID, so a memory-only commit
// does not change what a redeploy applies.
func TestStampIsTheEffectiveTreeID(t *testing.T) {
	root, source := depRawFixture(t)
	patchWeb(t, root)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range cluster.applied {
		if document["kind"] != "Deployment" {
			continue
		}
		meta := document["metadata"].(map[string]any)["annotations"].(map[string]any)
		templateMeta, _ := document["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
		template, _ := templateMeta["annotations"].(map[string]any)
		if _, stamped := template["sdo.dev/source-tree"]; stamped {
			t.Fatalf("pod template carries the tree stamp, so every advance would roll it: %v", template)
		}
		if meta["sdo.dev/source-tree"] != receipt.EffectiveTree || receipt.EffectiveTree == receipt.BaseTree {
			t.Fatalf("stamp %v, effective %s, base %s", meta, receipt.EffectiveTree, receipt.BaseTree)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, ".sdo", "playbooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo", "playbooks", "p.md"), []byte("memory"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "memory only")
	again, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil || again.EffectiveTree != receipt.EffectiveTree || again.BaseTree == receipt.BaseTree {
		t.Fatalf("memory-only commit: effective %s -> %s, base %s -> %s, %v", receipt.EffectiveTree, again.EffectiveTree, receipt.BaseTree, again.BaseTree, err)
	}
}

// Image builds read a materialization of the effective tree, never the
// agent's checkout: patched content, no memory, and a directory that stays
// the same when the checkout is edited afterwards.
func TestImageBuildsReadTheMaterializedEffectiveTree(t *testing.T) {
	root, source := depRawFixture(t)
	patchWeb(t, root)
	deployer, fake := depDeployer(t, root, &clusterFake{})
	deployer.Materializer = &rendered.Materializer{Root: filepath.Join(t.TempDir(), "effective")}
	var checkouts []string
	fake.Hook = func(_ context.Context, req imagesrc.Request) error {
		checkouts = append(checkouts, req.Checkout)
		body, err := os.ReadFile(filepath.Join(req.Checkout, "k8s", "web.yaml"))
		if err != nil || !strings.Contains(string(body), "web:2") {
			return errors.New("build input does not carry the patch: " + string(body))
		}
		if _, err := os.Stat(filepath.Join(req.Checkout, ".sdo", "deployment", "patches")); err == nil {
			return errors.New("build input carries the patch series")
		}
		// An edit of the agent's checkout during the build does not reach it.
		return os.WriteFile(filepath.Join(root, "k8s", "web.yaml"), []byte("scratch edit"), 0o644)
	}
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if len(checkouts) == 0 || checkouts[0] == root || filepath.Base(checkouts[0]) != receipt.EffectiveTree[:12] {
		t.Fatalf("checkouts = %v, effective %s", checkouts, receipt.EffectiveTree)
	}
}
