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
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// commitWeb2 commits a source edit that moves the web image tag, and a memory
// file, as a deployer or engineer would.
func commitWeb2(t *testing.T, root string) {
	t.Helper()
	file := filepath.Join(root, "k8s", "web.yaml")
	original, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(original), "web:1", "web:2")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".sdo", "goal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo", "goal", "note.md"), []byte("memory"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "web:2")
}

// Deploy and restart verification render the committed tree: a committed
// edit changes what is resolved and applied, and the digest recorded with it
// re-verifies.
func TestDeployAndVerifyRenderTheCommittedTree(t *testing.T) {
	root, source := depRawFixture(t)
	commitWeb2(t, root)
	deployer, fake := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if calls := strings.Join(fake.Calls(), ","); calls != "busybox:1,web:2" {
		t.Fatalf("images resolved from the committed source: %s", calls)
	}
	if _, err := deployer.VerifyAccepted(ctx, depHead(t, root), depNamespace, receipt); err != nil {
		t.Fatalf("the recorded digests must re-verify through the same effective tree: %v", err)
	}
}

// Uncommitted app files are scratch: the deployer deploys the commit.
func TestDeployIgnoresUncommittedAppFiles(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, fake := depDeployer(t, root, &clusterFake{})
	file := filepath.Join(root, "k8s", "web.yaml")
	original, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(original), "web:1", "web:7")), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb)); err != nil {
		t.Fatal(err)
	}
	if calls := strings.Join(fake.Calls(), ","); calls != "busybox:1,web:1" {
		t.Fatalf("an uncommitted edit was deployed: %s", calls)
	}
}

// A receipt made before a source commit does not verify the new tree: the
// effective tree moved, an *EffectiveTreeChangedError and not a renderer
// change or drift.
func TestVerifyRenderedSeesACommitTheReceiptDidNotHave(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	commitWeb2(t, root)
	tree := testutil.Git(t, root, "rev-parse", "HEAD^{tree}")
	err = verifyRendered(ctx, rendered.ObjectStore{Dir: root}, tree, renderenv.For(nil, depNamespace, render.SecretsOmit), deployer.Renderers, receipt, nil)
	var changed *EffectiveTreeChangedError
	if !errors.As(err, &changed) || changed.Want != receipt.EffectiveTree {
		t.Fatalf("err = %v, want an EffectiveTreeChangedError", err)
	}
	var renderer *RendererChangedError
	if errors.As(err, &renderer) {
		t.Fatal("a changed source is not a renderer change")
	}
}

func TestVerifyAcceptedRequiresTheRecordedEffectiveTree(t *testing.T) {
	root, source := depRawFixture(t)
	commitWeb2(t, root)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.EffectiveTree == receipt.BaseTree || receipt.ObservedRevision != receipt.EffectiveTree {
		t.Fatalf("a tree with memory deploys under the effective tree ID: %+v", receipt)
	}
	commit := depHead(t, root)
	t.Run("another effective tree", func(t *testing.T) {
		changed := receipt
		changed.EffectiveTree = strings.Repeat("3", 40)
		changed.ObservedRevision = changed.EffectiveTree
		_, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed)
		var treeErr *EffectiveTreeChangedError
		if !errors.As(err, &treeErr) {
			t.Fatalf("err = %v, want an EffectiveTreeChangedError", err)
		}
	})
	t.Run("a revision that is not the effective tree", func(t *testing.T) {
		changed := receipt
		changed.ObservedRevision = receipt.BaseTree
		if _, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed); err == nil {
			t.Fatal("evidence observed at the base tree stamp was accepted")
		}
	})
}

// The pod-template stamp is the effective tree ID, so a memory-only commit
// does not change what a redeploy applies.
func TestStampIsTheEffectiveTreeID(t *testing.T) {
	root, source := depRawFixture(t)
	commitWeb2(t, root)
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
		meta := document["metadata"].(map[string]any)["labels"].(map[string]any)
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
// agent's checkout: committed content, no memory, and a directory that stays
// the same when the checkout is edited afterwards.
func TestImageBuildsReadTheMaterializedEffectiveTree(t *testing.T) {
	root, source := depRawFixture(t)
	commitWeb2(t, root)
	deployer, fake := depDeployer(t, root, &clusterFake{})
	deployer.Materializer = &rendered.Materializer{Root: filepath.Join(t.TempDir(), "effective")}
	var checkouts []string
	fake.Hook = func(_ context.Context, req imagesrc.Request) error {
		checkouts = append(checkouts, req.Checkout)
		body, err := os.ReadFile(filepath.Join(req.Checkout, "k8s", "web.yaml"))
		if err != nil || !strings.Contains(string(body), "web:2") {
			return errors.New("build input does not carry the commit: " + string(body))
		}
		if _, err := os.Stat(filepath.Join(req.Checkout, ".sdo")); err == nil {
			return errors.New("build input carries memory")
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
