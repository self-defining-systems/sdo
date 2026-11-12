//go:build integration

package broker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// vendorText is a vendored file long enough for patches with real context.
const vendorText = "v01\nv02\nv03\nv04\nv05\nv06\nv07\nv08\nv09\nv10\nv11\nv12\nv13\nv14\nv15\nv16\n"

const advanceGoal = "---\nschema_version: 1\nowner: human\napplication: \"test\"\n---\n# Health objective\n\nGoal\n"

// advanceWorld is a synthetic followed source: an upstream repository whose
// first commit seeds the accepted repository, a Human goal commit that starts
// following it, and an onboarding deployment of the source as it stands (a
// legacy world has a source edit committed before it).
type advanceWorld struct {
	upstream, accepted string
	seed               string
	broker             *Broker
}

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for path, content := range files {
		target := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func gitOut(t *testing.T, root string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(testutil.Git(t, root, args...))
}

func newAdvanceWorld(t *testing.T) *advanceWorld { return newOnboardedWorld(t, true) }

// newOnboardedWorld onboards the synthetic source; editedByDeployer false is a
// provided deployment: the accepted source is deployed exactly as it stands.
func newOnboardedWorld(t *testing.T, editedByDeployer bool) *advanceWorld {
	t.Helper()
	base := t.TempDir()
	copyTree(t, onboardedTemplate(t, editedByDeployer), base)
	w := &advanceWorld{upstream: filepath.Join(base, "upstream"), accepted: filepath.Join(base, "accepted")}
	w.seed = gitOut(t, w.upstream, "rev-parse", "HEAD")
	b, err := New(w.accepted, filepath.Join(base, "workspaces"), func(context.Context, string, []string) error { return nil }, testObservation())
	if err != nil {
		t.Fatal(err)
	}
	w.broker = b
	return w
}

// onboardedTemplates holds one built world per variant. Building a world is
// about 150 git commands; every test copies the template instead, so a test
// pays for the behavior it checks and not for the shared setup. All state is
// repository content under the world's directory (no absolute paths survive
// onboarding), so a copy is a world of its own.
var onboardedTemplates [2]struct {
	once  sync.Once
	dir   string
	built bool
}

// onboardedTemplateRoot is removed by TestMain.
var onboardedTemplateRoot struct {
	once sync.Once
	dir  string
	err  error
}

func onboardedTemplate(t *testing.T, editedByDeployer bool) string {
	t.Helper()
	slot := &onboardedTemplates[0]
	if editedByDeployer {
		slot = &onboardedTemplates[1]
	}
	slot.once.Do(func() {
		onboardedTemplateRoot.once.Do(func() {
			onboardedTemplateRoot.dir, onboardedTemplateRoot.err = os.MkdirTemp("", "onboarded-templates-")
		})
		if onboardedTemplateRoot.err != nil {
			t.Fatal(onboardedTemplateRoot.err)
		}
		slot.dir = filepath.Join(onboardedTemplateRoot.dir, fmt.Sprint(editedByDeployer))
		buildOnboardedWorld(t, slot.dir, editedByDeployer)
		slot.built = true
	})
	if !slot.built {
		t.Fatal("the onboarded world template failed to build in an earlier test")
	}
	return slot.dir
}

func TestMain(m *testing.M) {
	code := m.Run()
	if onboardedTemplateRoot.dir != "" {
		os.RemoveAll(onboardedTemplateRoot.dir)
	}
	os.Exit(code)
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	if out, err := exec.Command("cp", "-a", from+"/.", to).CombinedOutput(); err != nil {
		t.Fatalf("copy %s: %v: %s", from, err, out)
	}
}

func buildOnboardedWorld(t *testing.T, base string, editedByDeployer bool) *advanceWorld {
	w := &advanceWorld{upstream: filepath.Join(base, "upstream"), accepted: filepath.Join(base, "accepted")}
	if err := os.MkdirAll(w.upstream, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, w.upstream)
	writeFiles(t, w.upstream, map[string]string{"app.txt": "one\ntwo\nthree\n", "Dockerfile": "FROM scratch\n", "lib/vendor.txt": vendorText})
	testutil.Git(t, w.upstream, "add", ".")
	// A user commit cannot start following by spelling the broker's trailer.
	testutil.Git(t, w.upstream, "commit", "-qm", "seed\n\nSDO-Upstream: "+strings.Repeat("f", 40))
	w.seed = gitOut(t, w.upstream, "rev-parse", "HEAD")
	testutil.Git(t, base, "clone", "-q", w.upstream, w.accepted)
	testutil.ConfigureGit(t, w.accepted)
	testutil.Git(t, w.accepted, "remote", "remove", "origin")
	b, err := New(w.accepted, filepath.Join(base, "workspaces"), func(context.Context, string, []string) error { return nil }, testObservation())
	if err != nil {
		t.Fatal(err)
	}
	w.broker = b
	publishHumanGoal(t, w.accepted, "goal", map[string]string{layout.GoalFile: advanceGoal, layout.SourcesFile: syntheticSources})
	if editedByDeployer {
		// A repository from before agents wrote only .sdo/: a source edit is
		// already in the accepted history (a plain commit; no agent can
		// publish one any more), so the mirror invariant does not hold.
		writeFiles(t, w.accepted, map[string]string{"app.txt": "one\ntwo\nthree deployed\n"})
		testutil.Git(t, w.accepted, "add", ".")
		testutil.Git(t, w.accepted, "commit", "-qm", "legacy source edit")
	}
	// Onboarding deployed the accepted source with a plan.
	p, err := b.CreateProposalForPhase(context.Background(), "onboard", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeSyntheticPlan(t, p.Path, "k8s")
	if _, err := b.PublishVerifiedSource(context.Background(), "onboard", advanceEvidence(t, p.Path, "onboard"), deploymentReportFor(t, advanceEvidence(t, p.Path, "onboard"), p.BaseCommit)); err != nil {
		t.Fatal(err)
	}
	return w
}

// syntheticPlan declares a raw source over path; the deployer's evidence for
// it names the same source.
func syntheticPlan(t *testing.T, path string) *contracts.DeploymentPlan {
	t.Helper()
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{path}})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "raw", Config: config}, ExpectedWorkloadIds: []string{"Deployment/ns/app"}}
}

// writeSyntheticPlan writes the plan.yaml the lifecycle writes at admission.
func writeSyntheticPlan(t *testing.T, root, path string) {
	t.Helper()
	if err := planfile.Write(root, syntheticPlan(t, path)); err != nil {
		t.Fatal(err)
	}
}

func advanceEvidence(t *testing.T, checkout, image string) SourceEvidence {
	t.Helper()
	return advanceEvidenceFor(t, checkout, image, true)
}

// advanceEvidenceFor builds evidence for checkout's tree. With applied false
// the tree's patches need not apply: the effective fields are placeholders (for
// tests that attest a tree the broker must refuse).
func advanceEvidenceFor(t *testing.T, checkout, image string, applied bool) SourceEvidence {
	t.Helper()
	// The complete working tree: an advance stage holds its merge uncommitted.
	tree, err := advanceTreeDigest(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "raw", Config: config}
	effective := rendered.EffectiveTree{ID: strings.Repeat("0", 40), PatchesDigest: strings.Repeat("0", 64)}
	if applied {
		var err error
		if effective, err = rendered.Effective(context.Background(), rendered.ObjectStore{Dir: checkout}, rendered.BaseTree(tree)); err != nil {
			t.Fatal(err)
		}
		defer effective.Close()
	}
	return SourceEvidence{
		DeploymentSource: source,
		BaseTree:         tree, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest, ObservedRevision: effective.ID,
		RenderDigest: strings.Repeat("b", 64), EnvDigest: strings.Repeat("c", 64),
		Images: []string{image + "@sha256:" + strings.Repeat("d", 64)}, ResourceIDs: []string{"Deployment/ns/app"}, Ready: true,
	}
}

// push commits files (nil content deletes) on the upstream and materializes
// the commit in the accepted object store the way the git watcher does.
func (w *advanceWorld) push(t *testing.T, parent string, files map[string]*string) string {
	t.Helper()
	testutil.Git(t, w.upstream, "checkout", "-q", "--detach", parent)
	for path, content := range files {
		if content == nil {
			testutil.Git(t, w.upstream, "rm", "-q", path)
			continue
		}
		writeFiles(t, w.upstream, map[string]string{path: *content})
		testutil.Git(t, w.upstream, "add", path)
	}
	testutil.Git(t, w.upstream, "commit", "-qm", "upstream change")
	commit := gitOut(t, w.upstream, "rev-parse", "HEAD")
	testutil.Git(t, w.accepted, "fetch", "-q", "--no-tags", w.upstream, commit+":refs/sdo/upstream-source/"+commit)
	return commit
}

// holds lists the revisions the accepted repository holds for the watcher.
func (w *advanceWorld) holds(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, ref := range strings.Fields(gitOut(t, w.accepted, "for-each-ref", "--format=%(refname)", "refs/sdo/upstream-source/")) {
		out = append(out, strings.TrimPrefix(ref, "refs/sdo/upstream-source/"))
	}
	slices.Sort(out)
	return out
}

func text(value string) *string { return &value }

func (w *advanceWorld) head(t *testing.T) string { return gitOut(t, w.accepted, "rev-parse", "HEAD") }

// The broker recomputes the effective tree of what it stages and refuses
// verified-source evidence that names another effective tree or patch series.
func TestPublishVerifiedSourceRecomputesTheEffectiveTree(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	p, err := w.broker.CreateProposalForPhase(ctx, "second", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{".sdo/deployment/values/second.yaml": "replicas: 2\n"})
	for name, mutate := range map[string]func(*SourceEvidence){
		"effective tree": func(e *SourceEvidence) {
			e.EffectiveTree = strings.Repeat("9", 40)
			e.ObservedRevision = e.EffectiveTree
		},
		"patch series": func(e *SourceEvidence) { e.PatchesDigest = strings.Repeat("9", 64) },
	} {
		evidence := advanceEvidence(t, p.Path, "second")
		mutate(&evidence)
		if _, err := w.broker.PublishVerifiedSource(ctx, "second", evidence, deploymentReportFor(t, evidence, p.BaseCommit)); err == nil || !strings.Contains(err.Error(), "differs from the verified deployment") {
			t.Fatalf("PublishVerifiedSource with another %s = %v", name, err)
		}
	}
	if _, err := w.broker.PublishVerifiedSource(ctx, "second", advanceEvidence(t, p.Path, "second"), deploymentReportFor(t, advanceEvidence(t, p.Path, "second"), p.BaseCommit)); err != nil {
		t.Fatalf("matching evidence must publish: %v", err)
	}
}

// The plan is part of the staged tree: evidence for a tree whose plan.yaml is
// missing, or names another source than the deployed one, is never accepted.
func TestPublishVerifiedSourceRequiresThePlanToNameTheDeployedSource(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	for name, c := range map[string]struct {
		prepare func(root string)
		want    string
	}{
		"no plan": {func(root string) {
			if err := os.Remove(filepath.Join(root, filepath.FromSlash(layout.DeploymentPlanFile))); err != nil {
				t.Fatal(err)
			}
		}, "plan.yaml"},
		"another source": {func(root string) { writeSyntheticPlan(t, root, "elsewhere") }, "another deployment source"},
	} {
		id := "plan-" + strings.ReplaceAll(name, " ", "-")
		p, err := w.broker.CreateProposalForPhase(ctx, id, InitialPhase)
		if err != nil {
			t.Fatal(err)
		}
		writeFiles(t, p.Path, map[string]string{".sdo/deployment/values/" + id + ".yaml": "replicas: 2\n"})
		c.prepare(p.Path)
		if _, err := w.broker.PublishVerifiedSource(ctx, id, advanceEvidence(t, p.Path, id), deploymentReportFor(t, advanceEvidence(t, p.Path, id), p.BaseCommit)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: PublishVerifiedSource = %v, want %q", name, err, c.want)
		}
	}
}
