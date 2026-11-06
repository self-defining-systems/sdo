//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
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
	b, err := New(w.accepted, filepath.Join(base, "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	w.broker = b
	if _, found, err := b.LastUpstream(context.Background()); err != nil || found {
		t.Fatalf("LastUpstream before following = found %v, %v; a user trailer is not trusted", found, err)
	}
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
	if _, err := b.PublishVerifiedSource(context.Background(), "onboard", advanceEvidence(t, p.Path, "onboard")); err != nil {
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

// An upstream commit merges three-way with the deployer's accepted edits; the
// candidate checkout is clean at its candidate, the accepted commit is
// single-parent with the upstream trailers, binds the deployed tree, and its
// evidence becomes the accepted deployment evidence.
func TestPublishAdvanceAcceptsTheDeployedThreeWayMerge(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	b := w.broker
	last, found, err := b.LastUpstream(ctx)
	if err != nil || !found || last != w.seed {
		t.Fatalf("LastUpstream = %q %v %v, want the seed %s", last, found, err, w.seed)
	}
	onboarded, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found || !rendered.SameDeploymentSource(onboarded.Plan.GetSource(), syntheticPlan(t, "k8s").GetSource()) || onboarded.Evidence.Images[0] != "onboard@sha256:"+strings.Repeat("d", 64) {
		t.Fatalf("AcceptedSourceEvidence after onboarding = %+v %v %v", onboarded, found, err)
	}
	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n"), "new.txt": text("new\n"), "Dockerfile": nil})
	ancestor, paths, err := b.UpstreamChange(ctx, w.seed, upstream)
	if err != nil || !ancestor || !slices.Equal(paths, []string{"Dockerfile", "app.txt", "new.txt"}) {
		t.Fatalf("UpstreamChange = %v %v %v", ancestor, paths, err)
	}
	stray := w.push(t, w.seed, map[string]*string{"stray.txt": text("held back, then superseded\n")})
	accepted := w.head(t)
	id := "advance-" + upstream[:12]
	proposal, candidate, conflicts, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil || len(conflicts) > 0 || candidate == "" {
		t.Fatalf("ProposeAdvance = %q %v %v", candidate, conflicts, err)
	}
	if proposal.BaseCommit != accepted || gitOut(t, proposal.Path, "rev-parse", "HEAD") != candidate || gitOut(t, proposal.Path, "status", "--porcelain", "--untracked-files=all") != "" {
		t.Fatalf("proposal %+v is not clean at candidate %s on accepted %s", proposal, candidate, accepted)
	}
	merged, err := os.ReadFile(filepath.Join(proposal.Path, "app.txt"))
	if err != nil || string(merged) != "ONE\ntwo\nthree deployed\n" {
		t.Fatalf("merged app.txt = %q %v", merged, err)
	}
	if _, err := os.Stat(filepath.Join(proposal.Path, "Dockerfile")); !os.IsNotExist(err) {
		t.Fatalf("upstream deletion not merged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(proposal.Path, layout.GoalFile)); err != nil {
		t.Fatalf("accepted memory lost in the merge: %v", err)
	}
	evidence := advanceEvidence(t, proposal.Path, "advanced")
	candidateTree := gitOut(t, proposal.Path, "rev-parse", candidate+"^{tree}")
	receipt, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Phase != AdvancePhase || receipt.Commit != w.head(t) {
		t.Fatalf("receipt %+v, head %s", receipt, w.head(t))
	}
	if parents := strings.Fields(gitOut(t, w.accepted, "log", "-1", "--format=%P", receipt.Commit)); !slices.Equal(parents, []string{accepted}) {
		t.Fatalf("advance commit parents %v, want only %s", parents, accepted)
	}
	message := "\n" + gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit) + "\n"
	for _, trailer := range []string{"SDO-Actor: upstream", "SDO-Phase: advance", "SDO-Incident: " + id, "SDO-Upstream: " + upstream, "SDO-Upstream-Base: " + w.seed} {
		if !strings.Contains(message, "\n"+trailer+"\n") {
			t.Fatalf("advance commit lacks %q:\n%s", trailer, message)
		}
	}
	if tree := gitOut(t, w.accepted, "rev-parse", receipt.Commit+"^{tree}"); tree != evidence.BaseTree || tree != candidateTree {
		t.Fatalf("accepted tree %s, deployed %s", tree, evidence.BaseTree)
	}
	if holds := w.holds(t); slices.Contains(holds, stray) || !slices.Contains(holds, upstream) {
		t.Fatalf("hold refs after the advance = %v, want the accepted upstream %s kept and %s pruned", holds, upstream, stray)
	}
	if last, found, err := b.LastUpstream(ctx); err != nil || !found || last != upstream {
		t.Fatalf("LastUpstream after advance = %q %v %v", last, found, err)
	}
	current, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found || current.Evidence.AcceptedCommit != receipt.Commit || current.Evidence.Images[0] != evidence.Images[0] || current.Plan == nil {
		t.Fatalf("AcceptedSourceEvidence after advance = %+v %v %v", current, found, err)
	}
	if _, err := os.Stat(b.proposalRoot(id, AdvancePhase)); !os.IsNotExist(err) {
		t.Fatalf("accepted advance checkout not removed: %v", err)
	}
	again, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence)
	if err != nil || again.Commit != receipt.Commit {
		t.Fatalf("repeated publish = %+v %v, want the accepted %s", again, err, receipt.Commit)
	}
	if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, advanceEvidence(t, w.accepted, "other")); err == nil {
		t.Fatal("repeated publish with other evidence accepted")
	}

	// The next advance merges from the recorded upstream revision.
	next := w.push(t, upstream, map[string]*string{"new.txt": text("newer\n")})
	nextID := "advance-" + next[:12]
	proposal, candidate, _, err = b.ProposeAdvance(ctx, nextID, upstream, next)
	if err != nil || candidate == "" {
		t.Fatal(err)
	}
	if _, _, _, err := b.ProposeAdvance(ctx, nextID, w.seed, next); err == nil || !strings.Contains(err.Error(), "not the accepted upstream revision") {
		t.Fatalf("advance from a stale base = %v", err)
	}
	proposal, candidate, _, err = b.ProposeAdvance(ctx, nextID, upstream, next)
	if err != nil || candidate == "" {
		t.Fatal(err)
	}
	if _, err := b.PublishAdvance(ctx, nextID, next, upstream, advanceEvidence(t, proposal.Path, "next")); err != nil {
		t.Fatal(err)
	}
	if holds := w.holds(t); !slices.Equal(holds, []string{min(upstream, next), max(upstream, next)}) {
		t.Fatalf("hold refs after the second advance = %v, want only the accepted upstream %s and its base %s", holds, next, upstream)
	}
}

// Every refusal leaves the accepted branch where it was.
func TestPublishAdvanceRefusesWhatWasNotDeployedOrMayNotMerge(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	b := w.broker
	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id := "advance-" + upstream[:12]
	accepted := w.head(t)

	t.Run("evidence of another tree", func(t *testing.T) {
		proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
		if err != nil {
			t.Fatal(err)
		}
		writeFiles(t, proposal.Path, map[string]string{".sdo/deployment/values/tampered.yaml": "tampered: true\n"})
		evidence := advanceEvidence(t, proposal.Path, "tampered")
		if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence); err == nil || !strings.Contains(err.Error(), "differs from the deployed tree") {
			t.Fatalf("PublishAdvance of another tree = %v", err)
		}
	})
	t.Run("evidence of another effective tree or patch series", func(t *testing.T) {
		proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
		if err != nil {
			t.Fatal(err)
		}
		for name, mutate := range map[string]func(*SourceEvidence){
			"effective tree": func(e *SourceEvidence) {
				e.EffectiveTree = strings.Repeat("9", 40)
				e.ObservedRevision = e.EffectiveTree
			},
			"patch series": func(e *SourceEvidence) { e.PatchesDigest = strings.Repeat("9", 64) },
		} {
			evidence := advanceEvidence(t, proposal.Path, "merged")
			mutate(&evidence)
			if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence); err == nil || !strings.Contains(err.Error(), "differs from the verified deployment") {
				t.Fatalf("PublishAdvance with another %s = %v", name, err)
			}
		}
	})
	t.Run("accepted head moved", func(t *testing.T) {
		proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
		if err != nil {
			t.Fatal(err)
		}
		evidence := advanceEvidence(t, proposal.Path, "moved")
		publishHumanGoal(t, w.accepted, "goal-edit", map[string]string{layout.GoalFile: advanceGoal + "\nEdited\n"})
		defer func() { accepted = w.head(t) }()
		if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence); err == nil || !strings.Contains(err.Error(), "accepted HEAD moved") {
			t.Fatalf("PublishAdvance after the head moved = %v", err)
		}
		if w.head(t) == accepted {
			t.Fatal("goal edit not accepted")
		}
	})
	t.Run("wrong base or upstream", func(t *testing.T) {
		proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
		if err != nil {
			t.Fatal(err)
		}
		evidence := advanceEvidence(t, proposal.Path, "wrong")
		other := w.push(t, w.seed, map[string]*string{"app.txt": text("other\ntwo\nthree\n")})
		if _, err := b.PublishAdvance(ctx, id, other, w.seed, evidence); err == nil {
			t.Fatal("PublishAdvance of another upstream than proposed accepted")
		}
		if _, err := b.PublishAdvance(ctx, id, upstream, upstream, evidence); err == nil {
			t.Fatal("PublishAdvance with a base that is not the accepted upstream revision accepted")
		}
	})
	t.Run("no proposal", func(t *testing.T) {
		if err := b.ReleaseAdvance(id); err != nil {
			t.Fatal(err)
		}
		if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, advanceEvidence(t, w.accepted, "none")); err == nil {
			t.Fatal("PublishAdvance without a proposal accepted")
		}
	})
	t.Run("rewritten upstream", func(t *testing.T) {
		testutil.Git(t, w.upstream, "checkout", "-q", "--orphan", "rewritten")
		testutil.Git(t, w.upstream, "commit", "-qm", "rewritten")
		rewritten := gitOut(t, w.upstream, "rev-parse", "HEAD")
		testutil.Git(t, w.accepted, "fetch", "-q", "--no-tags", w.upstream, rewritten+":refs/sdo/upstream-source/"+rewritten)
		if ancestor, _, err := b.UpstreamChange(ctx, w.seed, rewritten); err != nil || ancestor {
			t.Fatalf("UpstreamChange of rewritten history = %v %v", ancestor, err)
		}
		if _, _, _, err := b.ProposeAdvance(ctx, "advance-"+rewritten[:12], w.seed, rewritten); err == nil || !strings.Contains(err.Error(), "not an ancestor") {
			t.Fatalf("ProposeAdvance of rewritten history = %v", err)
		}
	})
	t.Run("upstream touches operational memory", func(t *testing.T) {
		touched := w.push(t, w.seed, map[string]*string{layout.GoalFile: text("upstream goal\n")})
		if ancestor, paths, err := b.UpstreamChange(ctx, w.seed, touched); err != nil || !ancestor || !slices.Equal(paths, []string{layout.GoalFile}) {
			t.Fatalf("UpstreamChange touching memory = %v %v %v", ancestor, paths, err)
		}
		if _, _, _, err := b.ProposeAdvance(ctx, "advance-"+touched[:12], w.seed, touched); err == nil || !strings.Contains(err.Error(), "operational memory") {
			t.Fatalf("ProposeAdvance touching memory = %v", err)
		}
	})
	t.Run("upstream adds a path named exactly the memory directory", func(t *testing.T) {
		touched := w.push(t, w.seed, map[string]*string{layout.Dir: text("not a directory\n")})
		if _, _, _, err := b.ProposeAdvance(ctx, "advance-"+touched[:12], w.seed, touched); err == nil || !strings.Contains(err.Error(), "operational memory") {
			t.Fatalf("ProposeAdvance adding a file named %s = %v", layout.Dir, err)
		}
	})
	t.Run("conflict with an accepted edit", func(t *testing.T) {
		conflicting := w.push(t, w.seed, map[string]*string{"app.txt": text("one\ntwo\nthree upstream\n")})
		proposal, candidate, conflicts, err := b.ProposeAdvance(ctx, "advance-"+conflicting[:12], w.seed, conflicting)
		if err != nil || candidate != "" || !slices.Equal(conflicts, []string{"app.txt"}) {
			t.Fatalf("ProposeAdvance conflict = %q %v %v", candidate, conflicts, err)
		}
		if gitOut(t, proposal.Path, "rev-parse", "HEAD") != proposal.BaseCommit || gitOut(t, proposal.Path, "status", "--porcelain", "--untracked-files=all") != "" {
			t.Fatal("conflicting checkout was not reset to the accepted HEAD")
		}
		if err := b.ReleaseAdvance("advance-" + conflicting[:12]); err != nil {
			t.Fatal(err)
		}
	})
	if w.head(t) != accepted {
		t.Fatalf("a refused advance moved the accepted HEAD from %s to %s", accepted, w.head(t))
	}
	if last, _, _ := b.LastUpstream(ctx); last != w.seed {
		t.Fatalf("LastUpstream after refusals = %s, want %s", last, w.seed)
	}
}

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
		if _, err := w.broker.PublishVerifiedSource(ctx, "second", evidence); err == nil || !strings.Contains(err.Error(), "differs from the verified deployment") {
			t.Fatalf("PublishVerifiedSource with another %s = %v", name, err)
		}
	}
	if _, err := w.broker.PublishVerifiedSource(ctx, "second", advanceEvidence(t, p.Path, "second")); err != nil {
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
		if _, err := w.broker.PublishVerifiedSource(ctx, id, advanceEvidence(t, p.Path, id)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: PublishVerifiedSource = %v, want %q", name, err, c.want)
		}
	}
}

// The controller's desired state includes the images the newest accepted
// deployment applied, bound to its effective tree: onboarding's pins, then the
// advance's (new tree, new pins); a memory-only commit changes neither.
func TestAcceptedImagePinsFollowTheNewestAcceptedDeployment(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	b := w.broker
	onboarded, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("AcceptedSourceEvidence = %v %v", found, err)
	}
	pins, err := b.AcceptedImagePins(ctx)
	if err != nil || pins.EffectiveTree != onboarded.Evidence.EffectiveTree || pins.BaseTree != onboarded.Evidence.BaseTree {
		t.Fatalf("pins after onboarding = %+v %v, want the onboarded effective tree", pins, err)
	}

	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id := "advance-" + upstream[:12]
	proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil {
		t.Fatal(err)
	}
	evidence := advanceEvidence(t, proposal.Path, "advanced")
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "ns", Name: "app"}
	digest := "advanced@sha256:" + strings.Repeat("e", 64)
	evidence.ContainerImages = []rendered.DeployedImage{{Ref: ref, Name: "app", Applied: digest, Digest: digest}}
	if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{"advance", "after a memory commit"} {
		pins, err = b.AcceptedImagePins(ctx)
		if err != nil || pins.EffectiveTree != evidence.EffectiveTree || pins.BaseTree != evidence.BaseTree || pins.EffectiveTree == onboarded.Evidence.EffectiveTree ||
			len(pins.Images) != 1 || pins.Images[0].Applied != digest {
			t.Fatalf("pins %s = %+v %v, want the advance's tree %s and image", step, pins, err, evidence.EffectiveTree)
		}
		publishHumanGoal(t, w.accepted, "goal", map[string]string{layout.GoalFile: advanceGoal + "\nmore " + step + "\n"})
	}
}
