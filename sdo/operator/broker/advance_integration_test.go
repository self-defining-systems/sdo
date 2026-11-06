//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
)

const advanceGoal = "---\nschema_version: 1\nowner: human\napplication: \"test\"\n---\n# Health objective\n\nGoal\n"

// advanceWorld is a synthetic followed source: an upstream repository whose
// first commit seeds the accepted repository, a Human goal commit that starts
// following it, and an onboarding deployment that edited the source.
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

func newAdvanceWorld(t *testing.T) *advanceWorld {
	t.Helper()
	base := t.TempDir()
	w := &advanceWorld{upstream: filepath.Join(base, "upstream"), accepted: filepath.Join(base, "accepted")}
	if err := os.MkdirAll(w.upstream, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, w.upstream)
	writeFiles(t, w.upstream, map[string]string{"app.txt": "one\ntwo\nthree\n", "Dockerfile": "FROM scratch\n"})
	testutil.Git(t, w.upstream, "add", ".")
	// A user commit cannot start following by spelling the broker's trailer.
	testutil.Git(t, w.upstream, "commit", "-qm", "seed\n\nSDO-Upstream: "+strings.Repeat("f", 40))
	w.seed = gitOut(t, w.upstream, "rev-parse", "HEAD")
	testutil.Git(t, base, "clone", "-q", w.upstream, w.accepted)
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
	// Onboarding deployed an edited source (third line) with a plan.
	p, err := b.CreateProposalForPhase(context.Background(), "onboard", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree deployed\n"})
	if _, err := b.PublishVerifiedSource(context.Background(), "onboard", advanceEvidence(t, p.Path, "onboard")); err != nil {
		t.Fatal(err)
	}
	return w
}

func advanceEvidence(t *testing.T, checkout, image string) SourceEvidence {
	t.Helper()
	tree, err := ProposalTreeDigest(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "raw", Config: config}
	return SourceEvidence{
		DeploymentSource: source,
		Plan:             &contracts.DeploymentPlan{Source: proto.Clone(source).(*contracts.DeploymentSource), ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		BaseTree:         tree, ObservedRevision: tree,
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
	if err != nil || !found || onboarded.Plan == nil || onboarded.Images[0] != "onboard@sha256:"+strings.Repeat("d", 64) {
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
	if err != nil || !found || current.AcceptedCommit != receipt.Commit || current.Images[0] != evidence.Images[0] || current.Plan == nil {
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
		writeFiles(t, proposal.Path, map[string]string{"app.txt": "tampered\n"})
		evidence := advanceEvidence(t, proposal.Path, "tampered")
		if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence); err == nil || !strings.Contains(err.Error(), "differs from the deployed tree") {
			t.Fatalf("PublishAdvance of another tree = %v", err)
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
