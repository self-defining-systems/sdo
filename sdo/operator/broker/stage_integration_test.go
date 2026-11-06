//go:build integration

package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/rendered"
)

// stageGoal writes a goal revision into a fresh Human/initial proposal.
func stageGoal(t *testing.T, w *advanceWorld, id, text string) Proposal {
	t.Helper()
	p, err := w.broker.CreateProposalForPhase(context.Background(), id, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{layout.GoalFile: advanceGoal + "\n" + text + "\n"})
	return p
}

// boundEvidence is what an independent verifier of c reports: the commit and
// tree it judged and the effective deployment identity of that tree.
func boundEvidence(t *testing.T, c Candidate) CandidateEvidence {
	t.Helper()
	effective, err := rendered.Effective(context.Background(), rendered.ObjectStore{Dir: c.Path}, rendered.BaseTree(c.Tree))
	if err != nil {
		t.Fatal(err)
	}
	defer effective.Close()
	return CandidateEvidence{Commit: c.Commit, Tree: c.Tree, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest}
}

func requireHead(t *testing.T, w *advanceWorld, want, why string) {
	t.Helper()
	if got := w.head(t); got != want {
		t.Fatalf("%s: accepted HEAD is %s, want %s", why, got, want)
	}
}

// A staged candidate is a validated broker commit in a private stage: the
// accepted repository neither points at nor contains it until Accept, which
// fast-forwards to exactly that commit (same tree, same trailers).
func TestStagedCandidateIsInvisibleUntilAccepted(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	before := w.head(t)
	stageGoal(t, w, "goal-2", "Staged")

	c, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	requireHead(t, w, before, "staging")
	if c.Commit == "" || c.Parent != before || c.Accepted {
		t.Fatalf("candidate = %+v, want a new commit on %s", c, before)
	}
	if _, err := runGit(ctx, w.accepted, "cat-file", "-e", c.Commit+"^{commit}"); err == nil {
		t.Fatal("the staged commit is already in the accepted object store")
	}
	if _, found, err := w.broker.AcceptedCommit(ctx, "goal-2", InitialPhase); err != nil || found {
		t.Fatalf("a staged candidate is reported accepted: found=%v err=%v", found, err)
	}
	// The verifier sees a clean checkout whose HEAD is the candidate.
	if head := gitOut(t, c.Path, "rev-parse", "HEAD"); head != c.Commit {
		t.Fatalf("candidate checkout HEAD %s, want %s", head, c.Commit)
	}
	if status := gitOut(t, c.Path, "status", "--porcelain", "--untracked-files=all"); status != "" {
		t.Fatalf("candidate checkout is not clean:\n%s", status)
	}
	if tree := gitOut(t, c.Path, "rev-parse", c.Commit+"^{tree}"); tree != c.Tree {
		t.Fatalf("candidate tree %s, commit tree %s", c.Tree, tree)
	}
	if !slices.Equal(c.ChangedPaths, []string{layout.GoalFile}) {
		t.Fatalf("changed paths %v", c.ChangedPaths)
	}

	receipt, err := w.broker.Accept(ctx, c, boundEvidence(t, c))
	if err != nil {
		t.Fatal(err)
	}
	requireHead(t, w, c.Commit, "accept")
	if receipt.Commit != c.Commit || !slices.Equal(receipt.ChangedPaths, c.ChangedPaths) || receipt.Phase != InitialPhase {
		t.Fatalf("receipt %+v does not describe the candidate %+v", receipt, c)
	}
	if found, _, err := w.broker.AcceptedCommit(ctx, "goal-2", InitialPhase); err != nil || found.Commit != c.Commit {
		t.Fatalf("accepted commit lookup = %+v, %v", found, err)
	}
	if _, err := os.Stat(c.Path); !os.IsNotExist(err) {
		t.Fatalf("accepted candidate stage was not removed: %v", err)
	}
	// Redelivery after a crash is idempotent; a restage reports the acceptance.
	again, err := w.broker.Accept(ctx, c, boundEvidence(t, Candidate{Path: w.accepted, Commit: c.Commit, Tree: c.Tree}))
	if err != nil || again.Commit != c.Commit {
		t.Fatalf("repeated accept = %+v, %v", again, err)
	}
	restaged, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil || !restaged.Accepted || restaged.Commit != c.Commit {
		t.Fatalf("restage after acceptance = %+v, %v", restaged, err)
	}
	requireHead(t, w, c.Commit, "restage after acceptance")
}

// Publish is StageCandidate then Accept: both produce the same tree and the
// same commit message from the same proposal.
func TestStageThenAcceptCommitsWhatPublishCommits(t *testing.T) {
	ctx := context.Background()
	published := newOnboardedWorld(t, false)
	stageGoal(t, published, "goal-2", "Same")
	receipt, err := published.broker.Publish(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	staged := newOnboardedWorld(t, false)
	stageGoal(t, staged, "goal-2", "Same")
	c, err := staged.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := staged.broker.Accept(ctx, c, boundEvidence(t, c))
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%T", "%B", "%an <%ae>", "%cn <%ce>"} {
		want := gitOut(t, published.accepted, "log", "-1", "--format="+format, receipt.Commit)
		got := gitOut(t, staged.accepted, "log", "-1", "--format="+format, accepted.Commit)
		if format == "%T" {
			// The onboarding commits differ by timestamp only; compare the goal.
			want = gitOut(t, published.accepted, "show", receipt.Commit+":"+layout.GoalFile)
			got = gitOut(t, staged.accepted, "show", accepted.Commit+":"+layout.GoalFile)
		}
		if got != want {
			t.Fatalf("%s differs: publish %q, stage+accept %q", format, want, got)
		}
	}
	if !slices.Equal(receipt.ChangedPaths, accepted.ChangedPaths) {
		t.Fatalf("changed paths: publish %v, stage+accept %v", receipt.ChangedPaths, accepted.ChangedPaths)
	}
}

// Accept recomputes the binding: evidence for another commit, tree, effective
// tree or patch series never advances HEAD, and the candidate stays acceptable
// with evidence that binds (the positive control).
func TestAcceptRefusesEvidenceNotBoundToTheCandidate(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	before := w.head(t)
	stageGoal(t, w, "goal-2", "Bound")
	c, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	good := boundEvidence(t, c)
	headTree := gitOut(t, w.accepted, "rev-parse", "HEAD^{tree}")
	for name, mutate := range map[string]func(*CandidateEvidence){
		"empty":                func(e *CandidateEvidence) { *e = CandidateEvidence{} },
		"accepted head":        func(e *CandidateEvidence) { e.Commit = before },
		"another tree":         func(e *CandidateEvidence) { e.Tree = headTree },
		"another effective":    func(e *CandidateEvidence) { e.EffectiveTree = headTree },
		"another patch digest": func(e *CandidateEvidence) { e.PatchesDigest = strings.Repeat("e", 64) },
	} {
		evidence := good
		mutate(&evidence)
		if _, err := w.broker.Accept(ctx, c, evidence); !errors.Is(err, ErrCandidateUnbound) {
			t.Fatalf("%s: accept = %v, want ErrCandidateUnbound", name, err)
		}
		requireHead(t, w, before, name)
	}
	if _, err := w.broker.Accept(ctx, c, good); err != nil {
		t.Fatalf("bound evidence after refusals: %v", err)
	}
	requireHead(t, w, c.Commit, "bound evidence")
}

// Only the candidate the broker staged can be accepted: a caller cannot swap
// in another commit or checkout, a discarded or replaced candidate is gone,
// and nothing is accepted without a stage.
func TestAcceptRefusesACandidateTheBrokerDidNotStage(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	before := w.head(t)
	p := stageGoal(t, w, "goal-2", "First")
	first, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	// An agent-made commit with the broker's message in the proposal checkout.
	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "add", "-A")
	testutil.Git(t, p.Path, "commit", "-qm", gitOut(t, first.Path, "log", "-1", "--format=%B"))
	forged := first
	forged.Commit = gitOut(t, p.Path, "rev-parse", "HEAD")
	forged.Path = p.Path
	forged.Tree = gitOut(t, p.Path, "rev-parse", "HEAD^{tree}")
	if _, err := w.broker.Accept(ctx, forged, boundEvidence(t, forged)); !errors.Is(err, ErrCandidateUnbound) {
		t.Fatalf("forged candidate: %v", err)
	}
	moved := first
	moved.Path = p.Path
	if _, err := w.broker.Accept(ctx, moved, boundEvidence(t, first)); !errors.Is(err, ErrCandidateUnbound) {
		t.Fatalf("candidate with another checkout: %v", err)
	}
	if _, err := w.broker.Accept(ctx, Candidate{IncidentID: "never-staged", Phase: InitialPhase, Commit: first.Commit}, boundEvidence(t, first)); !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("never staged: %v", err)
	}
	requireHead(t, w, before, "refused candidates")

	// A restage replaces the candidate; the first one is no longer acceptable.
	testutil.Git(t, p.Path, "reset", "-q", "--soft", "HEAD^")
	writeFiles(t, p.Path, map[string]string{layout.GoalFile: advanceGoal + "\nSecond\n"})
	second, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if second.Commit == first.Commit {
		t.Fatal("a changed proposal staged the same commit")
	}
	if _, err := w.broker.Accept(ctx, first, boundEvidence(t, second)); !errors.Is(err, ErrCandidateUnbound) {
		t.Fatalf("replaced candidate: %v", err)
	}
	requireHead(t, w, before, "replaced candidate")

	if err := w.broker.DiscardCandidate("goal-2", InitialPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(second.Path); !os.IsNotExist(err) {
		t.Fatalf("discard left the stage: %v", err)
	}
	if _, err := w.broker.Accept(ctx, second, CandidateEvidence{Commit: second.Commit, Tree: second.Tree}); !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("discarded candidate: %v", err)
	}
	requireHead(t, w, before, "discarded candidate")
	if _, err := os.Stat(filepath.Join(p.Path, layout.GoalFile)); err != nil {
		t.Fatalf("discard touched the proposal checkout: %v", err)
	}
}

// Validation was bound to the accepted HEAD at staging. When HEAD moved, the
// candidate is stale (re-stage and re-verify), never accepted on top.
func TestAcceptRefusesAStaleCandidate(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	stageGoal(t, w, "goal-2", "Slow")
	c, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	// Another publication (here the controller's outcome record) moves HEAD.
	peer, err := w.broker.RecordOutcome(ctx, "incident-1", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident-1", Classification: "success"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.broker.Accept(ctx, c, boundEvidence(t, c)); !errors.Is(err, ErrCandidateStale) {
		t.Fatalf("accept after HEAD moved = %v, want ErrCandidateStale", err)
	}
	requireHead(t, w, peer.Commit, "stale candidate")
	// Re-staging replays the proposal onto the new HEAD.
	restaged, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if restaged.Parent != peer.Commit {
		t.Fatalf("restaged on %s, want %s", restaged.Parent, peer.Commit)
	}
	if _, err := w.broker.Accept(ctx, restaged, boundEvidence(t, restaged)); err != nil {
		t.Fatal(err)
	}
	requireHead(t, w, restaged.Commit, "restaged candidate")
}

// Every publish guarantee holds at staging: a candidate the shared check
// rejects (ownership, verified-source, the advance rule, the compile
// validator) is never staged, HEAD is untouched and the proposal checkout keeps
// the agent's work for a correction.
func TestRejectedCandidateLeavesHeadAndCheckoutIntact(t *testing.T) {
	ctx := context.Background()
	type attempt struct {
		id    string
		actor Actor
		phase Phase
		files map[string]string
		// rule is the diagnostic the shared check reports; empty when the
		// refusal is the compile validator's.
		rule      string
		validator Validator
	}
	for name, a := range map[string]attempt{
		"ownership": {id: "judge", actor: HealthJudge, phase: InitialPhase,
			files: map[string]string{layout.GoalFile: advanceGoal + "\nJudge\n"}, rule: "ownership.denied"},
		"deployer source without verification": {id: "deploy-2", actor: Deployer, phase: InitialPhase,
			files: map[string]string{"app.txt": "one\ntwo\nthree unverified\n"}},
		"compile validator": {id: "goal-2", actor: Human, phase: InitialPhase,
			files:     map[string]string{layout.GoalFile: advanceGoal + "\nInvalid\n"},
			validator: func(context.Context, string, []string) error { return errors.New("synthetic compile failure") }},
	} {
		t.Run(name, func(t *testing.T) {
			w := newOnboardedWorld(t, false)
			if a.validator != nil {
				w.broker.validator = a.validator
			}
			before := w.head(t)
			p, err := w.broker.CreateProposalForPhase(ctx, a.id, a.phase)
			if err != nil {
				t.Fatal(err)
			}
			writeFiles(t, p.Path, a.files)
			_, err = w.broker.StageCandidate(ctx, a.id, a.actor, a.phase)
			if err == nil {
				t.Fatal("rejected candidate was staged")
			}
			if a.validator == nil {
				diagnostics, typed := RejectionDiagnostics(err)
				if !typed {
					t.Fatalf("rejection is not typed: %v", err)
				}
				if a.rule != "" && !slices.ContainsFunc(diagnostics, func(d *contracts.MemoryDiagnostic) bool { return d.GetRule() == a.rule }) {
					t.Fatalf("diagnostics %v lack %s", diagnostics, a.rule)
				}
			}
			requireHead(t, w, before, name)
			for path, content := range a.files {
				if got, err := os.ReadFile(filepath.Join(p.Path, path)); err != nil || string(got) != content {
					t.Fatalf("proposal file %s = %q, %v; want the agent's work", path, got, err)
				}
			}
			if _, err := os.Stat(w.broker.candidateRoot(a.id, a.phase)); !os.IsNotExist(err) {
				t.Fatalf("a rejected candidate left a stage: %v", err)
			}
			if _, err := w.broker.Accept(ctx, Candidate{IncidentID: a.id, Phase: a.phase, Commit: before}, CandidateEvidence{Commit: before}); !errors.Is(err, ErrNoCandidate) {
				t.Fatalf("accept after rejection = %v", err)
			}
			requireHead(t, w, before, name+" accept")
		})
	}
}

// An upstream advance is accepted only through PublishAdvance: staging it is
// refused by the shared check and leaves the advance proposal publishable.
func TestStagingAnAdvanceIsRefused(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	before := w.head(t)
	upstream := w.push(t, w.seed, map[string]*string{"new.txt": text("added upstream\n")})
	if _, _, _, err := w.broker.ProposeAdvance(ctx, "advance-1", w.seed, upstream); err != nil {
		t.Fatal(err)
	}
	_, err := w.broker.StageCandidate(ctx, "advance-1", UpstreamActor, AdvancePhase)
	diagnostics, typed := RejectionDiagnostics(err)
	if !typed || !slices.ContainsFunc(diagnostics, func(d *contracts.MemoryDiagnostic) bool { return d.GetRule() == "lifecycle.verified-source" }) {
		t.Fatalf("staging an advance = %v", err)
	}
	requireHead(t, w, before, "staged advance")
	proposal, err := w.broker.ProposalInfo("advance-1", AdvancePhase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publishAdvance(ctx, w.broker, "advance-1", upstream, w.seed, advanceEvidence(t, proposal.Path, "advance-1")); err != nil {
		t.Fatalf("advance after a refused stage: %v", err)
	}
}

// With a memory remote, Accept replicates the candidate; a peer publication
// that won the race makes the candidate stale instead of being overwritten.
func TestAcceptReplicatesAndRefusesWhenTheMemoryRemoteMoved(t *testing.T) {
	ctx := context.Background()
	w := newOnboardedWorld(t, false)
	remote := filepath.Join(t.TempDir(), "remote.git")
	testutil.Git(t, "", "init", "-q", "--bare", remote)
	testutil.Git(t, w.accepted, "push", "-q", remote, "HEAD:refs/heads/lane")
	if err := w.broker.SetUpstream(Upstream{URL: remote, Branch: "lane"}); err != nil {
		t.Fatal(err)
	}
	stageGoal(t, w, "goal-2", "Replicated")
	c, err := w.broker.StageCandidate(ctx, "goal-2", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.broker.Accept(ctx, c, boundEvidence(t, c)); err != nil {
		t.Fatal(err)
	}
	if got := gitOut(t, remote, "rev-parse", "refs/heads/lane"); got != c.Commit {
		t.Fatalf("remote lane at %s, want the accepted candidate %s", got, c.Commit)
	}

	stageGoal(t, w, "goal-3", "Raced")
	raced, err := w.broker.StageCandidate(ctx, "goal-3", Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	peerClone := filepath.Join(t.TempDir(), "peer")
	testutil.Git(t, "", "clone", "-q", "--branch", "lane", remote, peerClone)
	testutil.ConfigureGit(t, peerClone)
	writeFiles(t, peerClone, map[string]string{"peer.txt": "peer\n"})
	testutil.Git(t, peerClone, "add", ".")
	testutil.Git(t, peerClone, "commit", "-qm", "peer publication")
	testutil.Git(t, peerClone, "push", "-q", "origin", "HEAD:refs/heads/lane")
	peer := gitOut(t, peerClone, "rev-parse", "HEAD")
	if _, err := w.broker.Accept(ctx, raced, boundEvidence(t, raced)); !errors.Is(err, ErrCandidateStale) {
		t.Fatalf("accept after the remote moved = %v, want ErrCandidateStale", err)
	}
	if got := gitOut(t, remote, "rev-parse", "refs/heads/lane"); got != peer {
		t.Fatalf("remote lane at %s, want the peer's %s", got, peer)
	}
	if head := w.head(t); head == raced.Commit {
		t.Fatal("a stale candidate was accepted")
	}
}
