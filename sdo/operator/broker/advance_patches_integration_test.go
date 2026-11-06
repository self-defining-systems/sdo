//go:build integration

package broker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/rendered"
)

const patchReason = "Pin the vendored library line the deployment depends on for this application"

// patchWorld is a followed source whose accepted tree carries a patch series
// over the vendored file lib/vendor.txt. Series authors work in a scratch
// clone so every patch is a real `git diff` against the tree it applies to.
type patchWorld struct {
	*advanceWorld
	scratch string
	next    int
}

func newPatchWorld(t *testing.T, editedByDeployer bool) *patchWorld {
	t.Helper()
	w := &patchWorld{advanceWorld: newOnboardedWorld(t, editedByDeployer)}
	w.scratch = filepath.Join(t.TempDir(), "author")
	testutil.Git(t, filepath.Dir(w.scratch), "clone", "-q", w.accepted, w.scratch)
	return w
}

// addPatch authors a patch by editing lib/vendor.txt in the scratch clone,
// keeps the edit there (so the next patch stacks on it) and commits the patch
// file into the accepted repository.
func (w *patchWorld) addPatch(t *testing.T, owner string, edit func(string) string) string {
	t.Helper()
	path := filepath.Join(w.scratch, "lib", "vendor.txt")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(edit(string(before))), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, w.scratch, "add", "lib/vendor.txt")
	diff := testutil.Git(t, w.scratch, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff")
	testutil.Git(t, w.scratch, "-c", "user.name=t", "-c", "user.email=t@example.test", "commit", "-qm", "step")
	w.next++
	name := "000" + string(rune('0'+w.next)) + "-vendor.patch"
	incident := ""
	if owner == "responder" {
		incident = "Incident: inc-1\n"
	}
	body := "SDO-Patch: 1\nOwner: " + owner + "\n" + incident + "Reason: |\n  " + patchReason + "\n\n" + diff + "\n"
	writeFiles(t, w.accepted, map[string]string{".sdo/deployment/patches/" + name: body})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: "+name)
	return name
}

func replaceLine(from, to string) func(string) string {
	return func(text string) string { return strings.Replace(text, from+"\n", to+"\n", 1) }
}

func vendorUpstream(t *testing.T, w *patchWorld, parent string, edit func(string) string) string {
	t.Helper()
	testutil.Git(t, w.upstream, "checkout", "-q", "--detach", parent)
	text, err := os.ReadFile(filepath.Join(w.upstream, "lib", "vendor.txt"))
	if err != nil {
		t.Fatal(err)
	}
	next := edit(string(text))
	return w.push(t, parent, map[string]*string{"lib/vendor.txt": &next})
}

func effectiveFile(t *testing.T, repository, tree, path string) string {
	t.Helper()
	eff, err := rendered.Effective(context.Background(), rendered.ObjectStore{Dir: repository}, rendered.BaseTree(tree))
	if err != nil {
		t.Fatal(err)
	}
	defer eff.Close()
	cmd := exec.Command("git", "-C", repository, "cat-file", "-p", eff.ID+":"+path)
	cmd.Env = append(os.Environ(), eff.Store.Env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("effective %s: %v\n%s", path, err, out)
	}
	return string(out)
}

func holdOf(t *testing.T, err error) *PatchHoldError {
	t.Helper()
	var hold *PatchHoldError
	if !errors.As(err, &hold) {
		t.Fatalf("error %v is not a patch hold", err)
	}
	return hold
}

func (w *patchWorld) requireNoAdvanceState(t *testing.T, id, accepted string) {
	t.Helper()
	if w.head(t) != accepted {
		t.Fatalf("accepted HEAD moved from %s to %s", accepted, w.head(t))
	}
	if _, err := os.Stat(w.broker.proposalRoot(id, AdvancePhase)); !os.IsNotExist(err) {
		t.Fatalf("held advance left its checkout: %v", err)
	}
}

// A benign upstream change leaves the patch applying: the advance is accepted
// with the mirror recorded, and the accepted tree stays an exact mirror of the
// upstream outside .sdo/ while the effective tree still carries the patch.
func TestAdvanceReappliesPatchesAndRecordsTheMirror(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	w.addPatch(t, "deployer", replaceLine("v08", "v08 patched"))
	upstream := vendorUpstream(t, w, w.seed, replaceLine("v14", "v14 upstream"))
	id := "advance-" + upstream[:12]
	proposal, candidate, conflicts, err := w.broker.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil || candidate == "" || len(conflicts) > 0 {
		t.Fatalf("ProposeAdvance = %q %v %v", candidate, conflicts, err)
	}
	tree := gitOut(t, proposal.Path, "rev-parse", candidate+"^{tree}")
	got := effectiveFile(t, proposal.Path, tree, "lib/vendor.txt")
	if !strings.Contains(got, "v08 patched\n") || !strings.Contains(got, "v14 upstream\n") {
		t.Fatalf("effective vendor.txt = %q, want the patch on the new upstream", got)
	}
	receipt, err := w.broker.PublishAdvance(ctx, id, upstream, w.seed, advanceEvidence(t, proposal.Path, "benign"))
	if err != nil {
		t.Fatal(err)
	}
	if message := "\n" + gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit) + "\n"; !strings.Contains(message, "\nSDO-Mirror: exact\n") {
		t.Fatalf("advance commit does not record the mirror:\n%s", message)
	}
	want, err := nonMemoryTree(ctx, w.accepted, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := nonMemoryTree(ctx, w.accepted, receipt.Commit); err != nil || got != want {
		t.Fatalf("accepted source tree %s is not upstream's %s (%v)", got, want, err)
	}
}

func TestAdvanceHoldsAPatchWhoseContextChanged(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	name := w.addPatch(t, "deployer", replaceLine("v08", "v08 patched"))
	upstream := vendorUpstream(t, w, w.seed, replaceLine("v08", "v08 rewritten upstream"))
	id := "advance-" + upstream[:12]
	accepted := w.head(t)
	_, candidate, conflicts, err := w.broker.ProposeAdvance(ctx, id, w.seed, upstream)
	hold := holdOf(t, err)
	if candidate != "" || len(conflicts) > 0 {
		t.Fatalf("candidate %q conflicts %v with a patch hold", candidate, conflicts)
	}
	if hold.Patch.Kind != patchset.KindConflict || hold.Patch.Patch != name || hold.Owner != "deployer" || len(hold.Patch.Paths) != 1 || hold.Patch.Paths[0] != "lib/vendor.txt" {
		t.Fatalf("hold = %+v / %+v", hold, hold.Patch)
	}
	if len(hold.UpstreamCommits) != 1 || !strings.HasPrefix(hold.UpstreamCommits[0], upstream[:7]) || !strings.Contains(hold.UpstreamCommits[0], "upstream change") {
		t.Fatalf("upstream commits = %q, want the one that touched lib/vendor.txt", hold.UpstreamCommits)
	}
	w.requireNoAdvanceState(t, id, accepted)
}

func TestAdvanceHoldsAnUpstreamedPatch(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	name := w.addPatch(t, "responder", replaceLine("v08", "v08 patched"))
	upstream := vendorUpstream(t, w, w.seed, replaceLine("v08", "v08 patched"))
	id := "advance-" + upstream[:12]
	accepted := w.head(t)
	_, _, _, err := w.broker.ProposeAdvance(ctx, id, w.seed, upstream)
	hold := holdOf(t, err)
	if hold.Patch.Kind != patchset.KindUpstreamed || hold.Patch.Patch != name || hold.Owner != "responder" {
		t.Fatalf("hold = %+v / %+v", hold, hold.Patch)
	}
	w.requireNoAdvanceState(t, id, accepted)
}

// Patches stack: both re-apply on an unrelated upstream change, and a change
// that breaks only the second patch's context holds that patch by name.
func TestAdvanceReappliesAStackAndHoldsTheBrokenLink(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	first := w.addPatch(t, "deployer", replaceLine("v04", "v04 patched"))
	second := w.addPatch(t, "responder", replaceLine("v05", "v05 patched twice"))
	if first == second {
		t.Fatal("patch names are not distinct")
	}
	benign := vendorUpstream(t, w, w.seed, replaceLine("v15", "v15 upstream"))
	benignID := "advance-" + benign[:12]
	proposal, candidate, _, err := w.broker.ProposeAdvance(ctx, benignID, w.seed, benign)
	if err != nil || candidate == "" {
		t.Fatalf("ProposeAdvance of the benign change = %q %v", candidate, err)
	}
	tree := gitOut(t, proposal.Path, "rev-parse", candidate+"^{tree}")
	if got := effectiveFile(t, proposal.Path, tree, "lib/vendor.txt"); !strings.Contains(got, "v04 patched\n") || !strings.Contains(got, "v05 patched twice\n") || !strings.Contains(got, "v15 upstream\n") {
		t.Fatalf("effective vendor.txt = %q", got)
	}
	if err := w.broker.ReleaseAdvance(benignID); err != nil {
		t.Fatal(err)
	}
	// v08 is in the second patch's context window (v02..v08) but not the first's (v01..v07).
	broken := vendorUpstream(t, w, w.seed, replaceLine("v08", "v08 upstream"))
	brokenID := "advance-" + broken[:12]
	accepted := w.head(t)
	_, _, _, err = w.broker.ProposeAdvance(ctx, brokenID, w.seed, broken)
	hold := holdOf(t, err)
	if hold.Patch.Kind != patchset.KindConflict || hold.Patch.Patch != second || hold.Owner != "responder" {
		t.Fatalf("hold = %+v / %+v, want %s to conflict", hold, hold.Patch, second)
	}
	w.requireNoAdvanceState(t, brokenID, accepted)
}

// The broker recomputes at publish: a deployed tree whose patches do not apply
// is refused even though the proposal for another revision existed.
func TestPublishAdvanceRefusesATreeWhosePatchesDoNotApply(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	w.addPatch(t, "deployer", replaceLine("v08", "v08 patched"))
	broken := vendorUpstream(t, w, w.seed, replaceLine("v08", "v08 rewritten upstream"))
	id := "advance-" + broken[:12]
	// What a deployer that skipped the broker's proposal check would attest:
	// the merge of the broken revision in a private clone.
	stage := filepath.Join(t.TempDir(), "stage")
	testutil.Git(t, filepath.Dir(stage), "clone", "-q", "--no-local", w.accepted, stage)
	if conflicts, err := w.broker.mergeUpstream(ctx, stage, w.seed, broken); err != nil || len(conflicts) > 0 {
		t.Fatalf("merge = %v %v", conflicts, err)
	}
	evidence := advanceEvidence(t, stage, "skipped")
	if err := w.broker.locked(func() error {
		_, err := w.broker.createProposal(ctx, id, AdvancePhase, "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	accepted := w.head(t)
	_, err := w.broker.PublishAdvance(ctx, id, broken, w.seed, evidence)
	hold := holdOf(t, err)
	if hold.Patch.Kind != patchset.KindConflict {
		t.Fatalf("hold = %+v", hold.Patch)
	}
	if w.head(t) != accepted {
		t.Fatal("a tree whose patches do not apply was accepted")
	}
}

// A repository whose onboarding baked source edits into the tree keeps the
// three-way merge and records that it does not mirror its upstream.
func TestAdvanceRecordsALegacyMirror(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, true)
	upstream := vendorUpstream(t, w, w.seed, replaceLine("v14", "v14 upstream"))
	id := "advance-" + upstream[:12]
	proposal, candidate, _, err := w.broker.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil || candidate == "" {
		t.Fatalf("ProposeAdvance = %q %v", candidate, err)
	}
	receipt, err := w.broker.PublishAdvance(ctx, id, upstream, w.seed, advanceEvidence(t, proposal.Path, "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	if message := "\n" + gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit) + "\n"; !strings.Contains(message, "\nSDO-Mirror: legacy\n") {
		t.Fatalf("advance commit does not record the legacy state:\n%s", message)
	}
}

// Negative control for the invariant: a merge result that is not the upstream's
// source tree, in a repository that mirrored, is an error and never accepted.
func TestMirrorStateRefusesAMergeThatIsNotTheUpstreamTree(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	upstream := vendorUpstream(t, w, w.seed, replaceLine("v14", "v14 upstream"))
	stage := filepath.Join(t.TempDir(), "stage")
	testutil.Git(t, filepath.Dir(stage), "clone", "-q", "--no-local", w.accepted, stage)
	testutil.Git(t, stage, "fetch", "-q", "--no-tags", w.accepted, upstream)
	head := gitOut(t, stage, "rev-parse", "HEAD")
	if _, err := mirrorState(ctx, stage, head, w.seed, upstream); err == nil || !strings.Contains(err.Error(), "mirror invariant") {
		t.Fatalf("mirrorState of an unmerged checkout = %v", err)
	}
	if conflicts, err := w.broker.mergeUpstream(ctx, stage, w.seed, upstream); err != nil || len(conflicts) > 0 {
		t.Fatalf("merge = %v %v", conflicts, err)
	}
	if state, err := mirrorState(ctx, stage, head, w.seed, upstream); err != nil || state != MirrorExact {
		t.Fatalf("mirrorState after the merge = %q %v", state, err)
	}
}
