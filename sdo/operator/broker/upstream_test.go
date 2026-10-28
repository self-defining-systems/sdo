package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const upstreamBranch = "live/20261028-synthetic"

// upstreamLanes seeds a bare remote branch from one accepted fixture and gives
// two lanes private accepted clones of it, each with its own broker, as lanes
// on different hosts have.
func upstreamLanes(t *testing.T) (string, *Broker, *Broker, string, string) {
	t.Helper()
	seed, _, _ := sharedReflectionFixture(t)
	remote := filepath.Join(t.TempDir(), "memory.git")
	git(t, "", "init", "--quiet", "--bare", remote)
	git(t, seed, "push", "--quiet", remote, "HEAD:refs/heads/"+upstreamBranch)
	var roots []string
	var brokers []*Broker
	for _, lane := range []string{"lane-a", "lane-b"} {
		root := filepath.Join(t.TempDir(), lane, "accepted")
		git(t, "", "clone", "--quiet", "--branch", upstreamBranch, remote, root)
		b := sharedMemoryBroker(t, root, func(context.Context, string, []string) error { return nil })
		if err := b.SetUpstream(Upstream{URL: remote, Branch: upstreamBranch}); err != nil {
			t.Fatal(err)
		}
		roots, brokers = append(roots, root), append(brokers, b)
	}
	return remote, brokers[0], brokers[1], roots[0], roots[1]
}

func remoteHead(t *testing.T, remote string) string {
	t.Helper()
	return git(t, remote, "rev-parse", "refs/heads/"+upstreamBranch)
}

func syncLanes(t *testing.T, brokers ...*Broker) {
	t.Helper()
	for _, b := range brokers {
		if err := b.SyncUpstream(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUpstreamRefusesMissingOrSharedBranches(t *testing.T) {
	b := sharedMemoryBroker(t, repository(t), nil)
	for _, upstream := range []Upstream{{URL: "", Branch: "live/x"}, {URL: "git@github.com:org/app-sdo.git"}, {URL: "git@github.com:org/app-sdo.git", Branch: "main"}, {URL: "--upload-pack=x", Branch: "live/x"}, {URL: "git@github.com:org/app-sdo.git", Branch: "live/bad..name"}} {
		if err := b.SetUpstream(upstream); err == nil {
			t.Errorf("accepted upstream %+v", upstream)
		}
	}
}

// Two lanes with private accepted repositories publish concurrently through
// one remote branch. Every publication lands once, validated on the newest
// head, and both lanes converge on the same accepted tree.
func TestUpstreamConcurrentLanesConvergeOnOneHistory(t *testing.T) {
	remote, a, b, rootA, rootB := upstreamLanes(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "alpha", "Alpha learning")
	writeLearning(t, pb, "beta", "Beta learning")
	var mu sync.Mutex
	validatedOn := map[string][]string{}
	for name, lane := range map[string]*Broker{"lane-a": a, "lane-b": b} {
		lane.validator = func(_ context.Context, stage string, _ []string) error {
			mu.Lock()
			defer mu.Unlock()
			validatedOn[name] = append(validatedOn[name], git(t, stage, "rev-parse", "HEAD"))
			return nil
		}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, lane := range []struct {
		b        *Broker
		id, book string
	}{{a, "lane-a", "alpha"}, {b, "lane-b", "beta"}} {
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, err := publishLearning(lane.b, lane.id, lane.book); errs <- err }()
		go func() {
			defer wg.Done()
			<-start
			_, err := lane.b.RecordOutcome(context.Background(), lane.id+"-next", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: lane.id + "-next", Classification: "success"})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	syncLanes(t, a, b)
	head := remoteHead(t, remote)
	if git(t, rootA, "rev-parse", "HEAD") != head || git(t, rootB, "rev-parse", "HEAD") != head {
		t.Fatal("lanes did not converge on the remote head")
	}
	if git(t, rootA, "rev-parse", "HEAD^{tree}") != git(t, rootB, "rev-parse", "HEAD^{tree}") {
		t.Fatal("lane trees differ")
	}
	log := git(t, rootA, "log", "--first-parent", "--format=%s", head)
	for _, subject := range []string{"sdo(lane-a): validated reflection", "sdo(lane-b): validated reflection", "sdo(lane-a-next): validated outcome", "sdo(lane-b-next): validated outcome"} {
		if strings.Count(log, subject) != 1 {
			t.Fatalf("publication %q not accepted exactly once:\n%s", subject, log)
		}
	}
	if merges := git(t, rootA, "rev-list", "--merges", head); merges != "" {
		t.Fatalf("online lanes produced merge commits: %s", merges)
	}
	for _, id := range []string{"alpha", "beta"} {
		if _, err := os.Stat(filepath.Join(rootB, ".sdo/playbooks", id, "README.md")); err != nil {
			t.Error(err)
		}
	}
	for name, bases := range validatedOn {
		if len(bases) == 0 {
			t.Errorf("%s published without the shared check", name)
		}
	}
}

// A peer's publication on the remote makes an overlapping one a typed memory
// conflict, exactly as on one shared accepted checkout; the remote is unchanged
// and the corrected resubmission lands on top.
func TestUpstreamConflictIsRebasedAsLocalConcurrentPublish(t *testing.T) {
	remote, a, b, _, rootB := upstreamLanes(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "base", "Alpha intent")
	writeLearning(t, pb, "base", "Beta intent")
	first, err := publishLearning(a, "lane-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	_, err = publishLearning(b, "lane-b", "base")
	diagnostics, typed := RejectionDiagnostics(err)
	if !typed || len(diagnostics) == 0 || diagnostics[0].Rule != MemoryConflictRule || diagnostics[0].GetMemoryConflict().GetAcceptedCommit() != first.Commit {
		t.Fatalf("expected typed memory conflict against the peer's remote commit, got %v", err)
	}
	if remoteHead(t, remote) != first.Commit {
		t.Fatal("conflicting publication reached the remote")
	}
	rebased, err := b.RebaseProposal(context.Background(), "lane-b", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if rebased.BaseCommit != first.Commit {
		t.Fatalf("rebase base %s, want peer head %s", rebased.BaseCommit, first.Commit)
	}
	writeLearning(t, rebased, "base", "Alpha intent and Beta intent")
	second, err := publishLearning(b, "lane-b", "base")
	if err != nil {
		t.Fatal(err)
	}
	if remoteHead(t, remote) != second.Commit || git(t, rootB, "rev-parse", second.Commit+"^") != first.Commit {
		t.Fatal("resolved publication did not land on the peer's commit")
	}
}

// With the remote unreachable a lane keeps accepting locally; nothing is lost.
// When the remote returns, disjoint local and peer publications are joined and
// both lanes converge.
func TestUpstreamUnavailableKeepsPublishingLocallyAndCatchesUp(t *testing.T) {
	remote, a, b, rootA, rootB := upstreamLanes(t)
	offline := remote + ".offline"
	if err := os.Rename(remote, offline); err != nil {
		t.Fatal(err)
	}
	pb := reflectionProposal(t, b, "lane-b")
	writeLearning(t, pb, "beta", "Beta learning")
	local, err := publishLearning(b, "lane-b", "beta")
	if err != nil {
		t.Fatalf("publish failed while the remote was unreachable: %v", err)
	}
	if git(t, rootB, "rev-parse", "HEAD") != local.Commit {
		t.Fatal("offline publication was not accepted locally")
	}
	if err := os.Rename(offline, remote); err != nil {
		t.Fatal(err)
	}
	pa := reflectionProposal(t, a, "lane-a")
	writeLearning(t, pa, "alpha", "Alpha learning")
	peer, err := publishLearning(a, "lane-a", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	syncLanes(t, b, a)
	head := remoteHead(t, remote)
	if git(t, rootA, "rev-parse", "HEAD") != head || git(t, rootB, "rev-parse", "HEAD") != head {
		t.Fatal("lanes did not converge after the remote returned")
	}
	for _, commit := range []string{local.Commit, peer.Commit} {
		if _, err := runGit(context.Background(), rootA, "merge-base", "--is-ancestor", commit, head); err != nil {
			t.Fatalf("accepted commit %s lost: %v", commit, err)
		}
	}
	if git(t, rootA, "rev-parse", head+"^1") != peer.Commit {
		t.Fatal("remote first-parent history was rewritten")
	}
}

// Offline publications that overlap a peer's are never merged blindly or
// dropped: the lane keeps them and they are preserved on a side branch.
func TestUpstreamOverlappingOfflineHistoryIsKeptAndPreserved(t *testing.T) {
	remote, a, b, _, rootB := upstreamLanes(t)
	offline := remote + ".offline"
	if err := os.Rename(remote, offline); err != nil {
		t.Fatal(err)
	}
	pb := reflectionProposal(t, b, "lane-b")
	writeLearning(t, pb, "base", "Beta intent")
	local, err := publishLearning(b, "lane-b", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(offline, remote); err != nil {
		t.Fatal(err)
	}
	pa := reflectionProposal(t, a, "lane-a")
	writeLearning(t, pa, "base", "Alpha intent")
	peer, err := publishLearning(a, "lane-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	syncLanes(t, b)
	if git(t, rootB, "rev-parse", "HEAD") != local.Commit || remoteHead(t, remote) != peer.Commit {
		t.Fatal("overlapping offline history was merged or replaced")
	}
	if side := git(t, remote, "rev-parse", "refs/heads/"+upstreamBranch+"-unmerged-"+local.Commit[:12]); side != local.Commit {
		t.Fatalf("offline history not preserved on the remote: %s", side)
	}
}
