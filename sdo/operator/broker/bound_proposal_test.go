package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestBoundProposalIdentityIsolationAndConcurrentRejoin(t *testing.T) {
	ctx := context.Background()
	root := repository(t)
	b, err := New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	base := git(t, root, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(root, "peer.txt"), []byte("later accepted peer state\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "synthetic peer publication")
	head := git(t, root, "rev-parse", "HEAD")
	// The accepted working tree may be in the middle of a peer's update.
	// Bound creation consumes only committed objects, never those mutable bytes.
	if err := os.WriteFile(filepath.Join(root, "peer.txt"), []byte("uncommitted peer state\n"), 0600); err != nil {
		t.Fatal(err)
	}
	type result struct {
		proposal Proposal
		err      error
	}
	results := make(chan result, 8)
	var workers sync.WaitGroup
	for range cap(results) {
		workers.Go(func() {
			p, err := b.CreateProposalAt(ctx, "incident", base)
			results <- result{p, err}
		})
	}
	workers.Wait()
	close(results)
	var proposal Proposal
	for got := range results {
		if got.err != nil {
			t.Fatal(got.err)
		}
		if proposal.Path != "" && proposal != got.proposal {
			t.Fatalf("concurrent rejoin changed identity: %+v != %+v", proposal, got.proposal)
		}
		proposal = got.proposal
	}
	if proposal.BaseCommit != base || git(t, proposal.Path, "rev-parse", "HEAD") != base {
		t.Fatalf("bound proposal changed revision: %+v", proposal)
	}
	if _, err := os.Stat(filepath.Join(proposal.Path, "peer.txt")); !os.IsNotExist(err) {
		t.Fatalf("bound checkout contains later peer state: %v", err)
	}
	if git(t, proposal.Path, "remote") != "" || git(t, proposal.Path, "rev-parse", "--git-common-dir") != ".git" {
		t.Fatal("bound proposal lacks independent Git metadata or retains publishing authority")
	}
	if _, err := os.Stat(filepath.Join(proposal.Path, ".git/objects/info/alternates")); !os.IsNotExist(err) {
		t.Fatalf("bound proposal borrows accepted Git objects: %v", err)
	}
	working := filepath.Join(proposal.Path, "working.txt")
	if err := os.WriteFile(working, []byte("incident work\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateProposalAt(ctx, "incident", head); err == nil {
		t.Fatal("rejoin silently rebound an existing proposal")
	}
	if data, err := os.ReadFile(working); err != nil || string(data) != "incident work\n" {
		t.Fatalf("rejoin destroyed incident edits: %s %v", data, err)
	}
}

func TestBoundProposalRejectsNonAcceptedCommitIdentities(t *testing.T) {
	ctx := context.Background()
	root := repository(t)
	b, err := New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	base := git(t, root, "rev-parse", "HEAD")
	tree := git(t, root, "rev-parse", "HEAD^{tree}")
	unaccepted := git(t, root, "commit-tree", tree, "-p", base, "-m", "unaccepted candidate")
	for _, invalid := range []string{"", "HEAD", base[:12], base + "^{}", strings.Repeat("0", 40), tree, unaccepted} {
		t.Run(invalid, func(t *testing.T) {
			if _, err := b.CreateProposalAt(ctx, "incident", invalid); err == nil {
				t.Fatalf("accepted unbound commit identity %q", invalid)
			}
			if _, err := b.ProposalInfo("incident", ProposalPhase); !os.IsNotExist(err) {
				t.Fatalf("invalid base left a proposal: %v", err)
			}
		})
	}
}
