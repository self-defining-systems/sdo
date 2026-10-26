package broker

import (
	"context"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func TestBrokerGitWaitsForAutomaticMaintenance(t *testing.T) {
	root := repository(t)
	for _, key := range []string{"gc.autoDetach", "maintenance.autoDetach"} {
		// Even an existing accepted repository may enable detached maintenance.
		testutil.Git(t, root, "config", "--local", key, "true")
		got, err := gitText(context.Background(), root, "config", "--get", key)
		if err != nil || got != "false" {
			t.Errorf("broker command permits detached %s: value=%q error=%v", key, got, err)
		}
	}
}

func TestBrokerClonesKeepMaintenanceSynchronous(t *testing.T) {
	root := repository(t)
	b, err := New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	stage, cleanup, err := b.privateClone(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	proposal, err := b.CreateProposal(context.Background(), "clone-lifetime")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stage, proposal.Path} {
		for _, key := range []string{"gc.autoDetach", "maintenance.autoDetach"} {
			if got := testutil.Git(t, path, "config", "--local", "--get", key); got != "false" {
				t.Errorf("clone %s permits detached %s: %q", path, key, got)
			}
		}
	}
}
