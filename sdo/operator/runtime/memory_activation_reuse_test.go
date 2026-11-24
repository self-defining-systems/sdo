package runtime

import (
	"context"
	"testing"
)

func reuseActivation(builds *int, same bool) *memoryActivation {
	return &memoryActivation{
		active:  "host-commit",
		results: make(chan memoryCandidate, 1),
		notify:  make(chan struct{}, 1),
		build: func(_ context.Context, commit string) memoryCandidate {
			*builds++
			return memoryCandidate{commit: commit}
		},
		sameInputs: func(context.Context, string, string) (bool, error) { return same, nil },
	}
}

// A commit that leaves the compiled detector, playbook and library trees as they
// were (an outcome-only commit) is accepted without compiling or launching a host.
func TestAcceptedCommitWithUnchangedHostInputsReusesTheRunningHost(t *testing.T) {
	builds := 0
	activation := reuseActivation(&builds, true)
	ready, err := activation.refresh(context.Background(), "outcome-commit")
	if err != nil || !ready {
		t.Fatalf("refresh = %v, %v, want ready at once", ready, err)
	}
	if builds != 0 {
		t.Fatalf("built %d hosts for an outcome-only commit, want none", builds)
	}
	if activation.acceptedCommit() != "outcome-commit" || activation.hostCommit() != "host-commit" {
		t.Fatalf("accepted %q host %q, want the new accepted commit on the old host", activation.acceptedCommit(), activation.hostCommit())
	}
}

func TestAcceptedCommitWithChangedHostInputsBuildsANewHost(t *testing.T) {
	builds := 0
	activation := reuseActivation(&builds, false)
	if ready, err := activation.refresh(context.Background(), "program-commit"); err != nil || ready {
		t.Fatalf("refresh = %v, %v, want a build in flight", ready, err)
	}
	<-activation.notify
	if builds != 1 {
		t.Fatalf("built %d hosts, want one", builds)
	}
}
