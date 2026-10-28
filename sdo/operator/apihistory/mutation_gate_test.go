package apihistory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMutationGateFencesOnlyAfterPendingWritesResolve(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write, err := gate.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if observation, _ := gate.TryObserve(); observation != nil {
		observation.Close()
		t.Fatal("closure acquired during write")
	}
	if err := write.Finish(false); err != nil {
		t.Fatal(err)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("observe: %v", err)
	}
	at := time.Unix(42, 0).UTC()
	if err := observation.Fence(at); err != nil {
		t.Fatal(err)
	}
	observation.Close()
	if write, err := gate.Begin(); err == nil {
		write.Finish(false)
		t.Fatal("post-closure mutation admitted")
	}
	if got, err := gate.Verified(); err != nil || got == nil || !got.VerifiedAt.Equal(at) {
		t.Fatalf("warning: %+v %v", got, err)
	}
}

func TestMutationGateUncertainReceiptBlocksClosure(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write, err := gate.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Finish(true); err != nil {
		t.Fatal(err)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("observe: %v", err)
	}
	defer observation.Close()
	if !observation.Pending {
		t.Fatal("uncertain write was treated as resolved")
	}
}

// The controller's verification fence is what ends a running responder turn:
// the real Go publisher must be observed by the real Python worker predicate.
func TestVerificationFenceEndsTheResponderTurn(t *testing.T) {
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Skip("uv is required to run the responder worker predicate")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	gate, err := PrepareMutationGate(directory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	predicate := func() string {
		command := exec.CommandContext(ctx, uv, "run", "--no-sync", "python", "-c",
			"from sdo.agents.responder import codex\ncodex.VERIFIED_SETTLE_SECONDS = 0\nprint(codex.verified_recovery()())")
		command.Dir = root
		command.Env = append(os.Environ(), "SDO_API_HISTORY_DIR="+directory, "PYTHONPATH="+root)
		var stderr strings.Builder
		command.Stderr = &stderr
		output, err := command.Output()
		if err != nil {
			t.Fatalf("worker predicate: %v %s", err, stderr.String())
		}
		return strings.TrimSpace(string(output))
	}
	if got := predicate(); got != "False" {
		t.Fatalf("unverified incident ended the turn: %q", got)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("observe: %v", err)
	}
	if err := observation.Fence(time.Unix(42, 0)); err != nil {
		t.Fatal(err)
	}
	observation.Close()
	if got := predicate(); got != "True" {
		t.Fatalf("verified incident did not end the turn: %q", got)
	}
}
