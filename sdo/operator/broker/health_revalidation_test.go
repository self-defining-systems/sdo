package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHealthRevalidationRequiresIndependentValidationAndRecordsNoop(t *testing.T) {
	ctx := context.Background()
	b, root := judgeBroker(t)
	p, err := b.CreateProposalForPhase(ctx, "initial", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, p.Path)
	if _, err := b.Publish(ctx, "initial", HealthJudge, InitialPhase); err != nil {
		t.Fatal(err)
	}
	base := git(t, root, "rev-parse", "HEAD")
	id := "health-refresh-noop"
	if _, err := b.CreateProposalForPhase(ctx, id, InitialPhase); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("independent validation failed")
	calls := 0
	b.validator = func(context.Context, string, []string) error { calls++; return sentinel }
	if _, err := b.Publish(ctx, id, HealthJudge, InitialPhase); !errors.Is(err, sentinel) {
		t.Fatalf("no-op bypassed validator: %v", err)
	}
	if calls != 1 || git(t, root, "rev-parse", "HEAD") != base {
		t.Fatal("failed no-op changed accepted state")
	}
	b.validator = func(context.Context, string, []string) error { calls++; return nil }
	receipt, err := b.Publish(ctx, id, HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || receipt.Commit == base || len(receipt.ChangedPaths) != 0 || calls != 2 {
		t.Fatalf("no attributed no-op acceptance: %+v calls=%d", receipt, calls)
	}
	if git(t, root, "rev-parse", receipt.Commit+"^{tree}") != git(t, root, "rev-parse", base+"^{tree}") {
		t.Fatal("no-op changed policy bytes")
	}
	again, err := b.Publish(ctx, id, HealthJudge, InitialPhase)
	if err != nil || again.Commit != receipt.Commit || calls != 2 {
		t.Fatalf("acceptance not reused: %+v %v", again, err)
	}
	// A pre-existing malformed policy must be checked even with an empty diff.
	if err := os.WriteFile(filepath.Join(root, ".sdo/goal.md"), []byte("broken goal"), 0644); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "invalid seed")
	if _, err := b.CreateProposalForPhase(ctx, "health-refresh-invalid", InitialPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "health-refresh-invalid", HealthJudge, InitialPhase); err == nil {
		t.Fatal("no-op accepted malformed existing policy")
	}
	if calls != 2 {
		t.Fatal("invalid no-op reached independent execution")
	}
}

func TestNoopHealthValidationStillRequiresIsolatedReceipt(t *testing.T) {
	called := false
	validator := JobValidator(validationJobFunc(func(context.Context, ValidationJobRequest) (ValidationJobReceipt, error) {
		called = true
		return ValidationJobReceipt{}, nil
	}))
	if err := validator(context.Background(), repository(t), nil); err == nil || !called {
		t.Fatalf("no-op skipped independent compilation: called=%v err=%v", called, err)
	}
}
