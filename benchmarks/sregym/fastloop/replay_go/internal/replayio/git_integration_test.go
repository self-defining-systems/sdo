//go:build integration

package replayio

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitUsesRequestedCheckout(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	if err := Git(ctx, "", "init", "--quiet", root); err != nil {
		t.Fatal(err)
	}
	if err := Git(ctx, root, "config", "replay.fixture", "expected"); err != nil {
		t.Fatal(err)
	}
	if err := Git(ctx, root, "config", "--get", "replay.fixture", "expected"); err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	if err := Git(ctx, "", "init", "--quiet", other); err != nil {
		t.Fatal(err)
	}
	if err := Git(ctx, other, "config", "--get", "replay.fixture"); err == nil {
		t.Fatal("configuration escaped its checkout")
	}
}

func TestGitRetainsFailureContextAndCancellation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	if err := Git(context.Background(), root, "status"); err == nil || !strings.Contains(err.Error(), "git status:") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing actionable command/output context: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Git(ctx, "", "--version"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("cancellation: %v", err)
	}
}
