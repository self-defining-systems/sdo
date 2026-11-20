//go:build integration

package env

import (
	"context"
	"errors"
	"testing"
)

func TestVclusterLaneHasNoNodes(t *testing.T) {
	t.Setenv("FUZZOPS_LANE_PROVIDER", "vcluster")
	t.Setenv("FUZZOPS_ENTRY_HOST", "10.1.2.3")
	e := Env{}.WithDefaults()
	if e.HasNodes() || e.LaneProvider != ProviderVcluster || e.EntryHost != "10.1.2.3" {
		t.Fatalf("got %+v", e)
	}
	var calls []call
	e = e.WithRunner(fake("", nil, &calls))
	if _, err := e.NodeExec(context.Background(), "n1", "true"); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("NodeExec err = %v, want ErrNoNodes", err)
	}
	if _, err := e.NodeExecStdin(context.Background(), "n1", nil, "true"); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("NodeExecStdin err = %v, want ErrNoNodes", err)
	}
	if len(calls) != 0 {
		t.Fatalf("a node command ran: %v", calls)
	}
}

func TestKindLaneKeepsItsNodes(t *testing.T) {
	t.Setenv("FUZZOPS_LANE_PROVIDER", "")
	var calls []call
	e := Env{}.WithRunner(fake("ok", nil, &calls))
	if !e.HasNodes() {
		t.Fatal("a kind lane has nodes")
	}
	if _, err := e.NodeExec(context.Background(), "n1", "true"); err != nil || len(calls) != 1 || calls[0].cmd[0] != "docker" {
		t.Fatalf("calls %v err %v", calls, err)
	}
}
