package runtime

import (
	"context"
	"errors"
	clocktesting "k8s.io/utils/clock/testing"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestResponderSessionIDsCanReplay(t *testing.T) {
	original, _ := trustedPortLoop(t, false, true, true)
	makePlan := func() string {
		loop, err := NewResponderLoopWithOptions(original.Request, original.Programs, original.Provider, original.Detectors, original.Executor, original.MaxBatches, ResponderLoopOptions{NewSessionID: func() (string, error) { return strings.Repeat("a", 32), nil }})
		if err != nil {
			t.Fatal(err)
		}
		loop.ReadView = original.ReadView
		response := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{"service": original.Request.Findings[0].PrimaryResource}})
		if response.Error != "" {
			t.Fatal(response.Error)
		}
		return response.PlanID
	}
	if first, second := makePlan(), makePlan(); first == "" || first != second {
		t.Fatalf("plan IDs %q, %q", first, second)
	}
}

func TestResponderSessionIDFailuresPreserveContext(t *testing.T) {
	original, _ := trustedPortLoop(t, false, true, true)
	sentinel := errors.New("ID source unavailable")
	_, err := NewResponderLoopWithOptions(original.Request, original.Programs, original.Provider, original.Detectors, original.Executor, 5, ResponderLoopOptions{NewSessionID: func() (string, error) { return "", sentinel }})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v", err)
	}
	for _, id := range []string{"", "../outside", strings.Repeat("g", 32)} {
		_, err := NewResponderLoopWithOptions(original.Request, original.Programs, original.Provider, original.Detectors, original.Executor, 5, ResponderLoopOptions{NewSessionID: func() (string, error) { return id, nil }})
		if err == nil {
			t.Fatalf("unsafe ID %q accepted", id)
		}
	}
}

func TestSessionReplyIDCollisionCannotOverwrite(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	server := SessionToolServer{Directory: root, NewReplyID: func() string { return "fixed" }, Handle: func(context.Context, ToolRequest) ToolResponse { return ToolResponse{Summary: "ready"} }}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	collision := filepath.Join(root, "responses", ".reply-fixed")
	if err := os.WriteFile(collision, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "requests", "call.json"), []byte(`{"command":"status"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := server.process(context.Background()); !errors.Is(err, os.ErrExist) {
		t.Fatalf("collision error %v", err)
	}
	data, err := os.ReadFile(collision)
	if err != nil || string(data) != "original" {
		t.Fatalf("collision contents %q, error %v", data, err)
	}
}

func TestSessionToolServerUsesInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Unix(123, 0))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := filepath.Join(t.TempDir(), "tools")
		server := SessionToolServer{Directory: root, Clock: clock, PollInterval: time.Second, NewReplyID: func() string { return "fixed" }, Handle: func(context.Context, ToolRequest) ToolResponse { cancel(); return ToolResponse{Summary: "clocked"} }}
		done := make(chan error, 1)
		go func() { done <- server.Serve(ctx) }()
		synctest.Wait()
		if err := os.WriteFile(filepath.Join(root, "requests", "call.json"), []byte(`{"command":"status"}`), 0600); err != nil {
			t.Fatal(err)
		}
		clock.Step(time.Second)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(root, "responses", "call.json"))
		if err != nil || !strings.Contains(string(data), "clocked") {
			t.Fatalf("reply %s, error %v", data, err)
		}
	})
}
