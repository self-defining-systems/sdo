package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionToolServerRepliesToIncidentScopedRequest(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	server := SessionToolServer{Directory: directory, Handle: func(_ context.Context, request ToolRequest) ToolResponse {
		return ToolResponse{Command: request.Command, Summary: "ready"}
	}, PollInterval: time.Millisecond}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Serve(ctx) }()
	requests := filepath.Join(directory, "requests")
	responses := filepath.Join(directory, "responses")
	data, _ := json.Marshal(ToolRequest{Command: "status"})
	if err := os.WriteFile(filepath.Join(requests, "call-1.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	var response ToolResponse
	for i := 0; i < 200; i++ {
		data, err := os.ReadFile(filepath.Join(responses, "call-1.json"))
		if err == nil {
			if err := json.Unmarshal(data, &response); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if response.Command != "status" || response.Summary != "ready" {
		t.Fatalf("response: %+v", response)
	}
}

func TestSessionToolServerRejectsSymlinkedRequestDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "tools"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(directory, "tools", "requests")); err != nil {
		t.Fatal(err)
	}
	server := SessionToolServer{Directory: filepath.Join(directory, "tools"), Handle: func(context.Context, ToolRequest) ToolResponse { return ToolResponse{} }}
	if err := server.Prepare(); err == nil {
		t.Fatal("symlinked tool directory accepted")
	}
}
