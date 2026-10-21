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
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
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

func TestSessionToolServerRepreparesSharedWorkerDirectories(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "tools")
	server := SessionToolServer{Directory: directory, Handle: func(context.Context, ToolRequest) ToolResponse { return ToolResponse{} }}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{directory, filepath.Join(directory, "requests"), filepath.Join(directory, "responses")} {
		if err := os.Chmod(path, 02770); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Prepare(); err != nil {
		t.Fatalf("reprepare shared directories: %v", err)
	}
}

func TestSessionToolServerRejectsWorldWritableDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "tools")
	server := SessionToolServer{Directory: directory, Handle: func(context.Context, ToolRequest) ToolResponse { return ToolResponse{} }}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(directory, "responses"), 0777); err != nil {
		t.Fatal(err)
	}
	if err := server.Prepare(); err == nil {
		t.Fatal("world-writable response directory accepted")
	}
}

func TestSessionToolServerIgnoresAtomicRequestTemporaryFiles(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "tools")
	called := false
	server := SessionToolServer{Directory: directory, Handle: func(context.Context, ToolRequest) ToolResponse {
		called = true
		return ToolResponse{Summary: "handled"}
	}}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(directory, "requests", ".call.json")
	if err := os.WriteFile(temporary, []byte(`{"command":"status"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := server.process(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("atomic request temporary file was executed before rename")
	}
	if _, err := os.Stat(filepath.Join(directory, "responses", ".call.json")); !os.IsNotExist(err) {
		t.Fatalf("temporary request produced a response: %v", err)
	}
}

// Replacing any directory after Prepare must not redirect controller I/O into
// accepted state. Keep a real queued call to exercise response creation/rename
// and an outside reply to exercise the duplicate-request removal path.
func TestSessionToolServerRetainsPreparedDirectories(t *testing.T) {
	for _, replaced := range []string{"tools", "requests", "responses"} {
		t.Run(replaced, func(t *testing.T) {
			parent := t.TempDir()
			directory := filepath.Join(parent, "tools")
			calls := 0
			server := SessionToolServer{Directory: directory, Handle: func(context.Context, ToolRequest) ToolResponse {
				calls++
				return ToolResponse{Summary: "ready"}
			}}
			t.Cleanup(func() { _ = server.Close() })
			if err := server.Prepare(); err != nil {
				t.Fatal(err)
			}
			request := []byte(`{"command":"status"}`)
			for _, name := range []string{"call.json", "duplicate.json"} {
				if err := os.WriteFile(filepath.Join(directory, "requests", name), request, 0600); err != nil {
					t.Fatal(err)
				}
			}
			outside := t.TempDir()
			// A replacement tools directory needs both children to look valid.
			for _, child := range []string{"requests", "responses"} {
				if err := os.Mkdir(filepath.Join(outside, child), 0700); err != nil {
					t.Fatal(err)
				}
			}
			for _, dir := range []string{outside, filepath.Join(outside, "requests"), filepath.Join(outside, "responses")} {
				if err := os.WriteFile(filepath.Join(dir, "accepted.json"), request, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "duplicate.json"), request, 0600); err != nil {
					t.Fatal(err)
				}
			}
			original := directory
			if replaced != "tools" {
				original = filepath.Join(directory, replaced)
			}
			if err := os.Rename(original, original+"-saved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, original); err != nil {
				t.Fatal(err)
			}
			// Serve calls Prepare again; it must keep the original roots too.
			if err := server.Prepare(); err != nil {
				t.Fatal(err)
			}
			if err := server.process(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Errorf("handled %d calls; want only the 2 prepared requests", calls)
			}
			for _, dir := range []string{outside, filepath.Join(outside, "requests"), filepath.Join(outside, "responses")} {
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				want := 2
				if dir == outside {
					want = 4
				}
				if len(entries) != want {
					t.Errorf("outside directory changed: %s: %v", dir, entries)
				}
				for _, name := range []string{"accepted.json", "duplicate.json"} {
					data, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil || string(data) != string(request) {
						t.Errorf("outside file changed: %s/%s: %s, %v", dir, name, data, err)
					}
				}
			}
		})
	}
}
