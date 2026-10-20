package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SessionToolServer exchanges requests through the incident's narrow session
// PVC mount. The Job already has that mount, so the CLI needs no controller
// endpoint, service account token, or access to another incident's session.
type SessionToolServer struct {
	Directory    string
	Handle       func(context.Context, ToolRequest) ToolResponse
	PollInterval time.Duration
}

func (s SessionToolServer) Serve(ctx context.Context) error {
	if err := s.Prepare(); err != nil {
		return err
	}
	requests, responses := filepath.Join(s.Directory, "requests"), filepath.Join(s.Directory, "responses")
	interval := s.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.process(ctx, requests, responses); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s SessionToolServer) Prepare() error {
	if s.Directory == "" || s.Handle == nil {
		return fmt.Errorf("session tool directory and handler are required")
	}
	requests, responses := filepath.Join(s.Directory, "requests"), filepath.Join(s.Directory, "responses")
	for _, path := range []string{s.Directory, requests, responses} {
		if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("tool session path is not a real directory: %s", path)
		}
	}
	return nil
}

func (s SessionToolServer) process(ctx context.Context, requests, responses string) error {
	entries, err := os.ReadDir(requests)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := entry.Name()
		if len(name) > 90 {
			continue
		}
		responsePath := filepath.Join(responses, name)
		if _, err := os.Stat(responsePath); err == nil {
			_ = os.Remove(filepath.Join(requests, name))
			continue
		}
		data, err := os.ReadFile(filepath.Join(requests, name))
		if err != nil {
			return err
		}
		response := ToolResponse{}
		if len(data) > 64*1024 {
			response.Error = "tool request exceeds 64 KiB"
		} else {
			var request ToolRequest
			if err := json.Unmarshal(data, &request); err != nil {
				response.Error = "invalid tool request: " + err.Error()
			} else {
				response = s.Handle(ctx, request)
			}
		}
		if response.Summary == "" {
			response.Summary = response.Error
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return err
		}
		file, err := os.CreateTemp(responses, ".reply-*")
		if err != nil {
			return err
		}
		if _, err := file.Write(encoded); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return err
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(file.Name())
			return err
		}
		if err := os.Rename(file.Name(), responsePath); err != nil {
			_ = os.Remove(file.Name())
			return err
		}
		if err := os.Remove(filepath.Join(requests, name)); err != nil {
			return err
		}
	}
	return nil
}
