// Package buildtools carries the deployer agent's turn-time image builds from
// its worker pod to the operate process that holds the deploy identity (the
// BuildKit image, the registry push Secret and the builder service account).
// The transport is a pair of directories on the shared repository volume:
// the client writes requests/<id>.json, the server answers in
// responses/<id>.json. A request names images only; the checkout, the plan
// and every credential are chosen by the server for the task it serves.
package buildtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/safefs"
)

// EnvDirectory names the tool directory in the worker's environment.
const EnvDirectory = "SDO_TOOLS_DIR"

// PodDirectory is where a worker pod sees the directory.
const PodDirectory = "/sdo/session/tools"

// Request asks for a build of the named images (none: every image Go builds).
type Request struct {
	Images []string `json:"images,omitempty"`
}

// Response is the build report text, or why nothing could be built. Failed
// is a build that ran and at least one image failed.
type Response struct {
	Text   string `json:"text,omitempty"`
	Failed bool   `json:"failed,omitempty"`
	Error  string `json:"error,omitempty"`
}

// BuildFunc runs one build for the served task: the report text, whether an
// image failed, or an error when nothing could be built.
type BuildFunc func(ctx context.Context, images []string) (text string, failed bool, err error)

// Server answers build requests in Directory until its context ends, one at a
// time.
type Server struct {
	Directory string
	Build     BuildFunc
	// Wait pauses between directory scans; nil is a real timer.
	Wait func(context.Context, time.Duration) error
}

const maxRequestBytes = 64 << 10

// Prepare creates the request and response directories.
func (s Server) Prepare() error {
	if s.Directory == "" || s.Build == nil {
		return errors.New("buildtools: directory and build function are required")
	}
	for _, sub := range []string{"requests", "responses"} {
		if err := safefs.MkdirAll(s.Directory, sub, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// Serve answers requests until ctx ends.
func (s Server) Serve(ctx context.Context) error {
	if err := s.Prepare(); err != nil {
		return err
	}
	for {
		if err := s.process(ctx); err != nil {
			return err
		}
		if err := wait(ctx, s.Wait, 250*time.Millisecond); err != nil {
			return nil
		}
	}
}

func (s Server) process(ctx context.Context) error {
	requests := filepath.Join(s.Directory, "requests")
	entries, err := os.ReadDir(requests)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") || len(name) > 90 {
			continue
		}
		data, err := readFile(s.Directory, path.Join("requests", name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		response := s.answer(ctx, data)
		if ctx.Err() != nil {
			return nil
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return err
		}
		if err := safefs.WriteFile(s.Directory, path.Join("responses", name), encoded, 0o600); err != nil {
			return err
		}
		_ = safefs.Remove(s.Directory, path.Join("requests", name))
	}
	return nil
}

func (s Server) answer(ctx context.Context, data []byte) Response {
	if len(data) > maxRequestBytes {
		return Response{Error: "request is too large"}
	}
	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		return Response{Error: "malformed request: " + err.Error()}
	}
	text, failed, err := s.Build(ctx, request.Images)
	if err != nil {
		return Response{Text: text, Error: err.Error()}
	}
	return Response{Text: text, Failed: failed}
}

// Call sends one request through directory and waits for its answer.
// newID names the request file; Wait pauses between polls (nil: a real timer).
func Call(ctx context.Context, directory string, request Request, newID func() string, pause func(context.Context, time.Duration) error) (Response, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return Response{}, err
	}
	name := "build-" + newID() + ".json"
	if _, err := os.Stat(filepath.Join(directory, "requests")); err != nil {
		return Response{}, fmt.Errorf("send build request (is this a deployer task?): %w", err)
	}
	if err := safefs.WriteFile(directory, path.Join("requests", name), encoded, 0o600); err != nil {
		return Response{}, fmt.Errorf("send build request: %w", err)
	}
	responsePath := path.Join("responses", name)
	defer func() { _ = safefs.Remove(directory, responsePath) }()
	for {
		data, err := readFile(directory, responsePath)
		if err == nil {
			var response Response
			if err := json.Unmarshal(data, &response); err != nil {
				return Response{}, fmt.Errorf("malformed build response: %w", err)
			}
			return response, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return Response{}, err
		}
		if err := wait(ctx, pause, 500*time.Millisecond); err != nil {
			_ = safefs.Remove(directory, path.Join("requests", name))
			return Response{}, err
		}
	}
}

// readFile reads rel below directory without following a link out of it.
func readFile(directory, rel string) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(rel)
}

func wait(ctx context.Context, pause func(context.Context, time.Duration) error, d time.Duration) error {
	if pause != nil {
		return pause(ctx, d)
	}
	return deploymentio.Wait(ctx, d)
}
