package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

// SessionToolServer exchanges requests through the incident's narrow session
// PVC mount. The Job already has that mount, so the CLI needs no controller
// endpoint, service account token, or access to another incident's session.
type SessionToolServer struct {
	Directory    string
	Handle       func(context.Context, ToolRequest) ToolResponse
	PollInterval time.Duration

	requests  *os.Root
	responses *os.Root
}

func (s *SessionToolServer) Serve(ctx context.Context) error {
	if err := s.Prepare(); err != nil {
		return err
	}
	defer s.Close()
	interval := s.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.process(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *SessionToolServer) Prepare() error {
	if s.Directory == "" || s.Handle == nil {
		return fmt.Errorf("session tool directory and handler are required")
	}
	// Repeated Prepare calls retain the original handles, including when Serve
	// follows an explicit Prepare before starting the worker.
	if s.requests != nil && s.responses != nil {
		for _, root := range []*os.Root{s.requests, s.responses} {
			info, err := root.Stat(".")
			if err != nil {
				return err
			}
			if err := validateToolDirectory(info); err != nil {
				return err
			}
		}
		return nil
	}
	parent, err := os.OpenRoot(filepath.Dir(s.Directory))
	if err != nil {
		return err
	}
	defer parent.Close()
	name := filepath.Base(s.Directory)
	if err := parent.Mkdir(name, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := parent.Lstat(name)
	if err != nil {
		return err
	}
	if err := validateToolDirectory(info); err != nil {
		return err
	}
	directory, err := parent.OpenRoot(name)
	if err != nil {
		return err
	}
	defer directory.Close()
	roots := make([]*os.Root, 0, 2)
	for _, name := range []string{"requests", "responses"} {
		if err := directory.Mkdir(name, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		info, err := directory.Lstat(name)
		if err != nil {
			return err
		}
		if err := validateToolDirectory(info); err != nil {
			return err
		}
		root, err := directory.OpenRoot(name)
		if err != nil {
			return err
		}
		roots = append(roots, root)
		// Close even a partially prepared set if the next directory fails.
		defer func() {
			if s.requests == nil {
				_ = root.Close()
			}
		}()
		info, err = root.Stat(".")
		if err != nil {
			return err
		}
		if err := validateToolDirectory(info); err != nil {
			return err
		}
	}
	s.requests, s.responses = roots[0], roots[1]
	return nil
}

func validateToolDirectory(info os.FileInfo) error {
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0007 != 0 {
		return fmt.Errorf("tool session path is not a real private directory: %s", info.Name())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("tool session path has unexpected owner: %s", info.Name())
	}
	return nil
}

// Close releases prepared directory handles after serving has stopped.
func (s *SessionToolServer) Close() error {
	var errs []error
	for _, root := range []*os.Root{s.requests, s.responses} {
		if root != nil {
			errs = append(errs, root.Close())
		}
	}
	s.requests, s.responses = nil, nil
	return errors.Join(errs...)
}

func (s *SessionToolServer) process(ctx context.Context) error {
	if s.requests == nil || s.responses == nil {
		return fmt.Errorf("tool server is not prepared")
	}
	directory, err := s.requests.Open(".")
	if err != nil {
		return err
	}
	entries, err := directory.ReadDir(-1)
	_ = directory.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := entry.Name()
		if len(name) > 90 {
			continue
		}
		if _, err := s.responses.Stat(name); err == nil {
			_ = s.requests.Remove(name)
			continue
		}
		requestFile, err := s.requests.Open(name)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(requestFile, 64*1024+1))
		_ = requestFile.Close()
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
		temporary := ".reply-" + rand.Text()
		file, err := s.responses.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		if _, err := file.Write(encoded); err != nil {
			_ = file.Close()
			_ = s.responses.Remove(temporary)
			return err
		}
		if err := file.Close(); err != nil {
			_ = s.responses.Remove(temporary)
			return err
		}
		if err := s.responses.Rename(temporary, name); err != nil {
			_ = s.responses.Remove(temporary)
			return err
		}
		if err := s.requests.Remove(name); err != nil {
			return err
		}
	}
	return nil
}

// detect takes one fresh snapshot and runs only the selected deterministic
// detectors. It never diagnoses programs, prepares plans or consumes a batch.
func (l *ResponderLoop) detect(ctx context.Context, ids []string, response *ToolResponse) {
	selected := map[string]bool{}
	valid := make([]string, 0, len(l.Detectors))
	for _, detector := range l.Detectors {
		valid = append(valid, detector.Spec().ID)
	}
	sort.Strings(valid)
	for _, id := range ids {
		if !slices.Contains(valid, id) {
			response.Error = fmt.Sprintf("unknown detector %q; valid detector IDs: %s", id, strings.Join(valid, ", "))
			return
		}
		selected[id] = true
	}
	if l.Provider == nil {
		response.Error = "fresh detector snapshot provider is unavailable"
		return
	}
	view, err := l.Provider.Snapshot(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	for _, detector := range l.Detectors {
		id := detector.Spec().ID
		if len(selected) > 0 && !selected[id] {
			continue
		}
		observed, err := sdk.DetectObserved(ctx, detector, view)
		state := DetectorEvaluation{DetectorID: id, EvaluatedAt: time.Now().UTC(), Status: DetectorEvaluationClear}
		if err != nil {
			state.Status = DetectorEvaluationError
			state.Error = err.Error()
			response.Error += fmt.Sprintf("detector %s: %v; ", id, err)
		} else {
			for _, finding := range observed {
				if finding.Status == sdk.FindingActive {
					if finding.DetectorID == "" {
						finding.DetectorID = id
					}
					finding.Fingerprint = FindingFingerprint(finding)
					response.Findings = append(response.Findings, finding)
					state.Status = DetectorEvaluationFiring
					state.Fingerprints = append(state.Fingerprints, finding.Fingerprint)
				}
			}
		}
		response.DetectorStates = append(response.DetectorStates, state)
	}
	sortFindings(response.Findings)
	response.Summary = fmt.Sprintf("%d detectors evaluated; %d current findings", len(response.DetectorStates), len(response.Findings))
}
