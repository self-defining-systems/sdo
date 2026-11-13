package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	kclock "k8s.io/utils/clock"
	"os"
	"path/filepath"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
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
	// Clock and NewReplyID may be injected before Serve; nil selects production sources.
	Clock        kclock.WithTicker
	NewReplyID   func() string
	Directory    string
	Handle       func(context.Context, ToolRequest) ToolResponse
	PollInterval time.Duration
	// MaxRequestBytes defaults to 64 KiB. Reflection decisions use their own bounded envelope.
	MaxRequestBytes int64

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
	ticker := timerClock(s.Clock).NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.process(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
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
		// Clients own cancellation cleanup and may disappear after ReadDir.
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		limit := s.MaxRequestBytes
		if limit <= 0 {
			limit = 64 * 1024
		}
		data, err := io.ReadAll(io.LimitReader(requestFile, limit+1))
		_ = requestFile.Close()
		if err != nil {
			return err
		}
		response := ToolResponse{}
		if int64(len(data)) > limit {
			response.Error = fmt.Sprintf("tool request exceeds %d KiB", limit/1024)
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
		newID := s.NewReplyID
		if newID == nil {
			newID = lifecycleio.NewReplyID
		}
		id := newID()
		if id == "" || strings.ContainsAny(id, "/\\") {
			return errors.New("reply ID must be a non-empty filename component")
		}
		temporary := ".reply-" + id
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
		// The client also removes its request after reading the response or cancelling.
		if err := s.requests.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// detect takes one fresh snapshot and runs only the selected deterministic
// detectors. It never diagnoses programs, prepares plans or consumes a batch.
func (l *ResponderLoop) detect(ctx context.Context, ids []string, finding string, response *ToolResponse) {
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
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	readView := program.ReadFrom(snapshot)
	if l.ReadView != nil {
		readView = l.ReadView(snapshot)
	}
	view := NewCycleReadView(readView)
	var detectors []sdk.Detector
	for _, detector := range l.Detectors {
		if len(selected) == 0 || selected[detector.Spec().ID] {
			detectors = append(detectors, detector)
		}
	}
	results := evaluateDetectors(ctx, detectors, view)
	for _, detector := range detectors {
		id := detector.Spec().ID
		if len(selected) > 0 && !selected[id] {
			continue
		}
		result := results[id]
		observed, err := result.findings, result.err
		state := DetectorEvaluation{DetectorID: id, EvaluatedAt: l.clock().UTC(), Status: DetectorEvaluationClear, Reason: detectorConfirmationReason(detector, view)}
		if err == nil && len(observed) == 0 && !detectorConfirmed(detector, view) {
			state.Status = DetectorEvaluationPending
		}
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
	response.Healthy = response.Error == "" && len(response.Findings) == 0 && len(response.DetectorStates) > 0
	for _, state := range response.DetectorStates {
		response.Healthy = response.Healthy && state.Status == DetectorEvaluationClear
	}
	response.Summary = fmt.Sprintf("%d detectors evaluated; %d current findings", len(response.DetectorStates), len(response.Findings)) + pendingEvaluationSummary(response.DetectorStates)
	if reads, ok := any(view).(detectReads); ok && len(selected) == 0 {
		now := l.clock().UTC()
		page := l.pageInput(reads, response.Findings, now)
		summary := buildDetectSummary(detectInput{Request: l.liveRequest(), Findings: response.Findings, View: reads, Now: now, Recovered: l.detectIncidentRecovered(), AppState: page.AppState})
		response.Text = summary.Text
		if finding != "" {
			text, found := summary.item(finding)
			if !found {
				response.Error = fmt.Sprintf("no current item matches %q; sdo detect lists the current ones", finding)
				return
			}
			response.Text = text
		}
	}
}

// pageInput is what the on-demand pages are built from.
func (l *ResponderLoop) pageInput(view detectReads, findings []sdk.Finding, now time.Time) incidentPageInput {
	in := incidentPageInput{Request: l.Request, Findings: findings, View: view, Now: now}
	if l.IncidentSources != nil {
		in.Signals, in.AppState = l.IncidentSources()
	}
	return in
}

// incidentPage answers `sdo changes` and `sdo timeline` from the controller's
// current state: the evidence the incident holds and a fresh snapshot.
func (l *ResponderLoop) incidentPage(ctx context.Context, request ToolRequest, response *ToolResponse) {
	var since time.Duration
	if request.Since != "" {
		parsed, err := time.ParseDuration(request.Since)
		if err != nil || parsed <= 0 {
			response.Error = fmt.Sprintf("--since %q is not a positive duration such as 30m", request.Since)
			return
		}
		since = parsed
	}
	if request.Limit < 0 {
		response.Error = "--limit must not be negative"
		return
	}
	if l.Provider == nil {
		response.Error = "fresh snapshot provider is unavailable"
		return
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	readView := program.ReadFrom(snapshot)
	if l.ReadView != nil {
		readView = l.ReadView(snapshot)
	}
	reads, ok := any(NewCycleReadView(readView)).(detectReads)
	if !ok {
		response.Error = "the snapshot cannot be read for " + request.Command
		return
	}
	in := l.pageInput(reads, l.Request.Findings, l.clock().UTC())
	in.Since, in.Limit = since, request.Limit
	if request.Command == "timeline" {
		response.Text = string(buildIncidentTimeline(in))
		return
	}
	response.Text = string(buildIncidentChanges(in))
}
