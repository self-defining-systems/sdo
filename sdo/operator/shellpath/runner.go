// Package shellpath runs accepted shell detectors. Decisions use explicit time;
// process, repository, watch and report transports stay at the edges.
package shellpath

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/layout"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DetectorDirectory = layout.DetectorsDir
	MaxOutputBytes    = 16 << 10
	ScriptTimeout     = 20 * time.Second
	SweepInterval     = 30 * time.Second
	EventDebounce     = 2 * time.Second
	StabilityWindow   = 15 * time.Second
	MaxParallel       = 4
)

type Script struct {
	Path  string `json:"path"`
	Class string `json:"class"`
}
type Result struct {
	Script              string        `json:"script"`
	ExitCode            int           `json:"exit_code"`
	Duration            time.Duration `json:"duration"`
	Stdout              string        `json:"stdout"`
	Stderr              string        `json:"stderr"`
	At                  time.Time     `json:"at"`
	Error               string        `json:"error,omitempty"`
	TimedOut            bool          `json:"timed_out,omitempty"`
	Truncated           bool          `json:"truncated,omitempty"`
	Fired               bool          `json:"fired"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
}
type Report struct {
	Version        int       `json:"version"`
	AcceptedCommit string    `json:"accepted_commit"`
	ActivatedAt    time.Time `json:"activated_at"`
	At             time.Time `json:"at"`
	Results        []Result  `json:"results"`
	Healthy        bool      `json:"healthy"`
	Error          string    `json:"error,omitempty"`
}
type Executor interface {
	Execute(context.Context, string, Script) Result
}

// Discover returns every executable regular .sh file, recursively, in the two
// role-owned directories. No interpreter metadata or authored manifest is required.
func Discover(root string) ([]Script, error) {
	base := filepath.Join(root, DetectorDirectory)
	scripts := []Script{}
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && p == base {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(base, p)
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("shell detector symlink forbidden: %s", rel)
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("shell detector is not a regular file: %s", rel)
		}
		if info.Mode().Perm()&0111 == 0 {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) < 3 || !layout.ValidDetectorID(parts[1]) || (parts[0] != "health" && parts[0] != "incidents") || !strings.HasSuffix(rel, ".sh") {
			return fmt.Errorf("executable detector must be a health or incidents shell script: %s", rel)
		}
		scripts = append(scripts, Script{Path: filepath.ToSlash(filepath.Join(DetectorDirectory, rel)), Class: parts[0]})
		return nil
	})
	sort.Slice(scripts, func(i, j int) bool { return scripts[i].Path < scripts[j].Path })
	return scripts, err
}

type Schedule struct{ sweep, event time.Time }

func NewSchedule(now time.Time) *Schedule { return &Schedule{sweep: now} }
func (s *Schedule) Event(now time.Time)   { s.event = now.Add(EventDebounce) }
func (s *Schedule) Due(now time.Time) bool {
	sweep := !now.Before(s.sweep)
	event := !s.event.IsZero() && !now.Before(s.event)
	if !sweep && !event {
		return false
	}
	if sweep {
		s.sweep = now.Add(SweepInterval)
	}
	s.event = time.Time{}
	return true
}

// State owns failure streaks. An execution error cannot fire or establish health.
type State struct {
	scripts    []Script
	results    map[string]Result
	clearSince time.Time
}

func NewState(scripts []Script) *State {
	return &State{scripts: append([]Script(nil), scripts...), results: map[string]Result{}}
}
func (s *State) Record(r Result) {
	previous := s.results[r.Script]
	if r.Error == "" && !r.TimedOut && r.ExitCode >= 0 && r.ExitCode != 0 {
		r.ConsecutiveFailures = previous.ConsecutiveFailures + 1
		r.Fired = r.ConsecutiveFailures >= 2
	} else {
		r.ConsecutiveFailures = 0
		r.Fired = false
	}
	s.results[r.Script] = r
	allClear := len(s.scripts) > 0
	for _, script := range s.scripts {
		result, exists := s.results[script.Path]
		if !exists || result.Error != "" || result.TimedOut || result.ExitCode != 0 {
			allClear = false
			break
		}
	}
	if !allClear {
		s.clearSince = time.Time{}
	} else if s.clearSince.IsZero() {
		s.clearSince = r.At
		for _, result := range s.results {
			if result.At.After(s.clearSince) {
				s.clearSince = result.At
			}
		}
	}
}
func (s *State) Results() []Result {
	out := make([]Result, 0, len(s.scripts))
	for _, script := range s.scripts {
		if r, ok := s.results[script.Path]; ok {
			out = append(out, r)
		}
	}
	return out
}
func (s *State) Healthy(at time.Time) bool {
	return !s.clearSince.IsZero() && !at.Before(s.clearSince.Add(StabilityWindow))
}

// RunBatch runs at most four processes concurrently and returns declaration order.
func RunBatch(ctx context.Context, root string, scripts []Script, executor Executor) []Result {
	results := make([]Result, len(scripts))
	next := make(chan int)
	var workers sync.WaitGroup
	for range min(MaxParallel, len(scripts)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range next {
				results[i] = executor.Execute(ctx, root, scripts[i])
			}
		}()
	}
	for i := range scripts {
		next <- i
	}
	close(next)
	workers.Wait()
	return results
}
