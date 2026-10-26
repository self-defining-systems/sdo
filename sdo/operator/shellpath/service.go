package shellpath

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Snapshot struct {
	Commit, Root string
	Scripts      []Script
}
type Repository interface {
	Current(context.Context) (Snapshot, error)
}

// Service serializes activation and detector batches. Tick and Now are injected;
// a test can drive the same loop without timers, processes or Kubernetes.
type Service struct {
	Repository           Repository
	Executor             Executor
	Reports              *ReportStore
	Events               <-chan struct{}
	Tick                 <-chan time.Time
	Now                  func() time.Time
	snapshot             Snapshot
	state                *State
	schedule             *Schedule
	activatedAt, lastRun time.Time
}

func (s *Service) Step(ctx context.Context, at time.Time, event bool) error {
	snapshot, err := s.Repository.Current(ctx)
	if err != nil {
		s.Reports.Set(Report{Version: 1, AcceptedCommit: s.snapshot.Commit, ActivatedAt: s.activatedAt, At: at, Error: "accepted detector activation unavailable: " + err.Error(), Results: []Result{}})
		return err
	}
	if snapshot.Commit != s.snapshot.Commit || s.state == nil {
		s.snapshot = snapshot
		s.state = NewState(snapshot.Scripts)
		s.schedule = NewSchedule(at)
		s.activatedAt = at
		s.lastRun = time.Time{}
	}
	if event {
		s.schedule.Event(at)
	}
	if s.schedule.Due(at) {
		for _, r := range RunBatch(ctx, s.snapshot.Root, s.snapshot.Scripts, s.Executor) {
			s.state.Record(r)
		}
		s.lastRun = at
		for _, r := range s.state.Results() {
			if r.At.After(s.lastRun) {
				s.lastRun = r.At
			}
		}
	}
	s.Reports.Set(Report{Version: 1, AcceptedCommit: s.snapshot.Commit, ActivatedAt: s.activatedAt, At: s.lastRun, Results: s.state.Results(), Healthy: s.state.Healthy(at)})
	return nil
}
func (s *Service) Run(ctx context.Context) error {
	if s.Repository == nil || s.Executor == nil || s.Reports == nil || s.Tick == nil {
		return errors.New("shell service requires repository, executor, report store and clock")
	}
	now := s.Now
	if now == nil {
		now = time.Now
	}
	_ = s.Step(ctx, now(), false)
	events := s.Events
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if s.schedule != nil {
				s.schedule.Event(now())
			}
		case at, ok := <-s.Tick:
			if !ok {
				return nil
			}
			_ = s.Step(ctx, at, false)
		}
	}
}

// ReportStore publishes an immutable, bounded observation; the endpoint has no
// mutation surface or Kubernetes write permission.
type ReportStore struct {
	mu     sync.RWMutex
	report Report
}

func (s *ReportStore) Set(report Report) {
	s.mu.Lock()
	defer s.mu.Unlock()
	report.Results = append([]Result{}, report.Results...)
	s.report = report
}
func (s *ReportStore) Get() Report {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.report
	r.Results = append([]Result{}, r.Results...)
	return r
}
func (s *ReportStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/results" {
		http.Error(w, "read-only detector results", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Get())
}

const MaxReportBytes = 4 << 20

// HTTPReader refuses stale results, redirects and oversized or malformed reports.
type HTTPReader struct {
	URL    string
	Client *http.Client
	Now    func() time.Time
}

func (r HTTPReader) Read(ctx context.Context) (Report, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return Report{}, err
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := copy.Do(request)
	if err != nil {
		return Report{}, fmt.Errorf("shell runner unavailable: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Report{}, fmt.Errorf("shell runner returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxReportBytes+1))
	if err != nil {
		return Report{}, err
	}
	if len(data) > MaxReportBytes {
		return Report{}, errors.New("shell runner report exceeds size cap")
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return Report{}, fmt.Errorf("invalid shell runner report: %w", err)
	}
	now := r.Now
	if now == nil {
		now = time.Now
	}
	age := now().Sub(report.At)
	if report.Version != 1 || !commitPattern.MatchString(report.AcceptedCommit) || report.At.IsZero() || age > SweepInterval+ScriptTimeout*4 || age < -5*time.Second {
		return Report{}, errors.New("shell runner report is stale or has invalid identity")
	}
	if report.Error != "" {
		return report, errors.New(report.Error)
	}
	return report, nil
}
