package runtime

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
)

type scheduleEntry struct {
	detector sdk.Detector
	nextRun  time.Time
}

// Scheduler owns its complete mutable schedule. Selection, acceleration,
// snapshot and restore share one lock so asynchronous durable handoffs cannot
// read a transition concurrently with the controller loop.
type Scheduler struct {
	mu        sync.RWMutex
	namespace string
	entries   map[string]*scheduleEntry
}

func NewScheduler(detectors []sdk.Detector, start time.Time, namespace ...string) *Scheduler {
	entries := make(map[string]*scheduleEntry, len(detectors))
	for _, detector := range detectors {
		entries[detector.Spec().ID] = &scheduleEntry{detector: detector, nextRun: start}
	}
	s := &Scheduler{entries: entries}
	if len(namespace) > 0 {
		s.namespace = namespace[0]
	}
	return s
}

func (s *Scheduler) Select(now time.Time, event *sdk.WatchKind) []sdk.Detector {
	if event == nil {
		return s.SelectEvents(now, nil)
	}
	return s.SelectEvents(now, []sdk.WatchKind{*event})
}

// SelectEvents returns every detector that is due at now or watches any of the
// coalesced events, and advances each selected detector's next interval run.
func (s *Scheduler) SelectEvents(now time.Time, events []sdk.WatchKind) []sdk.Detector {
	s.mu.Lock()
	defer s.mu.Unlock()
	selected := make([]sdk.Detector, 0)
	for _, entry := range s.entries {
		due := !now.Before(entry.nextRun)
		for _, event := range events {
			if due {
				break
			}
			due = watchesEventInNamespace(entry.detector.Spec().Watches, event, s.namespace)
		}
		if !due {
			continue
		}
		selected = append(selected, entry.detector)
		entry.nextRun = now.Add(entry.detector.Spec().Interval)
	}
	sort.Slice(selected, func(left int, right int) bool {
		return selected[left].Spec().ID < selected[right].Spec().ID
	})
	return selected
}

// Expedite moves a detector's next run earlier, never later.
func (s *Scheduler) Expedite(detectorID string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[detectorID]
	if !ok || !at.Before(entry.nextRun) {
		return
	}
	entry.nextRun = at
}

// ExpediteAll makes every detector due at or before at.
func (s *Scheduler) ExpediteAll(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		if at.Before(entry.nextRun) {
			entry.nextRun = at
		}
	}
}

func (s *Scheduler) NextRun() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var next time.Time
	for _, entry := range s.entries {
		if next.IsZero() || entry.nextRun.Before(next) {
			next = entry.nextRun
		}
	}
	return next
}

func (s *Scheduler) Deadlines() map[string]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]time.Time, len(s.entries))
	for id, entry := range s.entries {
		result[id] = entry.nextRun
	}
	return result
}

func (s *Scheduler) RestoreDeadlines(deadlines map[string]time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, deadline := range deadlines {
		entry, ok := s.entries[id]
		if !ok {
			return fmt.Errorf("state references unknown detector %q", id)
		}
		entry.nextRun = deadline
	}
	return nil
}

func watchesEvent(watches []sdk.WatchKind, event sdk.WatchKind) bool {
	for _, watch := range watches {
		if watch.APIVersion != event.APIVersion || watch.Kind != event.Kind {
			continue
		}
		if watch.Namespace == "" || watch.Namespace == event.Namespace {
			return true
		}
	}
	return false
}

func watchesEventInNamespace(watches []sdk.WatchKind, event sdk.WatchKind, app string) bool {
	if app == "" {
		return watchesEvent(watches, event)
	}
	// Synthetic namespaced events omit namespace and refer to the application.
	// Normalize the event as well as its watch; this never matches a foreign watch.
	if event.Namespace == "" && !(sdk.ObservationTarget{Kind: event.Kind}).Cluster() {
		event.Namespace = app
	}
	for _, w := range watches {
		if w.APIVersion != event.APIVersion || w.Kind != event.Kind {
			continue
		}
		namespace := w.Namespace
		if namespace == "" && !(sdk.ObservationTarget{Kind: w.Kind}).Cluster() {
			namespace = app
		}
		if namespace == event.Namespace {
			return true
		}
	}
	return false
}
