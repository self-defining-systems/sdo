package runtime

import (
	"sort"
	"time"

	"sdo.dev/operator/sdk"
)

type Batcher struct {
	findings        map[string]sdk.Finding
	deadlines       map[string]time.Time
	defaultDebounce time.Duration
	deadline        time.Time
}

type BatcherState struct {
	Findings  []sdk.Finding
	Deadlines map[string]time.Time
	Deadline  time.Time
}

func NewBatcher() *Batcher {
	return NewDebouncedBatcher(0)
}

func NewDebouncedBatcher(debounce time.Duration) *Batcher {
	return &Batcher{
		findings: make(map[string]sdk.Finding), deadlines: make(map[string]time.Time), defaultDebounce: debounce,
	}
}

func (b *Batcher) Add(finding sdk.Finding) {
	b.AddAt(finding, time.Time{})
}

func (b *Batcher) AddAt(finding sdk.Finding, now time.Time) {
	fingerprint := FindingFingerprint(finding)
	finding.Fingerprint = fingerprint
	key := FindingStateKey(finding.DetectorID, fingerprint)
	b.findings[key] = finding
	if _, exists := b.deadlines[key]; !exists {
		deadline := b.deadline
		if len(b.deadlines) == 0 {
			deadline = time.Time{}
			if !now.IsZero() && b.defaultDebounce > 0 {
				deadline = now.Add(b.defaultDebounce)
			}
		}
		b.deadlines[key] = deadline
	}
	b.recomputeDeadline()
}

func (b *Batcher) AddAtWithDebounce(finding sdk.Finding, now time.Time, debounce time.Duration) {
	fingerprint := FindingFingerprint(finding)
	finding.Fingerprint = fingerprint
	key := FindingStateKey(finding.DetectorID, fingerprint)
	b.findings[key] = finding
	if _, exists := b.deadlines[key]; !exists {
		deadline := time.Time{}
		if !now.IsZero() && debounce > 0 {
			deadline = now.Add(debounce)
		}
		b.deadlines[key] = deadline
	}
	b.recomputeDeadline()
}

func (b *Batcher) Ready(now time.Time) bool {
	return len(b.findings) > 0 && (b.deadline.IsZero() || !now.Before(b.deadline))
}

func (b *Batcher) Deadline() (time.Time, bool) {
	return b.deadline, len(b.findings) > 0
}

func (b *Batcher) Snapshot() BatcherState {
	findings := make([]sdk.Finding, 0, len(b.findings))
	for _, finding := range b.findings {
		findings = append(findings, finding)
	}
	sortFindings(findings)
	deadlines := make(map[string]time.Time, len(b.deadlines))
	for key, deadline := range b.deadlines {
		deadlines[key] = deadline
	}
	return BatcherState{Findings: findings, Deadlines: deadlines, Deadline: b.deadline}
}

func (b *Batcher) Restore(state BatcherState) {
	b.findings = make(map[string]sdk.Finding, len(state.Findings))
	b.deadlines = make(map[string]time.Time, len(state.Findings))
	for _, finding := range state.Findings {
		key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
		b.findings[key] = finding
		if deadline, ok := state.Deadlines[key]; ok {
			b.deadlines[key] = deadline
		} else {
			b.deadlines[key] = state.Deadline
		}
	}
	b.recomputeDeadline()
}

func (b *Batcher) Drain() []sdk.Finding {
	result := make([]sdk.Finding, 0, len(b.findings))
	for _, finding := range b.findings {
		result = append(result, finding)
	}
	sortFindings(result)
	b.findings = make(map[string]sdk.Finding)
	b.deadlines = make(map[string]time.Time)
	b.deadline = time.Time{}
	return result
}

func (b *Batcher) DrainReady(now time.Time) []sdk.Finding {
	result := make([]sdk.Finding, 0)
	for key, finding := range b.findings {
		deadline := b.deadlines[key]
		if !deadline.IsZero() && now.Before(deadline) {
			continue
		}
		result = append(result, finding)
		delete(b.findings, key)
		delete(b.deadlines, key)
	}
	sortFindings(result)
	b.recomputeDeadline()
	return result
}

// RemoveKeys retracts findings that cleared while they were still waiting for
// their debounce deadline. FindingChanges uses the same detector-qualified
// keys, so identical fingerprints from independent detectors remain isolated.
func (b *Batcher) RemoveKeys(keys []string) {
	for _, key := range keys {
		delete(b.findings, key)
		delete(b.deadlines, key)
	}
	b.recomputeDeadline()
}

func (b *Batcher) recomputeDeadline() {
	b.deadline = time.Time{}
	first := true
	for _, deadline := range b.deadlines {
		if first || deadline.IsZero() || (!b.deadline.IsZero() && deadline.Before(b.deadline)) {
			b.deadline = deadline
			first = false
		}
		if b.deadline.IsZero() {
			return
		}
	}
}

func sortFindings(findings []sdk.Finding) {
	sort.Slice(findings, func(left int, right int) bool {
		if findings[left].Fingerprint != findings[right].Fingerprint {
			return findings[left].Fingerprint < findings[right].Fingerprint
		}
		return findings[left].DetectorID < findings[right].DetectorID
	})
}
