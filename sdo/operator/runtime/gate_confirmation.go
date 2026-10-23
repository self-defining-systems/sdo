package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

// GateConfirmationPolicy is the clear hysteresis of the responder's submit
// gate, kept apart from closure's.
//
// Closure counts a detector's scheduled and watch-driven evaluations
// (FindingStateTracker's clear threshold), so a detector with a long interval
// holds an incident open for up to that interval after a fix has landed. The
// gate answers a narrower question for `sdo incident status`: has every
// finding that blocks closure been observed clear on Evaluations consecutive
// fresh snapshots spanning at least Window? While a blocking finding is
// clearing, its detector is re-evaluated every Interval for the gate only;
// those evaluations never reach the finding tracker, the detector history, or
// the configuration baseline, so closure keeps the health judge's policy.
// Any firing observation, scheduled or not, restarts the finding's count.
//
// The zero policy disables the gate's own hysteresis: the gate then follows
// closure's clear count.
type GateConfirmationPolicy struct {
	Evaluations int
	Window      time.Duration
	Interval    time.Duration
}

func (p GateConfirmationPolicy) enabled() bool { return p != GateConfirmationPolicy{} }

func (p GateConfirmationPolicy) validate() error {
	if !p.enabled() {
		return nil
	}
	if p.Evaluations < 2 {
		return fmt.Errorf("gate confirmation needs at least 2 clear evaluations, got %d", p.Evaluations)
	}
	if p.Window < 0 {
		return fmt.Errorf("gate confirmation window must not be negative, got %s", p.Window)
	}
	if p.Interval <= 0 {
		return fmt.Errorf("gate confirmation interval must be positive, got %s", p.Interval)
	}
	return nil
}

// gateClear is one active finding's run of consecutive clear observations.
type gateClear struct {
	count int
	since time.Time
}

// observeGate records one evaluation of detectorID for the gate. findings
// are what the detector reported; every active finding of the detector that
// is absent from them was observed clear. It then schedules the detector's
// next gate re-evaluation while one of its findings is clearing but not yet
// confirmed. Requires the tracker to reflect the scheduled evaluations only.
func (c *Controller) observeGate(detectorID string, now time.Time, findings []sdk.Finding) {
	if !c.config.GateConfirmation.enabled() {
		return
	}
	reported := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		if finding.Status == sdk.FindingResolved {
			continue
		}
		reported[FindingStateKey(detectorID, FindingFingerprint(finding))] = struct{}{}
	}
	pending := false
	for key, state := range c.tracker.Snapshot() {
		if state.DetectorID != detectorID {
			continue
		}
		_, firing := reported[key]
		if !state.Active || firing {
			delete(c.gateClears, key)
			continue
		}
		clear, ok := c.gateClears[key]
		if !ok {
			clear = &gateClear{since: now}
			c.gateClears[key] = clear
		}
		clear.count++
		if !c.gateConfirmed(clear, now) {
			pending = true
		}
	}
	if pending {
		c.gateProbes[detectorID] = now.Add(c.config.GateConfirmation.Interval)
	} else {
		delete(c.gateProbes, detectorID)
	}
}

// probeGate re-evaluates clearing detectors against the step's snapshot for
// the gate only. An error or an invalid finding leaves the detector's state
// unknown, so its findings lose their clear count.
func (c *Controller) probeGate(
	ctx context.Context, now time.Time, snapshot sdk.DetectionContext, probes []sdk.Detector,
) {
	for _, detector := range probes {
		spec := detector.Spec()
		findings, err := sdk.DetectObserved(ctx, detector, snapshot)
		if err == nil {
			for index := range findings {
				if findings[index].DetectorID == "" {
					findings[index].DetectorID = spec.ID
				}
				if err = core.ValidateFinding(spec, findings[index]); err != nil {
					break
				}
			}
		}
		if err != nil {
			c.resetGate(spec.ID)
			if c.OnError != nil {
				c.OnError(fmt.Errorf("gate re-evaluation of detector %q: %w", spec.ID, err))
			}
			continue
		}
		c.observeGate(spec.ID, now, findings)
	}
}

// resetGate forgets the gate's clear counts of one detector's findings.
func (c *Controller) resetGate(detectorID string) {
	for key, state := range c.tracker.Snapshot() {
		if state.DetectorID == detectorID {
			delete(c.gateClears, key)
		}
	}
	delete(c.gateProbes, detectorID)
}

func (c *Controller) gateConfirmed(clear *gateClear, now time.Time) bool {
	policy := c.config.GateConfirmation
	return clear.count >= policy.Evaluations && now.Sub(clear.since) >= policy.Window
}

// forgetGate drops gate state that no longer matches an active finding, for
// example after closure's own hysteresis cleared it or the incident closed.
func (c *Controller) forgetGate() {
	active := c.tracker.Snapshot()
	for key := range c.gateClears {
		if state, ok := active[key]; !ok || !state.Active {
			delete(c.gateClears, key)
		}
	}
	c.mu.Lock()
	open := c.incidentOpen
	c.mu.Unlock()
	if !open {
		c.gateProbes = make(map[string]time.Time)
	}
}

// dueGateProbes returns the detectors whose gate re-evaluation is due and
// that the scheduler did not already select.
func (c *Controller) dueGateProbes(now time.Time, selected []sdk.Detector) []sdk.Detector {
	scheduled := make(map[string]struct{}, len(selected))
	for _, detector := range selected {
		scheduled[detector.Spec().ID] = struct{}{}
	}
	probes := make([]sdk.Detector, 0)
	for detectorID, due := range c.gateProbes {
		if now.Before(due) {
			continue
		}
		if _, ok := scheduled[detectorID]; ok {
			continue
		}
		if detector, ok := c.detectors[detectorID]; ok {
			probes = append(probes, detector)
		}
	}
	sort.Slice(probes, func(left int, right int) bool { return probes[left].Spec().ID < probes[right].Spec().ID })
	return probes
}

func (c *Controller) nextGateProbe() time.Time {
	var next time.Time
	for _, due := range c.gateProbes {
		if next.IsZero() || due.Before(next) {
			next = due
		}
	}
	return next
}

// gateConfirmedKey reports whether the gate has confirmed an active finding
// clear although closure still counts it as active.
func (c *Controller) gateConfirmedKey(key string, now time.Time) bool {
	clear, ok := c.gateClears[key]
	return ok && c.gateConfirmed(clear, now)
}
