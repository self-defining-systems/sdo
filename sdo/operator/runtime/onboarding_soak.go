package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// The onboarding soak is the first part of a new install's startup period:
// once every opener detector has been evaluated, the controller samples the
// committed probes on their schedule for ControllerConfig.OnboardingSoak and
// judges every health-probe sample with journeycheck.Soaked. The period
// cannot be established while the soak runs, so its reds never open an
// incident. The record is written once (OnboardingSoakStore): an install
// soaks once, and a restart or reinstall that finds it never soaks again.
// Guarded by Controller.mu.

// OnboardingSoakStore keeps the install's one soak record.
type OnboardingSoakStore interface {
	Load(context.Context) (*journeycheck.SoakRecord, error)
	Save(context.Context, journeycheck.SoakRecord) error
}

// JourneyCheck runs every health probe's full journey checks once against
// the installed application (MemoryTraffic.JourneyCheck).
type JourneyCheck func(context.Context) (journeycheck.Report, error)

// journeyCheckGrace is how long past the soak's end the record waits for the
// journey checks; past it they are recorded as unfinished.
const journeyCheckGrace = 15 * time.Minute

type onboardingSoak struct {
	store    OnboardingSoakStore
	duration time.Duration
	running  *journeycheck.InstalledSoak
	// ctx bounds the journey checks; check runs them once, when the soak
	// begins. checkDone is closed once checked holds their results.
	ctx       context.Context
	check     JourneyCheck
	checkDone chan struct{}
	checked   []journeycheck.Result
	// judged is the record not yet saved; done is set once it is saved or
	// an earlier record was found.
	judged *journeycheck.SoakRecord
	done   bool
}

// AttachOnboardingSoak attaches the install's soak record, after
// AttachInstallStartup. With no record and a start-up period still holding,
// this process soaks, and runs check (when set) once beside it: the probes'
// full journey checks, whose results join the record; otherwise it never does.
func (c *Controller) AttachOnboardingSoak(ctx context.Context, store OnboardingSoakStore, check JourneyCheck) error {
	if store == nil || c.config.OnboardingSoak <= 0 {
		return nil
	}
	record, err := store.Load(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.soak = &onboardingSoak{store: store, duration: c.config.OnboardingSoak, done: record != nil, ctx: ctx, check: check}
	if record != nil {
		bounds := record.LatencyBoundsByScenario()
		c.soakBounds.Store(&bounds)
	}
	return nil
}

// soakHolds advances the soak at one evaluation pass of a holding period:
// it starts once nothing is unevaluated, collects the snapshot's samples
// and judges when due. It reports whether the soak still runs, and the
// holder line naming it. The caller holds c.mu.
func (c *Controller) soakHoldsLocked(now time.Time, snapshot sdk.DetectionContext, evaluated bool) (bool, string) {
	soak := c.soak
	if soak == nil || soak.done || soak.judged != nil {
		return false, ""
	}
	if soak.running == nil {
		if !evaluated {
			return false, ""
		}
		soak.running = journeycheck.NewInstalledSoak(now.UTC(), soak.duration)
		slog.Info("onboarding_soak", "event", "began", "began_at", now.UTC(), "until", soak.running.Until())
		c.startJourneyCheckLocked()
	}
	if set, ok := snapshot.(traffic.WindowSet); ok {
		soak.running.Observe(set.TrafficWindows())
	}
	if !soak.running.Due(now) {
		return true, fmt.Sprintf("onboarding soak until %s", soak.running.Until().Format(time.RFC3339))
	}
	checked, finished := soak.journeyResultsLocked()
	if !finished {
		if until := soak.running.Until().Add(journeyCheckGrace); now.Before(until) {
			return true, fmt.Sprintf("onboarding journey checks running, at most until %s", until.Format(time.RFC3339))
		}
		checked = []journeycheck.Result{{Item: journeycheck.ItemJourneyCheck, Subject: "all", Status: journeycheck.Skipped,
			Detail: fmt.Sprintf("the probes' full journey checks did not finish within %s of the soak", journeyCheckGrace)}}
	}
	record := soak.running.Judge(now.UTC())
	record.Results = append(record.Results, checked...)
	soak.judged, soak.running = &record, nil
	bounds := record.LatencyBoundsByScenario()
	c.soakBounds.Store(&bounds)
	slog.Info("onboarding_soak", "event", "judged", "red", len(record.Red()), "results", len(record.Results))
	return false, ""
}

// startJourneyCheckLocked runs the probes' full journey checks once, in the
// background; their results land in soak.checked. The caller holds c.mu.
func (c *Controller) startJourneyCheckLocked() {
	soak := c.soak
	if soak.check == nil || soak.checkDone != nil {
		return
	}
	soak.checkDone = make(chan struct{})
	go func() {
		defer close(soak.checkDone)
		report, err := soak.check(soak.ctx)
		results := report.Results
		if err != nil {
			results = []journeycheck.Result{{Item: journeycheck.ItemJourneyCheck, Subject: "all", Status: journeycheck.Skipped,
				Detail: "the probes' full journey checks could not run: " + err.Error()}}
		}
		c.mu.Lock()
		soak.checked = results
		c.mu.Unlock()
		slog.Info("onboarding_soak", "event", "journey_checks", "results", len(results), "error", err)
	}()
}

// journeyResultsLocked is the journey checks' results and whether they are
// in (no check configured counts as in). The caller holds c.mu.
func (s *onboardingSoak) journeyResultsLocked() ([]journeycheck.Result, bool) {
	if s.checkDone == nil {
		return nil, true
	}
	select {
	case <-s.checkDone:
		return s.checked, true
	default:
		return nil, false
	}
}

// recordOnboardingSoak saves a judged soak. A failed write is retried at the
// next persist; the period is not held meanwhile.
func (c *Controller) recordOnboardingSoak(ctx context.Context) error {
	c.mu.Lock()
	soak := c.soak
	var record *journeycheck.SoakRecord
	if soak != nil && !soak.done {
		record = soak.judged
	}
	c.mu.Unlock()
	if record == nil {
		return nil
	}
	if err := soak.store.Save(ctx, *record); err != nil {
		return err
	}
	c.mu.Lock()
	soak.done = true
	c.mu.Unlock()
	return nil
}

// latencyBounds is the latency bounds the install's soak calibrated, by
// "workload/scenario"; nil before the soak is judged or without one. It takes
// no controller lock: the traffic observer calls it while building snapshots.
func (c *Controller) latencyBounds() map[string]time.Duration {
	if bounds := c.soakBounds.Load(); bounds != nil {
		return *bounds
	}
	return nil
}
