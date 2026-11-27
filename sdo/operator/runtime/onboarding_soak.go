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

type onboardingSoak struct {
	store    OnboardingSoakStore
	duration time.Duration
	running  *journeycheck.InstalledSoak
	// judged is the record not yet saved; done is set once it is saved or
	// an earlier record was found.
	judged *journeycheck.SoakRecord
	done   bool
}

// AttachOnboardingSoak attaches the install's soak record, after
// AttachInstallStartup. With no record and a start-up period still holding,
// this process soaks; otherwise it never does.
func (c *Controller) AttachOnboardingSoak(ctx context.Context, store OnboardingSoakStore) error {
	if store == nil || c.config.OnboardingSoak <= 0 {
		return nil
	}
	record, err := store.Load(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.soak = &onboardingSoak{store: store, duration: c.config.OnboardingSoak, done: record != nil}
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
	}
	if set, ok := snapshot.(traffic.WindowSet); ok {
		soak.running.Observe(set.TrafficWindows())
	}
	if soak.running.Due(now) {
		record := soak.running.Judge(now.UTC())
		soak.judged, soak.running = &record, nil
		slog.Info("onboarding_soak", "event", "judged", "red", len(record.Red()), "scenarios", len(record.Results))
		return false, ""
	}
	return true, fmt.Sprintf("onboarding soak until %s", soak.running.Until().Format(time.RFC3339))
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
