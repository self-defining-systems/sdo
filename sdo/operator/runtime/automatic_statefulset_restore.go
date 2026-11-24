package runtime

import (
	"context"
	"errors"
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// advanceAutomaticRestores runs before the step's recovery proof. A new Pod
// deletion invalidates the snapshot already taken by this step.
func (c *Controller) advanceAutomaticRestores(ctx context.Context, now time.Time, loop *ResponderLoop, report *contracts.StageReport, deadline time.Time) (waiting, changed bool) {
	if loop == nil || !playbookWrote(report) {
		return false, false
	}
	saved, waiting, err := loop.advanceAutomaticRestores(ctx, report, func() (context.Context, context.CancelFunc, error) {
		c.mu.Lock()
		incidentID := ""
		if c.awaitingAutomaticRecoveryLocked() == report {
			incidentID = c.currentIncidentRequest.IncidentID
		}
		err := c.requireDurableLocked("automatic StatefulSet restore", func(state *contracts.ControllerState) bool {
			request := state.GetIncidentRequest()
			stage := deterministicStageReport(request.GetPriorStages())
			return request.GetIncidentId() == incidentID && playbookWrote(stage) && !stage.GetHandoffSelected()
		})
		c.mu.Unlock()
		if incidentID == "" {
			return nil, nil, errors.New("automatic recovery no longer owns the incident")
		}
		if err != nil {
			return nil, nil, err
		}
		return c.actionContext(ctx)
	}, func() bool {
		return laterObservationTime(now, c.clock()).Before(deadline)
	})
	if err != nil && ctx.Err() == nil {
		c.reportBrokerError("continue automatic StatefulSet restore", err)
	}
	if saved == nil {
		return waiting, false
	}
	receipt, err := recordedPlanContract(*saved)
	if err != nil {
		c.reportBrokerError("project StatefulSet restore receipt", err)
		return true, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Only the stage that still owns this incident can publish its continuation.
	if c.awaitingAutomaticRecoveryLocked() != report {
		return waiting, true
	}
	for _, attempt := range report.GetMitigations() {
		if attempt.GetApplyReceipt().GetApplicationId() == saved.ID {
			attempt.ApplyReceipt = receipt
		}
	}
	if c.currentIncidentResult != nil {
		for i, application := range c.currentIncidentResult.PlanApplications {
			if application.ID == saved.ID {
				c.currentIncidentResult.PlanApplications[i] = *saved
			}
		}
	}
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	return true, true
}

func (l *ResponderLoop) advanceAutomaticRestores(ctx context.Context, report *contracts.StageReport, actionContext func() (context.Context, context.CancelFunc, error), canWrite func() bool) (*RecordedPlanApplication, bool, error) {
	if !l.operationMu.TryLock() {
		return nil, true, nil
	}
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	var waiting bool
	for _, app := range l.applicationEvidenceLocked() {
		attempt := slices.IndexFunc(report.GetMitigations(), func(attempt *contracts.StageMitigation) bool {
			receipt := attempt.GetApplyReceipt()
			return receipt.GetApplicationId() == app.ID && receipt.GetApplied() && !receipt.GetRolledBack()
		})
		if attempt == -1 || app.RolledBack || app.Result.RolledBack {
			continue
		}
		restores := sourceRestoredStatefulSets(app.Result)
		if len(restores) == 0 {
			continue
		}
		effectCount := len(app.Result.ObjectEffects)
		pending, err := l.Executor.advanceRestoredStatefulSets(ctx, restores, &app.Result, func(pod StuckRestartPod) error {
			if !canWrite() {
				return nil
			}
			writeCtx, cancel, err := actionContext()
			if err != nil {
				return err
			}
			defer cancel()
			return l.replaceAutomaticRestorePod(writeCtx, pod, &app.Result)
		})
		if len(app.Result.ObjectEffects) > effectCount {
			app.CompletedAt = l.clock()
			l.retainAutomaticRestore(app)
		}
		// Reconcile a previously saved effect if the controller restarted
		// after the tool checkpoint and before its step state was saved.
		if len(app.Result.ObjectEffects) > len(report.GetMitigations()[attempt].GetApplyReceipt().GetObjectEffects()) {
			if saveErr := l.persistToolState(ctx); saveErr != nil {
				return nil, true, errors.Join(err, saveErr)
			}
			return &app, true, err
		}
		if err != nil {
			return nil, true, err
		}
		waiting = waiting || pending
	}
	return nil, waiting, nil
}

func (l *ResponderLoop) replaceAutomaticRestorePod(ctx context.Context, pod StuckRestartPod, result *PlanApplicationResult) (err error) {
	if l.gateError != nil {
		return l.gateError
	}
	if l.MutationGate != nil {
		lease, err := l.MutationGate.Begin()
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, lease.Finish(false)) }()
	} else if l.IncidentStatus != nil && l.IncidentStatus() != nil {
		return errors.New("incident_verified")
	}
	return l.Executor.replaceStuckPods(ctx, []StuckRestartPod{pod}, result)
}

func (l *ResponderLoop) retainAutomaticRestore(app RecordedPlanApplication) {
	for i, old := range l.applicationsLog {
		if old.ID == app.ID {
			l.applicationsLog[i] = app
			l.applications[app.ID] = app.Result
			return
		}
	}
	for i, old := range l.archivedApplications {
		if old.ID == app.ID {
			l.archivedApplications[i] = app
			return
		}
	}
}
