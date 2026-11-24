package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

type stagedPageDispatcher struct{ *recordingDispatcher }

func (stagedPageDispatcher) DeterministicResponseStage() ResponseStage { return unusedPageStage{} }

type unusedPageStage struct{}

func (unusedPageStage) Name() string { return "deterministic" }
func (unusedPageStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	return nil, fmt.Errorf("a page must go straight to the responder")
}

func warmupPageController(t *testing.T, now *time.Time, cfg ControllerConfig, dispatcher Dispatcher) *Controller {
	t.Helper()
	detector := controllerDetector(memory.TrafficDetectorPrefix+"warm", time.Second, sdk.Finding{})
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "synthetic"
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	c, err := NewController(cfg, []sdk.Detector{unprovenProbe{detector}}, staticProvider{}, dispatcher, *now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return *now }
	c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) {
		return &ExternalIncident{ID: "warmup-page", Namespace: cfg.Namespace, Message: syntheticPage}, nil
	}
	return c
}

func TestPageDispatchesDuringWarmupWithoutOpeningAlerts(t *testing.T) {
	for _, startup := range []bool{false, true} {
		name := "established install"
		if startup {
			name = "startup hold"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := at(1000)
			cfg := testControllerConfig()
			if startup {
				cfg.StartupBound = 30 * time.Minute
			}
			dispatcher := stagedPageDispatcher{&recordingDispatcher{requests: make(chan IncidentRequest, 2)}}
			c := warmupPageController(t, &now, cfg, dispatcher)
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			if !c.IncidentOpen() {
				t.Fatal("checking detector health prevented the page from opening an incident")
			}
			if startup && !c.startupHolds(now) {
				t.Fatal("a page ended the startup hold for detector-opened incidents")
			}
			executePendingEffect(t, c)
			awaitEffectGoroutines(c)
			select {
			case request := <-dispatcher.requests:
				if request.Page != syntheticPage || len(alertKeys(&request)) != 0 {
					t.Fatalf("responder request lacks the opening page or contains detector alerts: %+v", request)
				}
			default:
				t.Fatal("page opened with no detector alerts, but no responder started")
			}
		})
	}
}

func TestPageWithoutAlertsClosesEscalatedAtVerificationBound(t *testing.T) {
	for _, cause := range []string{"responder gave up", "response timeout", "task preparation failed", "observer unavailable"} {
		t.Run(cause, func(t *testing.T) {
			ctx := context.Background()
			now := at(1000)
			cfg := testControllerConfig()
			cfg.VerificationTimeout = 5 * time.Second
			cfg.ClosureStabilityWindow = time.Second
			dispatcher := &pageDispatcher{requests: make(chan IncidentRequest, 1), returned: make(chan struct{})}
			t.Cleanup(func() { close(dispatcher.returned) })
			var transport Dispatcher = dispatcher
			if cause == "task preparation failed" {
				transport = &preparingDispatcher{prepare: func(int) error {
					return fmt.Errorf("%w: synthetic unreadable memory", memory.ErrUnreadable)
				}}
			}
			c := warmupPageController(t, &now, cfg, transport)
			var closed *IncidentClosure
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
			if cause == "task preparation failed" {
				prepared, err := c.PrepareDispatchStep(ctx)
				if err != nil || !prepared {
					t.Fatalf("dispatch failure was not recorded: changed %t, error %v", prepared, err)
				}
			} else {
				executePendingEffect(t, c)
				awaitRequest(t, dispatcher.requests)
				dispatcher.returned <- struct{}{}
				completion := <-c.results
				switch cause {
				case "response timeout":
					now = c.currentIncidentRequest.ResponseDeadline
					completion.err = &ResponseTimeoutError{Deadline: now}
				case "responder gave up":
					completion.result.Status = IncidentFailed
				case "observer unavailable":
					c.provider = externalUnavailableSnapshots{}
				}
				c.handleDispatchCompletion(completion, now)
			}
			bound := c.recoveryGraceEndsLocked()
			for !now.After(bound) {
				if err := c.Step(ctx, now, nil); err != nil && (cause != "observer unavailable" || !now.Before(bound)) {
					t.Fatal(err)
				}
				if now.Before(bound) && closed != nil {
					t.Fatal("escalation closed while verification still had time")
				}
				now = now.Add(time.Second)
			}
			if c.IncidentOpen() || closed == nil {
				t.Fatal("page escalation did not finalize the incident")
			}
			if closed.EscalatedAt.IsZero() || closed.HealthClearedAt != nil || closed.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("expected an escalated closure without recovery proof: %+v", closed)
			}
		})
	}
}
