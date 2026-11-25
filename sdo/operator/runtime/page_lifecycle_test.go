package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type stagedPageDispatcher struct{ *recordingDispatcher }

func (stagedPageDispatcher) DeterministicResponseStage() ResponseStage { return failingPageStage{} }

type failingPageStage struct{}

func (failingPageStage) Name() string { return "deterministic" }
func (failingPageStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	return nil, fmt.Errorf("synthetic automatic stage failure")
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
			if !startup {
				// The automatic stage failed without a write; the page still
				// reaches its responder, with no dispatch retry.
				if err := c.Step(ctx, now, nil); err != nil {
					t.Fatal(err)
				}
				executePendingEffect(t, c)
				awaitEffectGoroutines(c)
			}
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
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			c.incidentGate = func(string) (*apihistory.MutationGate, error) { return gate, nil }
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
			if lease, err := gate.Begin(); err == nil || err.Error() != "incident_closed" {
				if lease != nil {
					_ = lease.Finish(false)
				}
				t.Fatalf("terminal closure left the mutation gate open: %v", err)
			}
			if closed.EscalatedAt.IsZero() || closed.HealthClearedAt != nil || closed.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("expected an escalated closure without recovery proof: %+v", closed)
			}
		})
	}
}

type pageStageDispatcher struct {
	recordingDispatcher
	stage ResponseStage
}

func (d *pageStageDispatcher) DeterministicResponseStage() ResponseStage { return d.stage }

type pageLearnedDetector struct{ sdk.Detector }

func (d pageLearnedDetector) Spec() sdk.DetectorSpec {
	spec := d.Detector.Spec()
	spec.OriginatingIncident = "synthetic-prior-incident"
	return spec
}

// scanned is a learned playbook with no required parameter that restarts the
// synthetic Deployment. Each hang makes that call wait for its context.
type scanned struct {
	id       string
	result   program.Result
	hangFrom int32 // first Diagnose call that hangs; 0 never
	hangPlan bool
	action   program.Action
	asked    *atomic.Int32
}

func (p scanned) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.id} }

func (p scanned) Diagnose(ctx context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if call := p.asked.Add(1); p.hangFrom > 0 && call >= p.hangFrom {
		<-ctx.Done()
		return program.Verdict{}, ctx.Err()
	}
	verdict := program.Verdict{Result: p.result}
	if _, ok := p.action.(program.Escalate); ok {
		verdict.Reach, verdict.CauseClass = program.ReachOutOfReach, program.CauseClasses()[0]
		verdict.Evidence = []program.Evidence{{Fact: "synthetic", Observed: "a", Expected: "b"}}
	}
	return verdict, nil
}

func (p scanned) Mitigate(ctx context.Context, _ program.ReadView, _ program.Bindings) (program.Plan, error) {
	if p.hangPlan {
		<-ctx.Done()
		return program.Plan{}, ctx.Err()
	}
	if p.action != nil {
		return program.Plan{Actions: []program.Action{p.action}}, nil
	}
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, Reason: "synthetic"}}}, nil
}

func ask(id string, result program.Result) scanned {
	return scanned{id: id, result: result, asked: &atomic.Int32{}}
}

type twoRoles struct{ componentRestart }

func (p twoRoles) Spec() program.ProgramSpec {
	spec := p.componentRestart.Spec()
	spec.Params = append(spec.Params, program.ParamSpec{Name: "other", Kind: "Service", Required: true})
	return spec
}

// pageScan opens a page over the programs and runs its first response.
func pageScan(t *testing.T, programs []program.Diagnoser, configure func(*ControllerConfig, *ResponderLoop, *IncidentRequest)) (*Controller, *settlingWorld, time.Duration, *bool) {
	t.Helper()
	world := newSettlingWorld(settlingCase{})
	stage, state := settlingStageOn(t, settlingCase{}, world)
	request := state.Request
	request.Page, request.OpeningKeys, request.Findings = syntheticPage, nil, nil
	request.ResponseDeadline = world.clock.Now().Add(time.Hour)
	var mitigators []program.Mitigator
	for _, diagnoser := range programs {
		mitigators = append(mitigators, diagnoser.(program.Mitigator))
	}
	registry, err := program.NewRegistry(programs, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	loop := stage.NewLoop(request)
	cfg := testControllerConfig()
	cfg.Namespace = request.Namespace
	loop.Programs = registry
	if configure != nil {
		configure(&cfg, loop, &request)
	}
	loop.Request = request
	c, err := NewController(cfg, []sdk.Detector{loop.Detectors[0], pageLearnedDetector{loop.Detectors[1]}}, world, &pageStageDispatcher{stage: stage}, world.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	c.now = world.clock.Now
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	c.responderLoop = loop
	closed := false
	c.OnIncidentClosed = func(IncidentClosure) { closed = true }
	c.beginIncident(world.clock.Now(), request, request.Findings)
	entry := c.responseEntry(request)
	if entry != runAutomaticStages || !c.automaticResponsePending(&request) {
		return c, world, 0, &closed
	}
	before := lifecycleio.Now()
	completion := c.runResponsePipeline(t.Context(), request, entry)
	completion.incidentID = request.IncidentID
	elapsed := lifecycleio.Since(before)
	if completion.err != nil {
		t.Fatal(completion.err)
	}
	c.handleDispatchCompletion(completion, world.clock.Now())
	return c, world, elapsed, &closed
}

func TestPageScanWritesOnlyForOneCertainMatch(t *testing.T) {
	hang := func(p scanned, from int32) scanned { p.hangFrom = from; return p }
	slowPlan, escalate := ask("a", program.Match), ask("a", program.Match)
	slowPlan.hangPlan = true
	escalate.action = program.Escalate{Reason: "synthetic"}
	clue := func(_ *ControllerConfig, _ *ResponderLoop, request *IncidentRequest) {
		// A standing clue that names the playbook but not the paged symptom.
		request.Findings = []sdk.Finding{{DetectorID: "api-request-stall", Fingerprint: "standing", Status: sdk.FindingActive, Playbooks: []string{"a"}}}
	}
	credited := func(_ *ControllerConfig, loop *ResponderLoop, _ *IncidentRequest) {
		loop.Credits = credits("fix", "another-alert", "api")
	}
	for _, tc := range []struct {
		name      string
		programs  []program.Diagnoser
		configure func(*ControllerConfig, *ResponderLoop, *IncidentRequest)
		writes    int32
	}{
		{"one match", []program.Diagnoser{ask("a", program.Match), ask("b", program.NoMatch), restartProgram{id: "unbound"}}, nil, 1},
		{"one match beside a standing clue", []program.Diagnoser{ask("a", program.Match)}, clue, 1},
		{"credited component", []program.Diagnoser{componentRestart{id: "fix"}}, credited, 1},
		{"credited component with an unbound role", []program.Diagnoser{ask("a", program.Match), twoRoles{componentRestart{id: "fix"}}}, credited, 1},
		{"uncredited component", []program.Diagnoser{componentRestart{id: "fix"}}, nil, 0},
		{"two matches", []program.Diagnoser{ask("a", program.Match), ask("b", program.Match)}, nil, 0},
		{"no match", []program.Diagnoser{ask("a", program.NoMatch)}, clue, 0},
		{"unknown", []program.Diagnoser{ask("a", program.Unknown)}, nil, 0},
		{"match and unknown", []program.Diagnoser{ask("a", program.Match), ask("b", program.Unknown)}, nil, 0},
		{"match and needs judgement", []program.Diagnoser{ask("a", program.Match), ask("b", program.NeedsJudgement)}, nil, 0},
		{"match and timeout", []program.Diagnoser{ask("a", program.Match), hang(ask("b", program.NoMatch), 1)}, nil, 0},
		{"second diagnosis hangs", []program.Diagnoser{hang(ask("a", program.Match), 2)}, nil, 0},
		{"plan hangs", []program.Diagnoser{slowPlan}, nil, 0},
		{"plan escalates", []program.Diagnoser{escalate}, nil, 0},
		{"no playbooks", nil, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, world, elapsed, closed := pageScan(t, tc.programs, tc.configure)
				report := autoReport(c)
				if world.restarts.Load() != tc.writes || playbookWrote(report) != (tc.writes == 1) {
					t.Fatalf("writes = %d, want %d: %v", world.restarts.Load(), tc.writes, report)
				}
				limit := pageScanBudget
				switch tc.name {
				case "match and timeout", "second diagnosis hangs":
					limit = pageDiagnoseBudget
				}
				if elapsed > limit || limit > 2*time.Second {
					t.Fatalf("the page waited %s for its responder, limit %s", elapsed, limit)
				}
				if !report.GetHandoffSelected() || c.responseEntry(*c.currentIncidentRequest) != dispatchStagedHandoff {
					t.Fatalf("page was not handed to the responder: %v", report)
				}
				if got := stageDispatchRequest(*c.currentIncidentRequest); got.Page != syntheticPage || len(got.Findings) != len(c.currentIncidentRequest.Findings) {
					t.Fatalf("handoff lost the page or its clues: %+v", got)
				}
				if tc.writes == 0 && len(report.GetMitigations()) == 0 && report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED {
					t.Fatalf("unwritten page reports %v", report.GetRecoveryWait())
				}
				frozen := *c.currentIncidentRequest
				frozen.PreparedAgentTask = "handoff"
				if c.responseEntry(frozen) != dispatchStagedHandoff {
					t.Fatal("a frozen handoff lost its stage report")
				}
				// No finding clearing, or never firing, closes a page.
				for range 5 {
					autoStep(t, c, world.clock, time.Minute)
				}
				if *closed {
					t.Fatal("page closed without its responder")
				}
				if tc.writes == 1 && report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
					t.Fatalf("exit reason %v claims a recovery wait", report.GetRecoveryWait())
				}
			})
		})
	}
}

func TestPageStartsOnItsResponderWithoutAScan(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*ControllerConfig, *ResponderLoop, *IncidentRequest)
	}{
		{"startup", func(cfg *ControllerConfig, _ *ResponderLoop, _ *IncidentRequest) { cfg.StartupBound = 30 * time.Minute }},
		{"frozen task", func(_ *ControllerConfig, _ *ResponderLoop, request *IncidentRequest) {
			request.PreparedAgentTask = "task"
		}},
		{"escalate policy", func(cfg *ControllerConfig, _ *ResponderLoop, _ *IncidentRequest) {
			cfg.AfterPlaybook = settings.AfterPlaybookEscalate
		}},
		{"wait policy", func(cfg *ControllerConfig, _ *ResponderLoop, _ *IncidentRequest) {
			cfg.AfterPlaybook = settings.AfterPlaybookWait
			cfg.AfterPlaybookWait = time.Minute
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			playbook := ask("a", program.Match)
			c, world, _, _ := pageScan(t, []program.Diagnoser{playbook}, tc.configure)
			if c.responseEntry(*c.currentIncidentRequest) != dispatchDirectResponder || c.automaticResponsePending(c.currentIncidentRequest) ||
				playbook.asked.Load() != 0 || world.restarts.Load() != 0 {
				t.Fatalf("page did not go straight to its responder: asked %d, writes %d", playbook.asked.Load(), world.restarts.Load())
			}
		})
	}
}

// A page whose write was recorded just before a restart is handed off by the
// next recovery check, at no later bound.
func TestPageHandsOffARestoredWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, world, _, closed := pageScan(t, []program.Diagnoser{ask("a", program.Match)}, nil)
		c.mu.Lock()
		report := autoReport(c)
		report.HandoffSelected, report.HandoffKeys, report.HandoffState, report.RecoveryWait = false, nil, nil, nil
		c.responderDone, c.responderCompletedAt = true, world.clock.Now()
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		c.mu.Unlock()
		autoStep(t, c, world.clock, time.Second)
		if !autoReport(c).GetHandoffSelected() || *closed {
			t.Fatalf("restored write was not handed off: %v", autoReport(c))
		}
	})
}

func TestPageTakesNoLaterPass(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, passes := churnController(t, now)
	c.currentIncidentRequest.OpeningKeys = nil
	c.currentIncidentRequest.Page = syntheticPage
	c.admitActivation(t.Context(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	c.resumePendingLearnedPass(t.Context())
	c.Wait()
	if passes.Load() != 0 || c.midStage.pending {
		t.Fatalf("later passes = %d, pending %t", passes.Load(), c.midStage.pending)
	}
}

type slowWorld struct{ *settlingWorld }

func (w slowWorld) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lifecycleio.NewTimer(2 * pageScanBudget).C:
		return w.settlingWorld.Observe(ctx, refs)
	}
}

// The budget ends before the write: one that began finishes and is recorded.
func TestPageWriteOutlivesTheScanBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, world, elapsed, _ := pageScan(t, []program.Diagnoser{ask("a", program.Match)}, func(_ *ControllerConfig, loop *ResponderLoop, _ *IncidentRequest) {
			loop.Executor.Rollouts = slowWorld{loop.Executor.Rollouts.(*settlingWorld)}
			loop.saveToolState = func(ctx context.Context, _ []byte) error { return ctx.Err() }
		})
		attempts := autoReport(c).GetMitigations()
		if elapsed <= pageScanBudget || world.restarts.Load() != 1 || len(attempts) != 1 || attempts[0].GetResult() == "apply_error" ||
			attempts[0].GetApplyReceipt().GetRolloutPending().GetReason() != "synthetic rollout in progress" {
			t.Fatalf("write was cut short after %s: %v", elapsed, attempts)
		}
	})
}

func TestPageWithoutPlaybooksReadsNothing(t *testing.T) {
	unavailable := new(unavailableHandoffView)
	pageScan(t, nil, func(_ *ControllerConfig, loop *ResponderLoop, _ *IncidentRequest) { loop.Provider = unavailable })
	if unavailable.reads != 0 {
		t.Fatalf("snapshots = %d, want none", unavailable.reads)
	}
}

type pageErrorStage struct{ report *contracts.StageReport }

func (pageErrorStage) Name() string { return "deterministic" }
func (s pageErrorStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	return s.report, fmt.Errorf("synthetic")
}

// Only a live attempt that wrote nothing may lose its error.
func TestPageStageErrorAfterAWriteOrShutdownIsKept(t *testing.T) {
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	wrote := &contracts.StageReport{Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}
	for _, tc := range []struct {
		ctx    context.Context
		report *contracts.StageReport
		keep   bool
	}{{t.Context(), nil, false}, {t.Context(), wrote, true}, {stopped, nil, true}} {
		state := &IncidentState{Request: IncidentRequest{Page: syntheticPage}}
		if err := RunResponseStages(tc.ctx, state, []ResponseStage{pageErrorStage{tc.report}}); (err != nil) != tc.keep {
			t.Fatalf("error = %v, keep %t", err, tc.keep)
		}
	}
}
