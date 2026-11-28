package runtime

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// lateDrift is an unexplained-drift clue on Service/api that appears once on
// is set, as a fault member that lands after the agent was dispatched.
type lateDrift struct{ on atomic.Bool }

func (d *lateDrift) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "drift.unexplained", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (d *lateDrift) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.on.Load() {
		return []sdk.Finding{}, nil
	}
	finding := sdk.Finding{DetectorID: d.Spec().ID, RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "Service/api differs from accepted source", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Playbooks: []string{"source-restore"}}
	finding.Fingerprint = FindingFingerprint(finding)
	return []sdk.Finding{finding}, nil
}

// A cause that lands after the handoff is pulled, never pushed: `sdo detect`
// leads with it as new since the handoff, with the drift's field, writer and
// time; `--wait` returns on it once; SDO writes nothing after dispatch and
// dispatches no second agent.
func TestLateDriftLeadsDetectAndEndsAWaitOnceWithoutAStageWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(1000, 0).UTC()
	world := e2eWorld()
	churn, drift := &e2eChurn{}, &lateDrift{}
	churn.on.Store(true)
	detectors := []sdk.Detector{churn, drift}
	restart := componentRestart{id: "fix"}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	dispatcher := &recordingDispatcher{}
	c, err := NewController(cfg, detectors, world, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	snapshot, err := world.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opening, _ := churn.Detect(ctx, snapshot)
	request := IncidentRequest{IncidentID: "synthetic-late", Application: "demo", Namespace: "app", SourceCommit: "synthetic", RepairPolicy: "recorded-actions",
		Findings: opening, OpeningKeys: findingKeys(opening), DetectedAt: now, ResponseDeadline: lifecycleio.Now().Add(time.Hour)}
	request.PriorStages = []*contracts.StageReport{{Stage: "deterministic", HandoffSelected: true}}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	loop.initializeDispatch(nil, 5, nil)
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	c.responderLoop = loop
	c.incidentOpen, c.dispatchState = true, contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.currentIncidentRequest = &request
	c.incidentFindingKeys = findingKeys(opening)
	c.incidentDetectedAt, c.incidentDispatchedAt = now, now
	loop.CurrentEvidence = func() *IncidentRequest { return c.currentIncidentRequest }
	step := func(count int) {
		for range count {
			now = now.Add(time.Second)
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			c.Wait()
		}
	}
	clock := autoClock{clocktesting.NewFakeClock(now)}
	loop.Now, loop.WaitClock = clock.Now, clock

	step(2)
	if text := loop.Handle(ctx, ToolRequest{Command: "detect"}).Text; strings.Contains(text, "NEW SINCE THE HANDOFF") {
		t.Fatalf("nothing is new yet:\n%s", text)
	}
	// The late member lands after the handoff.
	drift.on.Store(true)
	writer := now
	c.currentIncidentRequest.DriftEvents = append(c.currentIncidentRequest.DriftEvents, sdk.DriftEvent{
		Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Manager: "kubectl-edit", ManagerAt: &writer, ObservedAt: writer,
		Fields: []sdk.DriftField{{Path: "spec.selector.app", Old: "api", New: "api-old"}}})
	step(3)

	wait := loop.Handle(ctx, ToolRequest{Command: "detect", WaitSeconds: 60})
	if line := waitLine(wait.Text); line != "wait: after 0s, new since the handoff: drift.unexplained Service/api" {
		t.Fatalf("the wait did not return on the new finding: %q", line)
	}
	block := wait.Text[strings.Index(wait.Text, "NEW SINCE THE HANDOFF"):]
	if strings.Count(wait.Text, "Service/api differs from accepted source") != 1 ||
		!strings.Contains(block, "changed spec.selector.app api -> api-old by kubectl-edit at") {
		t.Fatalf("detect must list the late drift once, with field, writer and time:\n%s", wait.Text)
	}
	if strings.Index(wait.Text, "NEW SINCE THE HANDOFF") > strings.Index(wait.Text, "OPENED ON") && strings.Contains(wait.Text, "OPENED ON") {
		t.Fatalf("the new finding must lead the findings:\n%s", wait.Text)
	}
	// A large drift stays bounded and never pushes the opening alert out.
	wide := make([]sdk.DriftField, 12)
	for i := range wide {
		wide[i] = sdk.DriftField{Path: strings.Repeat("spec.template.spec.containers.env", 2), Old: strings.Repeat("o", 40), New: strings.Repeat("n", 40)}
	}
	for range 4 {
		c.currentIncidentRequest.DriftEvents = append(c.currentIncidentRequest.DriftEvents, sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Manager: "kubectl-edit", ObservedAt: writer.Add(time.Second), Fields: wide})
	}
	if r := loop.Handle(ctx, ToolRequest{Command: "detect"}); !strings.Contains(r.Text, "OPENED ON") || !strings.Contains(r.Text, "+9 more fields") {
		t.Fatalf("a wide drift must stay bounded below the opening alert: %s\n%s", r.Error, r.Text)
	}
	// Shown once: the next wait does not return on it again.
	again := loop.Handle(ctx, ToolRequest{Command: "detect", WaitSeconds: 4})
	if line := waitLine(again.Text); !strings.HasPrefix(line, "wait: no change in 4s") {
		t.Fatalf("a shown finding ended a second wait: %q", line)
	}
	step(3)
	if world.restarts.Load() != 0 {
		t.Fatalf("SDO wrote %d times after dispatch", world.restarts.Load())
	}
	if dispatcher.calls != 0 {
		t.Fatalf("dispatched %d more agents, want the one already running", dispatcher.calls)
	}
}

func TestANamedDetectDoesNotMarkNewFindingsShown(t *testing.T) {
	loop, _ := detectWorldLoop(&detectWorld{})
	loop.handoffKeys = map[string]bool{}
	// Every finding of this world is new since an empty handoff, outside an empty alert set.
	if response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "tests"}); response.Error != "" {
		t.Fatal(response.Error)
	}
	if len(loop.newShown) != 0 {
		t.Fatalf("an item read marked %v shown without showing them", loop.newShown)
	}
	loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if len(loop.newShown) == 0 {
		t.Fatal("the summary showed the new findings but did not mark them")
	}
}
