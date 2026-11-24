package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
)

// headWorld is the advance fake world with a branch head someone else pushed.
type headWorld struct {
	*advWorld
	needed   bool
	recorded []broker.SourceEvidence
	busy     string
	pause    *recordedPause
	holds    *memoryHolds
}

type memoryHolds struct{ held map[string]string }

func (h *memoryHolds) Held(_ context.Context, change broker.HeadChange) (string, bool, error) {
	detail, ok := h.held[change.EffectiveTree]
	return detail, ok, nil
}

func (h *memoryHolds) Hold(_ context.Context, change broker.HeadChange, reason lifecycle.HoldReason, detail string) error {
	h.held[change.EffectiveTree] = string(reason) + ": " + detail
	return nil
}

type recordedPause struct {
	events *[]string
	acks   bool
}

func (p *recordedPause) Pause(context.Context, string, time.Time) error {
	*p.events = append(*p.events, "pause")
	return nil
}
func (p *recordedPause) Acknowledged(context.Context) (bool, error) { return p.acks, nil }
func (p *recordedPause) Resume(context.Context) error {
	*p.events = append(*p.events, "resume")
	return nil
}

func (w *headWorld) HeadToDeploy(context.Context) (broker.HeadChange, bool, error) {
	w.log("head")
	return broker.HeadChange{Commit: advCandidate, EffectiveTree: advEffective, PatchesDigest: advDigest}, w.needed, nil
}

func (w *headWorld) HeadCheckout(_ context.Context, commit string) (string, func(), error) {
	w.log("checkout %s", commit[:4])
	return advCheckout, func() { w.log("release") }, nil
}

func (w *headWorld) TryAcceptedDeploymentRevision(context.Context) (string, error) {
	return "", nil
}

func (w *headWorld) RecordHeadDeployment(_ context.Context, commit string, evidence broker.SourceEvidence) error {
	w.log("record %s", commit[:4])
	w.recorded = append(w.recorded, evidence)
	return nil
}

func (w *headWorld) deployer() lifecycle.HeadDeployer {
	repair := lifecycle.RepairDeployer{Readiness: w, Verifier: w, Wait: func(context.Context, time.Duration) error { return nil }, Now: func() time.Time { return time.Unix(1, 0) }}
	if w.pause != nil {
		repair.Maintenance = w.pause
		repair.Busy = func(context.Context) (string, error) { return w.busy, nil }
	}
	deployer := lifecycle.HeadDeployer{Broker: w, Deployer: repair}
	if w.holds != nil {
		deployer.Holds = w.holds
	}
	return deployer
}

func newHeadWorld(t *testing.T, needed bool) *headWorld {
	t.Helper()
	return &headWorld{advWorld: newAdvWorld(t), needed: needed}
}

// A head that is already the newest deployment does nothing.
func TestHeadCurrentDeploysNothing(t *testing.T) {
	w := newHeadWorld(t, false)
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadCurrent || !slices.Equal(w.events, []string{"head"}) {
		t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
	}
}

// A new head is applied from a clean checkout of that commit, verified by the
// independent verifier, and only then recorded as the newest deployment.
func TestANewHeadIsDeployedVerifiedThenRecorded(t *testing.T) {
	w := newHeadWorld(t, true)
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeployed || result.Commit != advCandidate {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	want := []string{"head", "checkout 4444", "apply", "verify 4444", "record 4444", "release"}
	if !slices.Equal(w.events, want) || len(w.recorded) != 1 || w.recorded[0].EffectiveTree != advEffective {
		t.Fatalf("events\n got %q\nwant %q; recorded %+v", w.events, want, w.recorded)
	}
}

// A head the cluster refuses is held: nothing is recorded.
func TestAHeadTheClusterRefusesIsHeldAndNotRecorded(t *testing.T) {
	w := newHeadWorld(t, true)
	w.applyErrs = []error{errors.New("deployment never became ready")}
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadHeld || result.Hold != lifecycle.HoldRollout || len(w.recorded) != 0 {
		t.Fatalf("result = %+v, err = %v, recorded %d", result, err, len(w.recorded))
	}
}

// With an open incident the deploy never starts: the pause is lifted and the
// head is tried again later.
func TestAHeadIsDeferredWhileAnIncidentIsOpen(t *testing.T) {
	w := newHeadWorld(t, true)
	w.pause, w.busy = &recordedPause{events: &w.events, acks: true}, "an incident is open"
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeferred || result.Detail != "an incident is open" {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if slices.Contains(w.events, "apply") || w.events[len(w.events)-1] != "resume" {
		t.Fatalf("events %q: nothing may be applied and the pause must be lifted", w.events)
	}
}

// A refused head is held by its effective source: the next poll does not apply
// it again (and does not pause detection), however many SDO memory commits
// have landed on top.
func TestARefusedHeadIsHeldAndNotAppliedAgain(t *testing.T) {
	w := newHeadWorld(t, true)
	w.holds = &memoryHolds{held: map[string]string{}}
	w.applyErrs = []error{errors.New("deployment never became ready")}
	first, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || first.Outcome != lifecycle.HeadHeld {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	w.events = nil
	second, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || second.Outcome != lifecycle.HeadHeld || slices.Contains(w.events, "apply") || slices.Contains(w.events, "checkout 4444") {
		t.Fatalf("second = %+v, err = %v, events %q: a held head must not be applied again", second, err, w.events)
	}
}

// deployedNotices is memoryHolds that also keeps the notice of every head deployed.
type deployedNotices struct {
	*memoryHolds
	deployed []broker.HeadChange
}

func (d *deployedNotices) Deployed(_ context.Context, change broker.HeadChange, _ lifecycle.HeadResult) error {
	d.deployed = append(d.deployed, change)
	return nil
}

// A head SDO deployed adds no commit and no trailer to history (it is someone else's commit), so whoever pushed it
// (a benchmark's replayer, an engineer's tooling) learns it was deployed from the notice the holds keep beside a
// refusal: told once per deployed head, never for a held one.
func TestADeployedHeadIsNoticedAndAHeldOneIsNot(t *testing.T) {
	w := newHeadWorld(t, true)
	notices := &deployedNotices{memoryHolds: &memoryHolds{held: map[string]string{}}}
	w.holds = notices.memoryHolds
	deployer := w.deployer()
	deployer.Holds = notices
	if result, err := deployer.Deploy(context.Background(), repairConfig); err != nil || result.Outcome != lifecycle.HeadDeployed {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if len(notices.deployed) != 1 || notices.deployed[0].Commit != advCandidate {
		t.Fatalf("notices %+v: want one, for the deployed head", notices.deployed)
	}
	refused := newHeadWorld(t, true)
	refused.applyErrs = []error{errors.New("deployment never became ready")}
	held := &deployedNotices{memoryHolds: &memoryHolds{held: map[string]string{}}}
	deployer = refused.deployer()
	deployer.Holds = held
	if result, _ := deployer.Deploy(context.Background(), repairConfig); result.Outcome != lifecycle.HeadHeld || len(held.deployed) != 0 {
		t.Fatalf("a held head must not be noticed as deployed: %+v %+v", result, held.deployed)
	}
}

// A detector that fires in the minute after a rollout and clears by itself
// (a queue's retried jobs failing while a worker restarts) must not decide a
// head: the same release was accepted in one run and held in the next
// depending on whether the single evaluation landed inside that minute. The
// refusal needs a fault that is still there when the bounded window ends.
func TestAHeadWhoseDetectorsFireOnlyTransientlyIsDeployed(t *testing.T) {
	w := newHeadWorld(t, true)
	firing := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "accepted health detectors report firing", Detectors: []lifecycle.DetectorState{{DetectorID: "request-metrics", Status: "firing", Reason: "a queue's jobs failed"}}}
	w.verdicts = []lifecycle.HealthVerdict{firing, firing, {Outcome: lifecycle.VerificationClear}}
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeployed || len(w.recorded) != 1 {
		t.Fatalf("result = %+v, err = %v, recorded %d: a transient firing held the head", result, err, len(w.recorded))
	}
}

// A fault that persists through the whole window still holds the head, with
// the verdict the last evaluation gave.
func TestAHeadWhoseDetectorsKeepFiringIsHeld(t *testing.T) {
	w := newHeadWorld(t, true)
	firing := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "accepted health detectors report firing", Detectors: []lifecycle.DetectorState{{DetectorID: "request-metrics", Status: "firing", Reason: "still failing"}}}
	w.verdicts = []lifecycle.HealthVerdict{firing}
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadHeld || result.Hold != lifecycle.HoldHealth || len(w.recorded) != 0 {
		t.Fatalf("result = %+v, err = %v, recorded %d", result, err, len(w.recorded))
	}
	verifies := 0
	for _, event := range w.events {
		if len(event) >= 6 && event[:6] == "verify" {
			verifies++
		}
	}
	if verifies < 2 {
		t.Fatalf("verified %d time(s) in events %q, want the window re-evaluated before holding", verifies, w.events)
	}
}

// sessionWorld is a head world whose verifier can begin before the rollout: its session records the order of
// the deployment's own steps and answers the verdict from what it observed, never through VerifyCandidate.
type sessionWorld struct {
	*headWorld
	pins    []rendered.ReceiptPins
	readyAt []time.Time
}

func (w *sessionWorld) BeginCandidate(_ context.Context, root, commit, namespace, objective string) (lifecycle.CandidateSession, error) {
	w.log("begin %s", commit[:4])
	return &fakeSession{world: w}, nil
}

func (w *sessionWorld) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	receipt, err := w.headWorld.ApplyAndVerify(ctx, checkout, plan)
	if hook := lifecycle.AppliedHookFrom(ctx); hook != nil {
		hook(rendered.ReceiptPins{EffectiveTree: advEffective, BaseTree: advTree})
	}
	return receipt, err
}

type fakeSession struct{ world *sessionWorld }

func (s *fakeSession) Applied(_ context.Context, pins rendered.ReceiptPins) error {
	s.world.log("applied")
	s.world.pins = append(s.world.pins, pins)
	return nil
}

func (s *fakeSession) Verdict(_ context.Context, _ lifecycle.DeploymentReceipt, readyAt time.Time) (lifecycle.HealthVerdict, error) {
	s.world.log("session verdict")
	s.world.readyAt = append(s.world.readyAt, readyAt)
	return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear}, nil
}

func (s *fakeSession) Close() { s.world.log("session closed") }

// The verdict's preparation overlaps the rollout: the session begins before the apply, is told when the
// last phase is applied, and gives the verdict, so VerifyCandidate (a cold evaluation after the rollout) is
// never called.
func TestAHeadVerifierThatCanBeginBeforeTheRolloutDoes(t *testing.T) {
	w := &sessionWorld{headWorld: newHeadWorld(t, true)}
	deployer := lifecycle.HeadDeployer{Broker: w, Deployer: lifecycle.RepairDeployer{Readiness: w, Verifier: w, Wait: func(context.Context, time.Duration) error { return nil }, Now: func() time.Time { return time.Unix(1, 0) }}}
	result, err := deployer.Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeployed {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	want := []string{"head", "checkout 4444", "begin 4444", "apply", "applied", "session verdict", "record 4444", "session closed", "release"}
	if !slices.Equal(w.events, want) {
		t.Fatalf("events\n got %q\nwant %q", w.events, want)
	}
	if len(w.pins) != 1 || len(w.readyAt) != 1 {
		t.Fatalf("pins %v, readyAt %v: want one of each", w.pins, w.readyAt)
	}
}

func TestHeadHoldsAnUnreadableOpenerAfterTheBound(t *testing.T) {
	w := newHeadWorld(t, true)
	w.verdicts = []lifecycle.HealthVerdict{{
		Outcome:   lifecycle.VerificationPending,
		Summary:   "accepted health detectors report unavailable",
		Detectors: []lifecycle.DetectorState{{DetectorID: "request-metrics", Status: "unavailable", Reason: "scrape /metrics: HTTP 503"}},
	}}
	cfg := repairConfig
	cfg.PendingEvaluations = 3
	result, err := w.deployer().Deploy(context.Background(), cfg)
	if err != nil || result.Outcome != lifecycle.HeadHeld || result.Hold != lifecycle.HoldHealth || len(w.recorded) != 0 {
		t.Fatalf("result = %+v, err = %v, recorded %d", result, err, len(w.recorded))
	}
	count := 0
	for _, event := range w.events {
		if strings.HasPrefix(event, "verify ") {
			count++
		}
	}
	if count != cfg.PendingEvaluations || !strings.Contains(result.Detail, "scrape /metrics: HTTP 503") {
		t.Fatalf("events = %q, result = %+v; want the bounded retries and the failed read", w.events, result)
	}
}
