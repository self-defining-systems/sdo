package runtime

import (
	"context"
"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

// Whole-incident observation is never negative application evidence.
func TestSettlementUnconfirmedApplicationIsNeutral(t *testing.T) {
	for _, classification := range []string{"unconfirmed", "failed", "cleared_without_action", "success"} {
		outcome := &contracts.OutcomeRecord{Classification: classification, SourceCommit: "synthetic", PlanApplications: []*contracts.PlanApplication{{ApplicationId: "a", Applied: true, ProgramIds: []string{"p"}, VerificationCompleted: true, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}}
		if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})["p"]; got.Failures != 0 || got.VerifiedSuccesses != 0 {
			t.Fatalf("%s: observation became evidence: %+v", classification, got)
		}
	}
}

func TestSettlementReleasedReservationOnlySupersededByCausalWrite(t *testing.T) {
	loop, _ := pipelineTestLoop(t, false)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	loop.pendingWrites = map[string]pendingMitigation{"a": {Refs: []sdk.ObjectRef{ref}, Released: true}}
	loop.pendingConflict(context.Background(), []sdk.ObjectRef{{Kind: "ConfigMap", Namespace: "app", Name: "peer"}}, &ToolResponse{})
	if loop.pendingWrites["a"].Superseded {
		t.Fatal("independent cause superseded pending repair")
	}
	loop.pendingConflict(context.Background(), []sdk.ObjectRef{ref}, &ToolResponse{})
	if !loop.pendingWrites["a"].Superseded {
		t.Fatal("same-resource write did not supersede repair")
	}
}

func TestSettlementWatchingResponderSkipsLearningWorker(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result.ResponderSessionID = "synthetic-watching-session"
	result.ResolutionPath = "responder"
	result, stages := loop.SettleRecoveryApplications(context.Background(), result, state.PriorStages)
	closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
	closure.Request.PriorStages = stages
	if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
		t.Fatalf("watching session dispatched %d learning workers", calls)
	}
}

// The real stage producer runs all writes first, then observes one shared
// deadline. A logical minute costs milliseconds; no worker/model is involved.
func TestSettlementSharedMinuteRecoveryWithoutModels(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	start := time.Now()
	now := start
	loop.Request.ResponseDeadline = start.Add(3 * time.Minute)
	state.Request.ResponseDeadline = loop.Request.ResponseDeadline
	rollout := &settle2DelayedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: start.Add(time.Minute)}
	loop.Executor.Rollouts = rollout
	calls := 0
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
	stage.recoveryNow = func() time.Time { return now }
	stage.recoveryWait = func(context.Context, time.Duration) error { now = now.Add(15 * time.Second); return nil }
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(state.Findings) != 0 {
		t.Fatalf("covered minute recovery handed off: calls=%d findings=%v", calls, state.Findings)
	}
	if now.Sub(start) != time.Minute {
		t.Fatalf("shared wait=%s", now.Sub(start))
	}
	result, err := responsePipelineResult(state, start, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
	closure.Request.PriorStages = state.PriorStages
	if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
		t.Fatalf("covered minute recovery invoked learning: %d", calls)
	}
}

type settle2DelayedRollout struct {
	base  PlanRollouts
	now   func() time.Time
	ready time.Time
}

func (r *settle2DelayedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Observe(ctx, refs)
}
func (r *settle2DelayedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.now().Before(r.ready) {
		return context.DeadlineExceeded
	}
	return r.base.Wait(ctx, refs)
}

// Execute through the stage, persist/reload its real receipts, then consume the
// recovery projection through outcome derivation, learning and trust.
func TestSettlementProducerConsumerSweep(t *testing.T) {
 for _, session:=range []bool{false,true}{for _, restart:=range []bool{false,true}{for _, delay:=range []time.Duration{0,45*time.Second,90*time.Second}{
  t.Run(fmt.Sprintf("session=%v/restart=%v/delay=%s",session,restart,delay),func(t *testing.T){
   loop,state:=pipelineTestLoop(t,false)
   loop.Executor.Rollouts=&pipelineDeadlineRollout{failure:context.DeadlineExceeded}
   var saved []byte
   loop.saveToolState=func(_ context.Context,b []byte)error{saved=append([]byte(nil),b...);return nil}
   if err:=RunResponseStages(context.Background(),state,[]ResponseStage{&DeterministicResponseStage{NewLoop:func(IncidentRequest)*ResponderLoop{return loop},ObservationCeiling:time.Nanosecond}});err!=nil{t.Fatal(err)}
   before,err:=responsePipelineResult(state,time.Now().Add(-time.Second),time.Now());if err!=nil{t.Fatal(err)}
   if session{before.ResponderSessionID="synthetic-watcher";before.ResolutionPath="responder"}
   if restart {
    restored:=NewResponderLoop(loop.Request,loop.Programs,loop.Provider,loop.Detectors,loop.Executor,5);restored.ReadView=loop.ReadView
    if err:=restored.restoreToolSnapshot(saved);err!=nil{t.Fatal(err)};loop=restored
   }
   loop.Executor.Rollouts=observedDeploymentRollout{client:loop.Executor.Client.(TypedPlanClient).Client}
   result,stages:=loop.SettleRecoveryApplications(context.Background(),before,state.PriorStages)
   at:=time.Now().Add(delay)
   closure:=IncidentClosure{Request:state.Request,Result:&result,VerifiedAt:at,HealthClearedAt:&at,FinalDetectorStates:[]DetectorEvaluation{{DetectorID:loop.Detectors[0].Spec().ID,Status:DetectorEvaluationClear,EvaluatedAt:at}},Frozen:true}
   closure.Request.PriorStages=stages
   derived,err:=(ProtoOutcomeDeriver{}).Derive(context.Background(),closure,"","test","test");if err!=nil{t.Fatal(err)}
   if derived.Record.Classification!="success" || !coveredRepeatCandidate(derived.Record){t.Fatalf("attributable recovery lost: %v",derived.Record)}
   stats:=ProgramTrustStats([]*contracts.OutcomeRecord{derived.Record})
   for _,app:=range derived.Record.PlanApplications{for _,id:=range app.ProgramIds{if got:=stats[id];got.Failures!=0 || got.VerifiedSuccesses!=1{t.Fatalf("application settlement lost trust for %s: %+v",id,got)}}}
   if calls:=settlementBrokerWorkers(t,loop,closure);calls!=0{t.Fatalf("covered recovery dispatched %d workers",calls)}
  })
 }}}
}
