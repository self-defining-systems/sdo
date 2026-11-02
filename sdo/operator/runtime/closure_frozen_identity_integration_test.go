//go:build integration

package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// CLOSE2: the first broker attempt receives the closure-time recheck's notes
// and stores the closure in its ledger, then fails before an outcome commits
// (the controller exits for a health refresh). Live drift changes before the
// retry, and the controller restarts from its durable state. The retry must
// hand the broker the identical closure; recomputing its evidence from later
// live state made every retry fail with "closure differs from persisted broker
// evidence" until the closure permanently failed.
func TestClosureIdentityIsFrozenAcrossBrokerRetryAndRestart(t *testing.T) {
	for _, later := range []struct {
		name string
		diff program.Diff
		err  error
	}{
		{"drift restored", program.Diff{}, nil},
		{"source not comparable", program.Diff{}, errors.New("dry-run: connection refused")},
	} {
		t.Run(later.name, func(t *testing.T) {
			ctx := context.Background()
			target := nativeRepository(t)
			repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			interrupted := true
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
				Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
					if interrupted {
						return DerivedOutcome{}, errors.New("controller exited for health refresh")
					}
					return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
				}),
				Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
					return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
				}),
			}
			view := &switchableSourceView{diff: stillDrifted}
			now := time.Unix(1000, 0).UTC()
			first := newSourceInformationController(t, &windowDetector{confirmed: true}, view.drift(), native, &now)
			id := first.pendingClosure.Request.IncidentID
			workspace, err := native.PrepareIncident(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			first.pendingClosure.Request.RepositoryWorktree, first.pendingClosure.Request.RepositoryBaseCommit = workspace.Worktree, workspace.BaseCommit
			first.pendingClosure.Request.RepairPolicy = "recorded-actions"
			completeDriftFinding(first)

			executeRecheck(t, first)
			awaitBrokerResult(t, first.closureResults)
			first.processBrokerCompletions()
			ledger, err := native.readLedger(id)
			if err != nil || ledger.Closure == nil || ledger.OutcomeCommit != "" || first.closureFailure == nil {
				t.Fatalf("fixture must stop after the ledger stores the closure and before the outcome: ledger=%+v failure=%+v err=%v", ledger, first.closureFailure, err)
			}
			if !strings.Contains(strings.Join(ledger.Closure.GetSourceCheckNotes(), ";"), "unexplained drift remains on Service/front") {
				t.Fatalf("fixture: the first attempt must carry the recheck's drift note: %v", ledger.Closure.GetSourceCheckNotes())
			}
			durable := exportState(t, first)
			if !proto.Equal(durable.GetPendingClosure(), ledger.Closure) {
				t.Fatal("the closure the broker stored is not the controller's durable pending closure")
			}

			interrupted = false
			view.set(later.diff, later.err)
			restarted, err := NewController(testControllerConfig(),
				[]sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault")), &windowDetector{confirmed: true}},
				staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
			if err != nil {
				t.Fatal(err)
			}
			restarted.now = func() time.Time { return now }
			restarted.ConfigureDrift(view.drift())
			if err := restarted.RestoreState(durable); err != nil {
				t.Fatal(err)
			}
			if err := restarted.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			now = now.Add(restarted.config.ClosureRetry.MaxBackoff)
			executeRecheck(t, restarted)
			awaitBrokerResult(t, restarted.closureResults)
			restarted.processBrokerCompletions()
			if restarted.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || restarted.closureFailure != nil {
				t.Fatalf("retry after restart did not commit the same closure: state=%v failure=%+v", restarted.closureState, restarted.closureFailure)
			}
			ledger, err = native.readLedger(id)
			if err != nil || ledger.OutcomeCommit == "" || !proto.Equal(ledger.Closure, durable.GetPendingClosure()) {
				t.Fatalf("committed evidence differs from the first attempt's closure: %+v %v", ledger, err)
			}
		})
	}
}

// A closure that differs from the broker's stored frozen closure names the
// differing fields and is not retried: the same durable closure would fail
// the same comparison on every attempt.
func TestBrokerIdentityMismatchNamesFieldsAndIsNotRetried(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
			return DerivedOutcome{}, errors.New("controller exited for health refresh")
		})}
	workspace, err := native.PrepareIncident(ctx, "identity")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(ctx, closure); err == nil {
		t.Fatal("fixture must interrupt the first attempt")
	}
	changed := closure
	changed.SourceCheckNotes = []string{"recomputed from later live state"}
	_, err = native.ProcessClosure(ctx, changed)
	var mismatch *ClosureIdentityError
	if !errors.As(err, &mismatch) || !mismatch.NonRetryable() || strings.Join(mismatch.Fields, ",") != "source_check_notes" {
		t.Fatalf("mismatch = %v, want a non-retryable error naming source_check_notes", err)
	}
}

