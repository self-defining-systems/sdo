package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestSelectionMutationOwnerDrainsBeforeAdoption(t *testing.T) {
	ctx := context.Background()
	journal, _, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := journal.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	owner := newMutationOwner(journal)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- owner.run(ctx, 1, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	adopted := make(chan error, 1)
	go func() { adopted <- owner.revokeAndAdopt(ctx, "source-b") }()
	select {
	case err := <-adopted:
		t.Fatalf("adopted before API call drained: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-adopted; err != nil {
		t.Fatal(err)
	}
	if journal.snapshot().DispatchEpoch != 2 || journal.snapshot().WritesFrozen {
		t.Fatal("drained adoption did not retain write capability")
	}
	if err := owner.run(ctx, 1, func(context.Context) error { t.Fatal("old epoch executed"); return nil }); err == nil {
		t.Fatal("old owner authorized")
	}
	if journal.snapshot().Attempts[0].AttemptId != attempt.AttemptId {
		t.Fatal("adoption erased selection")
	}
}
func TestSelectionMutationOwnerFailedDrainPermanentlyFreezes(t *testing.T) {
	ctx := context.Background()
	journal, _, _, _ := selectionLedgerFixture(t)
	owner := newMutationOwner(journal)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- owner.run(ctx, 1, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	drainCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := owner.revokeAndAdopt(drainCtx, "source-b"); err == nil {
		t.Fatal("unproven drain succeeded")
	}
	close(release)
	<-finished
	if !journal.snapshot().WritesFrozen {
		t.Fatal("lost drain did not durably freeze")
	}
	if err := owner.run(ctx, 2, func(context.Context) error { t.Fatal("fallback executed"); return nil }); err == nil {
		t.Fatal("frozen fallback authorized")
	}
}
func TestSelectionLedgerRecoveryNeverReplaysAndChargesUncertainty(t *testing.T) {
	for _, cut := range []string{"accepted", "intent", "completion"} {
		t.Run(cut, func(t *testing.T) {
			ctx := context.Background()
			journal, store, menu, selection := selectionLedgerFixture(t)
			attempt, _, err := journal.accept(ctx, menu, selection)
			if err != nil {
				t.Fatal(err)
			}
			if cut != "accepted" {
				receipt := &contracts.MitigationActionReceipt{ActionId: "action", Resource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}, Effect: contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
				if err := journal.beginAction(ctx, attempt.AttemptId, 1, "forward", receipt); err != nil {
					t.Fatal(err)
				}
				if cut == "completion" {
					if err := journal.completeAction(ctx, attempt.AttemptId, "action", false, "uid", "rv"); err != nil {
						t.Fatal(err)
					}
				}
			}
			replacement, err := openSelectionJournal(ctx, store, IncidentRequest{IncidentID: "incident"}, "ignored", 50)
			if err != nil {
				t.Fatal(err)
			}
			if err := replacement.adopt(ctx, "source-b", true); err != nil {
				t.Fatal(err)
			}
			verified := 0
			err = replacement.recover(ctx, func(context.Context, *contracts.SelectionAttempt) (*contracts.MitigationBarrier, error) {
				verified++
				return &contracts.MitigationBarrier{CaptureId: "fresh-recovery", AttemptId: attempt.AttemptId, Phase: "recovery", Occurrence: 1, ObservedAt: menu.OriginalDeadline, Complete: true}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			state := replacement.snapshot()
			report := state.Attempts[0].Report
			if verified != 1 || report == nil || report.Status != contracts.MitigationReport_STATUS_INTERRUPTED {
				t.Fatalf("recovery did not visibly settle: %+v", state)
			}
			if cut == "intent" && state.UsedBatches != 2 {
				t.Fatal("uncertain request released reservation")
			}
			if cut == "completion" && state.UsedBatches != 1 {
				t.Fatal("completed round lost accounting")
			}
			if cut == "accepted" && state.UsedBatches != 0 {
				t.Fatal("acceptance without intent charged execution")
			}
			if report.AcceptanceEpoch != 1 || len(report.Barriers) != 1 {
				t.Fatal("recovery lost identity or verification")
			}
			if err := replacement.recover(ctx, func(context.Context, *contracts.SelectionAttempt) (*contracts.MitigationBarrier, error) {
				t.Fatal("terminal report reverified/reexecuted")
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSelectionLedgerRecoveryWithoutFreshBarrierFailsVisibly(t *testing.T) {
	ctx := context.Background()
	journal, _, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := journal.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.recover(ctx, func(context.Context, *contracts.SelectionAttempt) (*contracts.MitigationBarrier, error) {
		return nil, errors.New("synthetic read failure")
	}); err != nil {
		t.Fatal(err)
	}
	report := journal.snapshot().Attempts[0].Report
	if report == nil || report.AttemptId != attempt.AttemptId || report.Status != contracts.MitigationReport_STATUS_INTERRUPTED || report.ReasonCode != "recovery_observation_unavailable" {
		t.Fatalf("fresh read failure hidden: %+v", report)
	}
}

func TestSelectionLedgerRecoveryRetainsCompensationUncertainty(t *testing.T) {
	ctx := context.Background()
	journal, _, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := journal.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &contracts.MitigationActionReceipt{ActionId: "action", Resource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}, Effect: contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
	if err := journal.beginAction(ctx, attempt.AttemptId, 1, "forward", receipt); err != nil {
		t.Fatal(err)
	}
	if err := journal.completeAction(ctx, attempt.AttemptId, "action", false, "uid", "rv"); err != nil {
		t.Fatal(err)
	}
	if err := journal.beginAction(ctx, attempt.AttemptId, 1, "compensation", receipt); err != nil {
		t.Fatal(err)
	}
	if err := journal.recover(ctx, nil); err != nil {
		t.Fatal(err)
	}
	report := journal.snapshot().Attempts[0].Report
	action := report.Rounds[0].Actions[0]
	if action.State != contracts.MitigationActionReceipt_STATE_APPLIED || action.CompensationState != contracts.MitigationActionReceipt_STATE_UNKNOWN {
		t.Fatalf("compensation intent lost: %+v", action)
	}
	if journal.snapshot().UsedBatches != 2 {
		t.Fatal("uncertain compensation released budget")
	}
}
