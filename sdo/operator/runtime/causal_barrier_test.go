package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type causalTestReader struct {
	lists     map[sdk.WatchKind]CurrentObjectList
	reads     []sdk.WatchKind
	afterRead func()
}

func (r *causalTestReader) ListCurrent(_ context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	r.reads = append(r.reads, kind)
	if r.afterRead != nil {
		r.afterRead()
	}
	value, ok := r.lists[kind]
	if !ok {
		return CurrentObjectList{}, errors.New("synthetic direct read unavailable")
	}
	return value, nil
}
func causalBarrierFixture(t *testing.T) (*CausalBarrier, *causalTestReader, *selectionJournal, *contracts.SelectionAttempt) {
	t.Helper()
	journal, _, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := journal.accept(context.Background(), menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	service := planObject(menuService, map[string]any{"ports": []any{map[string]any{"targetPort": int64(8080)}}})
	service.SetUID("uid")
	service.SetResourceVersion("opaque:earlier-looking")
	kind := sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}
	reader := &causalTestReader{lists: map[sdk.WatchKind]CurrentObjectList{kind: {Items: []unstructured.Unstructured{*service}, ResourceVersion: "collection-current", Complete: true}}}
	barrier := &CausalBarrier{Journal: journal, Reader: reader, Namespace: "app", Dependencies: []sdk.WatchKind{kind}, AuthorizeRead: func(context.Context, sdk.WatchKind) error { return nil }, CheckFence: func(context.Context) error { return nil }}
	return barrier, reader, journal, attempt
}
func TestCausalBarrierFeedsDirectObjectsAndOpaqueVersionIdentity(t *testing.T) {
	barrier, reader, journal, attempt := causalBarrierFixture(t)
	expected := EffectExpectation{Ref: menuService, UID: "uid", Before: reader.lists[barrier.Dependencies[0]].Items[0].DeepCopy(), After: reader.lists[barrier.Dependencies[0]].Items[0].DeepCopy()}
	_ = unstructured.SetNestedSlice(expected.Before.Object, []any{map[string]any{"targetPort": int64(9999)}}, "spec", "ports")
	observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{expected})
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.reads) != 1 || !observed.Receipt.Complete || observed.View.Services()[0].Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatal("verification did not receive direct current objects")
	}
	observation := observed.Receipt.Observations[0]
	if observation.Uid != "uid" || observation.ResourceVersion != "opaque:earlier-looking" || !observation.EffectConfirmed {
		t.Fatalf("lost causal identity: %+v", observation)
	}
	if len(journal.snapshot().Attempts[0].Barriers) != 1 || !journal.snapshot().Attempts[0].Barriers[0].Complete {
		t.Fatal("barrier not durable")
	}
}
func TestCausalBarrierMissingDependencyAndStalledListFailClosed(t *testing.T) {
	for _, failure := range []string{"missing_reader", "incomplete_list", "missing_dependency", "uid_replaced", "effect_missing", "scope_changed", "deadline", "read_denied"} {
		t.Run(failure, func(t *testing.T) {
			barrier, reader, journal, attempt := causalBarrierFixture(t)
			expected := EffectExpectation{Ref: menuService, UID: "uid", Before: reader.lists[barrier.Dependencies[0]].Items[0].DeepCopy(), After: reader.lists[barrier.Dependencies[0]].Items[0].DeepCopy()}
			_ = unstructured.SetNestedSlice(expected.Before.Object, []any{map[string]any{"targetPort": int64(9999)}}, "spec", "ports")
			switch failure {
			case "missing_reader":
				barrier.Reader = nil
			case "incomplete_list":
				list := reader.lists[barrier.Dependencies[0]]
				list.Complete = false
				reader.lists[barrier.Dependencies[0]] = list
			case "missing_dependency":
				barrier.Dependencies = append(barrier.Dependencies, sdk.WatchKind{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app"})
			case "uid_replaced":
				expected.UID = "old-uid"
			case "effect_missing":
				_ = unstructured.SetNestedSlice(expected.After.Object, []any{map[string]any{"targetPort": int64(9090)}}, "spec", "ports")
			case "scope_changed":
				changed := false
				reader.afterRead = func() { changed = true }
				barrier.CheckFence = func(context.Context) error {
					if changed {
						return ErrStaleHealthInvestigation
					}
					return nil
				}
			case "deadline":
				journal.now = func() time.Time { return journal.state.SelectionLedger.OriginalDeadline.AsTime().Add(time.Second) }
			case "read_denied":
				barrier.AuthorizeRead = func(context.Context, sdk.WatchKind) error { return errors.New("SYNTHETIC-CREDENTIAL-CANARY") }
			}
			observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{expected})
			if err == nil || observed.View != nil || observed.Receipt != nil && observed.Receipt.Complete {
				t.Fatalf("failure yielded usable observation: %+v %v", observed, err)
			}
			if failure == "read_denied" && len(reader.reads) != 0 {
				t.Fatal("unauthorized evidence was fetched")
			}
			for _, capture := range journal.snapshot().Attempts[0].Barriers {
				if capture.Complete {
					t.Fatal("failed capture durably marked complete")
				}
			}
		})
	}
}
func TestCausalBarrierCapturesAreUniqueAcrossPhasesAndReplacement(t *testing.T) {
	barrier, _, journal, attempt := causalBarrierFixture(t)
	ids := map[string]bool{}
	for _, phase := range []string{"before_round", "after_write", "after_compensation", "recovery", "closure", "closure"} {
		observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, phase, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ids[observed.Receipt.CaptureId] || observed.Receipt.CaptureId == "open@detect" {
			t.Fatal("capture reused")
		}
		ids[observed.Receipt.CaptureId] = true
	}
	// Occurrences are counted from the durable attempt, not process-local state.
	replacement := *barrier
	observed, err := replacement.Capture(context.Background(), attempt.AttemptId, 1, "closure", nil)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Receipt.Occurrence != 3 || ids[observed.Receipt.CaptureId] || len(journal.snapshot().Attempts[0].Barriers) != 7 {
		t.Fatal("replacement reused capture identity")
	}
}
func TestCausalBarrierDeletionRequiresAbsenceAndCreationRequiresNewUID(t *testing.T) {
	barrier, reader, _, attempt := causalBarrierFixture(t)
	if _, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{{Ref: menuService, UID: "uid", RequireAbsent: true}}); err == nil {
		t.Fatal("delete acceptance counted as confirmed absence")
	}
	list := reader.lists[barrier.Dependencies[0]]
	list.Items = nil
	reader.lists[barrier.Dependencies[0]] = list
	observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{{Ref: menuService, UID: "uid", RequireAbsent: true}})
	if err != nil || !observed.Receipt.Observations[0].ConfirmedAbsent {
		t.Fatalf("direct absence not recorded: %v", err)
	}
}
func TestCausalBarrierUnlistedKindCannotMasqueradeAsEmpty(t *testing.T) {
	barrier, _, _, attempt := causalBarrierFixture(t)
	observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "before_round", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("unobserved Deployments silently read as empty")
		}
	}()
	_ = observed.View.Deployments()
}

func TestCausalBarrierDoesNotConfirmAFieldRemovalDefaultedToPreimage(t *testing.T) {
	barrier, reader, _, attempt := causalBarrierFixture(t)
	before := reader.lists[barrier.Dependencies[0]].Items[0].DeepCopy()
	after := before.DeepCopy()
	unstructured.RemoveNestedField(after.Object, "spec", "ports")
	observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{{Ref: menuService, UID: "uid", Before: before, After: after}})
	if err == nil || observed.View != nil {
		t.Fatal("pre-image default was mistaken for the intended removal")
	}
}

func TestCausalBarrierCreationRequiresDurablePriorAbsence(t *testing.T) {
	barrier, reader, _, attempt := causalBarrierFixture(t)
	kind := barrier.Dependencies[0]
	list := reader.lists[kind]
	created := list.Items[0].DeepCopy()
	created.SetUID("new-uid")
	list.Items = nil
	reader.lists[kind] = list
	absent, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "before_round", []EffectExpectation{{Ref: menuService, RequireAbsent: true}})
	if err != nil {
		t.Fatal(err)
	}
	list.Items = []unstructured.Unstructured{*created}
	reader.lists[kind] = list
	if _, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{{Ref: menuService, After: created, PriorAbsenceCaptureID: "invented"}}); err == nil {
		t.Fatal("unproven prior absence authorized create observation")
	}
	observed, err := barrier.Capture(context.Background(), attempt.AttemptId, 1, "after_write", []EffectExpectation{{Ref: menuService, After: created, PriorAbsenceCaptureID: absent.Receipt.CaptureId}})
	if err != nil || !observed.Receipt.Observations[0].EffectConfirmed || observed.Receipt.Observations[0].Uid != "new-uid" {
		t.Fatalf("new UID not observed against prior absence: %v", err)
	}
}
