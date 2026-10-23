package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A genuine CAS edge, shared by independent replacement owners.
type selectionTestStore struct {
	mu       sync.Mutex
	state    *contracts.ControllerState
	revision int
	fail     bool
}

func (s *selectionTestStore) Load(context.Context) (*contracts.ControllerState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return &contracts.ControllerState{Version: RuntimeStateVersion}, "", nil
	}
	return proto.Clone(s.state).(*contracts.ControllerState), fmtRevision(s.revision), nil
}
func fmtRevision(revision int) string { return time.Unix(int64(revision), 0).Format(time.RFC3339) }
func (s *selectionTestStore) Save(_ context.Context, state *contracts.ControllerState, revision string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expected := ""
	if s.revision > 0 {
		expected = fmtRevision(s.revision)
	}
	if expected != revision {
		return "", ErrStateConflict
	}
	if s.fail {
		return "", errors.New("synthetic unavailable")
	}
	s.revision++
	s.state = proto.Clone(state).(*contracts.ControllerState)
	return fmtRevision(s.revision), nil
}
func selectionLedgerFixture(t *testing.T) (*selectionJournal, *selectionTestStore, *contracts.MitigationMenu, *contracts.MitigationSelection) {
	t.Helper()
	ctx := context.Background()
	store := &selectionTestStore{}
	request := IncidentRequest{IncidentID: "incident", SourceCommit: "source-a", Namespace: "app", ResponseDeadline: time.Now().Add(10 * time.Minute)}
	ledger, err := openSelectionJournal(ctx, store, request, "session", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.checkpoint(ctx, "codex", "provider-session", "checkpoint-digest"); err != nil {
		t.Fatal(err)
	}
	menu := &contracts.MitigationMenu{Version: 1, IncidentId: "incident", SessionId: "session", DispatchEpoch: 1, MenuId: "menu", SourceCommit: "source-a", ScopeDigest: "scope", PolicyDigest: "policy", RoundLimit: 2, OriginalDeadline: timestamppb.New(request.ResponseDeadline), RemainingBatches: 5, Options: []*contracts.MitigationOption{{OptionId: "route", ProgramId: "program", ProgramRevision: "program-rev", DependencyRevision: "dependencies", Effects: []contracts.MitigationOption_Effect{contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD}, MaxActionsPerRound: 2}}}
	selection := &contracts.MitigationSelection{Version: 1, IncidentId: "incident", SessionId: "session", DispatchEpoch: 1, RequestId: "request", MenuId: "menu", OptionId: "route", ProgramRevision: "program-rev", DependencyRevision: "dependencies", SourceCommit: "source-a", ScopeDigest: "scope", PolicyDigest: "policy", RoundLimit: 2}
	return ledger, store, menu, selection
}
func TestSelectionLedgerRequiresCheckpointBeforeReservation(t *testing.T) {
	ctx := context.Background()
	ledger, _, menu, selection := selectionLedgerFixture(t)
	ledger.state.SelectionLedger.ProviderSessionId = ""
	if _, _, err := ledger.accept(ctx, menu, selection); err == nil {
		t.Fatal("accepted without a provider checkpoint")
	}
	if ledger.snapshot().ReservedBatches != 0 {
		t.Fatal("rejected acceptance reserved budget")
	}
}
func TestSelectionLedgerAtomicReservationAndSemanticRetry(t *testing.T) {
	ctx := context.Background()
	ledger, store, menu, selection := selectionLedgerFixture(t)
	attempt, retry, err := ledger.accept(ctx, menu, selection)
	if err != nil || retry {
		t.Fatalf("accept: %v %v", retry, err)
	}
	state := ledger.snapshot()
	if state.ReservedBatches != 2 || state.UsedBatches != 0 || len(state.Attempts) != 1 {
		t.Fatalf("not atomic: %+v", state)
	}
	retried, retry, err := ledger.accept(ctx, menu, proto.Clone(selection).(*contracts.MitigationSelection))
	if err != nil || !retry || !proto.Equal(attempt, retried) {
		t.Fatalf("retry changed acceptance: %v %v", retry, err)
	}
	changed := proto.Clone(selection).(*contracts.MitigationSelection)
	changed.RoundLimit = 1
	if _, _, err := ledger.accept(ctx, menu, changed); err == nil {
		t.Fatal("request ID reused for different payload")
	}
	store.fail = true
	other := proto.Clone(selection).(*contracts.MitigationSelection)
	other.RequestId = "second"
	if _, _, err := ledger.accept(ctx, menu, other); err == nil {
		t.Fatal("accepted without durable reservation")
	}
	if len(ledger.snapshot().Attempts) != 1 {
		t.Fatal("failed CAS changed local accepted state")
	}
	if !ledger.frozen {
		t.Fatal("persistence failure did not freeze owner")
	}
}
func TestSelectionLedgerCanonicalBindingsAndEpoch(t *testing.T) {
	_, _, _, selection := selectionLedgerFixture(t)
	a := &contracts.Binding{Name: "a", Object: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}}
	b := &contracts.Binding{Name: "b", Object: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "b"}}
	selection.Bindings = []*contracts.Binding{b, a}
	first, err := selectionPayloadHash(selection)
	if err != nil {
		t.Fatal(err)
	}
	selection.Bindings = []*contracts.Binding{a, b}
	selection.DispatchEpoch = 2
	second, err := selectionPayloadHash(selection)
	if err != nil || first != second {
		t.Fatalf("semantic hash changed: %v", err)
	}
	selection.Bindings = []*contracts.Binding{a, a}
	if _, err := selectionPayloadHash(selection); err == nil {
		t.Fatal("duplicate binding accepted")
	}
}
func TestSelectionLedgerSettlementIsImmutableAndReleasesOnce(t *testing.T) {
	ctx := context.Background()
	ledger, _, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := ledger.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	report := &contracts.MitigationReport{Version: 1, IncidentId: "incident", SessionId: "session", RequestId: "request", AttemptId: attempt.AttemptId, AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_UNRESOLVED}
	settled, err := ledger.settle(ctx, attempt.AttemptId, report, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.snapshot().UsedBatches != 1 || ledger.snapshot().ReservedBatches != 0 || settled.RemainingBatches != 4 {
		t.Fatal("bad settlement accounting")
	}
	settled.Status = contracts.MitigationReport_STATUS_SUCCESS
	if ledger.snapshot().Attempts[0].Report.Status != contracts.MitigationReport_STATUS_UNRESOLVED {
		t.Fatal("caller mutated durable report")
	}
	duplicate, err := ledger.settle(ctx, attempt.AttemptId, report, 1)
	if err != nil || duplicate.Status != report.Status {
		t.Fatalf("idempotent settle: %v", err)
	}
	report.Status = contracts.MitigationReport_STATUS_SUCCESS
	if _, err := ledger.settle(ctx, attempt.AttemptId, report, 1); err == nil {
		t.Fatal("terminal report overwritten")
	}
	if ledger.snapshot().UsedBatches != 1 {
		t.Fatal("settlement charged twice")
	}
}
func TestSelectionLedgerAdoptionRetainsLimitsAndFencesLateCalls(t *testing.T) {
	ctx := context.Background()
	ledger, store, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := ledger.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	before := ledger.snapshot()
	replacement, err := openSelectionJournal(ctx, store, IncidentRequest{IncidentID: "incident", SourceCommit: "source-b", Namespace: "app", ResponseDeadline: time.Now().Add(time.Hour)}, "new-session", 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.adopt(ctx, "source-b", true); err != nil {
		t.Fatal(err)
	}
	after := replacement.snapshot()
	if after.SessionId != before.SessionId || !proto.Equal(after.OriginalDeadline, before.OriginalDeadline) || after.TotalBatches != 5 || after.DispatchEpoch != 2 || after.ReservedBatches != 2 {
		t.Fatalf("adoption reset identity/limits: %+v", after)
	}
	if after.Attempts[0].AcceptanceEpoch != 1 || after.Attempts[0].AttemptId != attempt.AttemptId {
		t.Fatal("adoption rewrote acceptance")
	}
	if _, _, err := replacement.accept(ctx, menu, selection); err == nil {
		t.Fatal("accepted queued stale epoch")
	}
	if err := ledger.beginAction(ctx, attempt.AttemptId, 1, "forward", &contracts.MitigationActionReceipt{ActionId: "late", Resource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}, Effect: contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}); err == nil {
		t.Fatal("old owner wrote intent after adoption")
	}
}
func TestSelectionLedgerUnprovenDrainFreezesAllWrites(t *testing.T) {
	ctx := context.Background()
	ledger, store, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := ledger.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	action := &contracts.MitigationActionReceipt{ActionId: "action", Resource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}, Effect: contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
	if err := ledger.beginAction(ctx, attempt.AttemptId, 1, "forward", action); err != nil {
		t.Fatal(err)
	}
	replacement, err := openSelectionJournal(ctx, store, IncidentRequest{IncidentID: "incident", SourceCommit: "source-a"}, "ignored", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.adopt(ctx, "source-a", false); err != nil {
		t.Fatal(err)
	}
	if !replacement.snapshot().WritesFrozen {
		t.Fatal("unknown old request did not freeze")
	}
	if err := replacement.authorizeMutation(ctx, 2); err == nil {
		t.Fatal("fallback authorized without quiescence")
	}
	if err := ledger.completeAction(ctx, attempt.AttemptId, "action", false, "uid", "rv"); err == nil {
		t.Fatal("late completion accepted")
	}
	if err := replacement.authorizeMutation(ctx, 1); err == nil {
		t.Fatal("stale epoch authorized")
	}
}
func TestSelectionLedgerSharedLegacyBudget(t *testing.T) {
	ctx := context.Background()
	ledger, _, menu, selection := selectionLedgerFixture(t)
	if err := ledger.chargeLegacy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ledger.accept(ctx, menu, selection); err != nil {
		t.Fatal(err)
	}
	another := proto.Clone(selection).(*contracts.MitigationSelection)
	another.RequestId = "other"
	if _, _, err := ledger.accept(ctx, menu, another); err != nil {
		t.Fatal(err)
	}
	if err := ledger.chargeLegacy(ctx); err == nil {
		t.Fatal("legacy apply spent reserved budget")
	}
	if ledger.snapshot().UsedBatches != 1 || ledger.snapshot().ReservedBatches != 4 {
		t.Fatal("shared accounting changed")
	}
}
func TestSelectionLedgerRejectsSelectionFenceMismatch(t *testing.T) {
	for _, field := range []string{"incident", "session", "epoch", "menu", "source", "scope", "policy", "program", "dependency", "option", "bindings", "rounds", "deadline"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			ledger, _, menu, s := selectionLedgerFixture(t)
			switch field {
			case "incident":
				s.IncidentId = "other"
			case "session":
				s.SessionId = "other"
			case "epoch":
				s.DispatchEpoch++
			case "menu":
				s.MenuId = "other"
			case "source":
				s.SourceCommit = "other"
			case "scope":
				s.ScopeDigest = "other"
			case "policy":
				s.PolicyDigest = "other"
			case "program":
				s.ProgramRevision = "other"
			case "dependency":
				s.DependencyRevision = "other"
			case "option":
				menu.Options[0].DisabledReason = "unsupported"
			case "bindings":
				s.Bindings = []*contracts.Binding{{Name: "extra", Object: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "a"}}}
			case "rounds":
				s.RoundLimit = 3
			case "deadline":
				ledger.now = func() time.Time { return menu.OriginalDeadline.AsTime() }
			}
			if _, _, err := ledger.accept(ctx, menu, s); err == nil {
				t.Fatal("accepted invalid fence")
			}
			if ledger.snapshot().ReservedBatches != 0 {
				t.Fatal("refusal spent budget")
			}
		})
	}
}
