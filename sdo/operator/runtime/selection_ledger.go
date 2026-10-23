package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

var errSelectionFenced = errors.New("selection owner fenced; writes prohibited")
var selectionToken = regexp.MustCompile(`^[a-zA-Z0-9._:-]{1,128}$`)

// selectionJournal is one incident's separate StateStore CAS document. A failed
// save freezes this instance, including ambiguous writes: only read-only recovery
// may decide what landed. No method retries a mutation or stores a pre-image.
// mu serializes journal operations; mutationOwner (below) drains actual calls.
type selectionJournal struct {
	mu       sync.Mutex
	store    StateStore
	state    *contracts.ControllerState
	revision string
	epoch    uint64
	frozen   bool
	now      func() time.Time
}

func openSelectionJournal(ctx context.Context, store StateStore, request IncidentRequest, session string, maxBatches int) (*selectionJournal, error) {
	if store == nil {
		return nil, errors.New("selection ledger requires a durable CAS store")
	}
	state, revision, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	j := &selectionJournal{store: store, state: state, revision: revision, now: time.Now}
	if state == nil {
		j.state = &contracts.ControllerState{Version: RuntimeStateVersion}
	}
	if existing := j.state.SelectionLedger; existing != nil {
		if existing.IncidentId != request.IncidentID {
			return nil, errors.New("selection ledger incident mismatch")
		}
		if err := wire.Validate(existing); err != nil {
			return nil, fmt.Errorf("invalid selection ledger: %w", err)
		}
		if err := validateSelectionLedger(existing); err != nil {
			return nil, err
		}
		j.epoch = existing.DispatchEpoch
		return j, nil
	}
	if request.IncidentID == "" || session == "" || request.SourceCommit == "" || request.ResponseDeadline.IsZero() || maxBatches <= 0 || maxBatches > 100 {
		return nil, errors.New("selection ledger requires incident, session, source, original deadline and bounded budget")
	}
	// Migrate counters in the same genesis CAS. A crash between separate
	// genesis/migration writes must never restore a fresh budget.
	used := 0
	if len(j.state.ResponderToolStateJson) > 0 {
		var legacy responderToolState
		if json.Unmarshal(j.state.ResponderToolStateJson, &legacy) != nil || (legacy.Version != 1 && legacy.Version != 2) || legacy.IncidentID != request.IncidentID || legacy.Batches < 0 || legacy.MaxBatches < 0 {
			return nil, errors.New("invalid legacy incident budget")
		}
		used = legacy.Batches
		if legacy.MaxBatches > 0 && legacy.MaxBatches < maxBatches {
			maxBatches = legacy.MaxBatches
		}
		if used > maxBatches {
			return nil, errors.New("legacy incident budget exceeds original limit")
		}
	}
	next := proto.Clone(j.state).(*contracts.ControllerState)
	next.SelectionLedger = &contracts.SelectionLedger{Version: 1, IncidentId: request.IncidentID, SessionId: session, OriginalDeadline: timestamppb.New(request.ResponseDeadline), DispatchEpoch: 1, SourceHistory: []string{request.SourceCommit}, TotalBatches: uint32(maxBatches), UsedBatches: uint32(used), OwnerQuiescent: true}
	if err := j.save(ctx, next); err != nil {
		return nil, err
	}
	j.epoch = 1
	return j, nil
}

func (j *selectionJournal) snapshot() *contracts.SelectionLedger {
	j.mu.Lock()
	defer j.mu.Unlock()
	return proto.Clone(j.state.SelectionLedger).(*contracts.SelectionLedger)
}
func (j *selectionJournal) save(ctx context.Context, next *contracts.ControllerState) error {
	if err := validateSelectionLedger(next.SelectionLedger); err != nil {
		j.frozen = true
		return err
	}
	if err := wire.Validate(next); err != nil {
		j.frozen = true
		return err
	}
	revision, err := j.store.Save(ctx, next, j.revision)
	if err != nil {
		j.frozen = true
		return fmt.Errorf("persist selection facts: %w", err)
	}
	j.state, j.revision = next, revision
	return nil
}
func (j *selectionJournal) next() *contracts.ControllerState {
	return proto.Clone(j.state).(*contracts.ControllerState)
}
func (j *selectionJournal) writable(epoch uint64) error {
	s := j.state.SelectionLedger
	if j.frozen || s.WritesFrozen || s.Closed || epoch != j.epoch || epoch != s.DispatchEpoch {
		return errSelectionFenced
	}
	if !j.now().Before(s.OriginalDeadline.AsTime()) {
		return context.DeadlineExceeded
	}
	return nil
}

// authorizeMutation is a read check only. A write must also hold mutationOwner
// for the entire call and durably record an intent before entering the backend.
func (j *selectionJournal) authorizeMutation(ctx context.Context, epoch uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.writable(epoch); err != nil {
		return err
	}
	return j.currentEpochLocked(ctx, epoch)
}

func (j *selectionJournal) currentEpochLocked(ctx context.Context, epoch uint64) error {
	if j.frozen || epoch != j.epoch {
		return errSelectionFenced
	}
	current, revision, err := j.store.Load(ctx)
	if err != nil {
		j.frozen = true
		return err
	}
	if revision != j.revision || current.GetSelectionLedger().GetDispatchEpoch() != epoch {
		j.frozen = true
		return errSelectionFenced
	}
	return nil
}
func (j *selectionJournal) currentEpoch(ctx context.Context, epoch uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.currentEpochLocked(ctx, epoch)
}

func (j *selectionJournal) checkpoint(ctx context.Context, provider, session, digest string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.currentEpochLocked(ctx, j.epoch); err != nil {
		return err
	}
	if err := j.writable(j.epoch); err != nil {
		return err
	}
	if (provider != "codex" && provider != "claude") || !selectionToken.MatchString(session) || !selectionToken.MatchString(digest) {
		return errors.New("invalid provider checkpoint identity")
	}
	existing := j.state.SelectionLedger
	if existing.ProviderSessionId != "" && (existing.Provider != provider || existing.ProviderSessionId != session) {
		return errors.New("provider session cannot change within an incident")
	}
	next := j.next()
	s := next.SelectionLedger
	s.Provider, s.ProviderSessionId, s.CheckpointDigest = provider, session, digest
	return j.save(ctx, next)
}

// Hash only validated semantics. Sorting bindings and excluding the transport
// epoch makes a retry stable when a replacement owner adopts the same session.
func selectionPayloadHash(selection *contracts.MitigationSelection) (string, error) {
	if selection == nil {
		return "", errors.New("selection required")
	}
	if err := wire.Validate(selection); err != nil {
		return "", errors.New("invalid selection contract")
	}
	canonical := proto.Clone(selection).(*contracts.MitigationSelection)
	canonical.DispatchEpoch = 1 // Fixed sentinel; actual transport epoch is excluded.
	sort.Slice(canonical.Bindings, func(i, k int) bool { return canonical.Bindings[i].Name < canonical.Bindings[k].Name })
	for i, b := range canonical.Bindings {
		if b == nil || i > 0 && b.Name == canonical.Bindings[i-1].Name {
			return "", errors.New("duplicate selection binding")
		}
	}
	payload, err := wire.Canonical(canonical)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}
func sameSelectionBindings(a, b []*contracts.Binding) bool {
	if len(a) != len(b) {
		return false
	}
	byName := make(map[string]*contracts.ObjectRef, len(a))
	for _, binding := range a {
		if binding == nil || byName[binding.Name] != nil {
			return false
		}
		byName[binding.Name] = binding.Object
	}
	for _, binding := range b {
		if binding == nil || !proto.Equal(byName[binding.Name], binding.Object) {
			return false
		}
		delete(byName, binding.Name)
	}
	return len(byName) == 0
}
func (j *selectionJournal) accept(ctx context.Context, menu *contracts.MitigationMenu, selection *contracts.MitigationSelection) (*contracts.SelectionAttempt, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	hash, err := selectionPayloadHash(selection)
	if err != nil {
		return nil, false, err
	}
	if err := j.currentEpochLocked(ctx, selection.DispatchEpoch); err != nil {
		return nil, false, err
	}
	s := j.state.SelectionLedger
	if selection.IncidentId != s.IncidentId || selection.SessionId != s.SessionId || !selectionToken.MatchString(selection.RequestId) {
		return nil, false, errors.New("selection incident/session/request identity mismatch")
	}
	// Look up semantic retry before testing current menu/source. Source adoption
	// invalidates new selections, never an already accepted immutable report.
	for _, a := range s.Attempts {
		if a.RequestId == selection.RequestId {
			if a.PayloadHash != hash {
				return nil, false, errors.New("selection request ID reused with different semantics")
			}
			return proto.Clone(a).(*contracts.SelectionAttempt), true, nil
		}
	}
	if err := j.writable(selection.DispatchEpoch); err != nil {
		return nil, false, err
	}
	if s.ProviderSessionId == "" || s.CheckpointDigest == "" {
		return nil, false, errors.New("provider checkpoint required before selection acceptance")
	}
	if menu == nil || wire.Validate(menu) != nil {
		return nil, false, errors.New("invalid menu")
	}
	source := s.SourceHistory[len(s.SourceHistory)-1]
	if menu.IncidentId != s.IncidentId || menu.SessionId != s.SessionId || menu.DispatchEpoch != s.DispatchEpoch || menu.SourceCommit != source || !proto.Equal(menu.OriginalDeadline, s.OriginalDeadline) || selection.MenuId != menu.MenuId || selection.SourceCommit != source || selection.ScopeDigest != menu.ScopeDigest || selection.PolicyDigest != menu.PolicyDigest || selection.RoundLimit > menu.RoundLimit {
		return nil, false, errors.New("selection menu/revision/scope/policy fence mismatch")
	}
	var option *contracts.MitigationOption
	for _, candidate := range menu.Options {
		if candidate.OptionId == selection.OptionId {
			option = candidate
			break
		}
	}
	if option == nil || option.DisabledReason != "" || len(option.Effects) == 0 || option.ProgramRevision != selection.ProgramRevision || option.DependencyRevision != selection.DependencyRevision {
		return nil, false, errors.New("selection option disabled or revision mismatch")
	}
	bindings := option.Bindings
	if len(bindings) == 0 {
		bindings = menu.Bindings
	}
	if !sameSelectionBindings(selection.Bindings, bindings) {
		return nil, false, errors.New("selection bindings mismatch")
	}
	if len(s.Attempts) >= 32 || selection.RoundLimit > s.TotalBatches-s.UsedBatches-s.ReservedBatches {
		return nil, false, errors.New("selection incident budget exhausted")
	}
	// Reserve cleanup/verification inside the original deadline. A round receives
	// at most 60s and settlement at most 30s; this is never a deadline extension.
	if j.now().Add(time.Duration(selection.RoundLimit)*60*time.Second + 60*time.Second).After(s.OriginalDeadline.AsTime()) {
		return nil, false, errors.New("selection has insufficient original deadline remaining")
	}
	next := j.next()
	n := next.SelectionLedger
	a := &contracts.SelectionAttempt{RequestId: selection.RequestId, PayloadHash: hash, AttemptId: "attempt-" + rand.Text(), AcceptanceEpoch: j.epoch, ReservedBatches: selection.RoundLimit}
	n.Attempts = append(n.Attempts, a)
	n.ReservedBatches += selection.RoundLimit
	n.OwnerQuiescent = false
	if err := j.save(ctx, next); err != nil {
		return nil, false, err
	}
	return proto.Clone(a).(*contracts.SelectionAttempt), false, nil
}
func selectionAttempt(s *contracts.SelectionLedger, id string) (*contracts.SelectionAttempt, error) {
	for _, a := range s.Attempts {
		if a.AttemptId == id {
			return a, nil
		}
	}
	return nil, errors.New("unknown selection attempt")
}
func (j *selectionJournal) beginAction(ctx context.Context, attempt string, round uint32, phase string, receipt *contracts.MitigationActionReceipt) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.writable(j.epoch); err != nil {
		return err
	}
	next := j.next()
	a, err := selectionAttempt(next.SelectionLedger, attempt)
	if err != nil {
		return err
	}
	if a.Report != nil || round == 0 || round > a.ReservedBatches || receipt == nil || receipt.State != contracts.MitigationActionReceipt_STATE_UNKNOWN || (phase != "forward" && phase != "compensation") {
		return errors.New("invalid selection action intent")
	}
	for _, action := range a.Actions {
		if !action.Complete || action.Receipt.ActionId == receipt.ActionId && action.Phase == phase {
			return errors.New("outstanding or duplicate action intent")
		}
	}
	a.Actions = append(a.Actions, &contracts.SelectionAction{RoundIndex: round, Phase: phase, Receipt: proto.Clone(receipt).(*contracts.MitigationActionReceipt)})
	return j.save(ctx, next)
}
func (j *selectionJournal) completeAction(ctx context.Context, attempt, id string, compensation bool, uid, rv string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.writable(j.epoch); err != nil {
		return err
	}
	next := j.next()
	a, err := selectionAttempt(next.SelectionLedger, attempt)
	if err != nil {
		return err
	}
	phase := "forward"
	if compensation {
		phase = "compensation"
	}
	for _, action := range a.Actions {
		if action.Receipt.ActionId == id && action.Phase == phase {
			if a.Report != nil || action.Complete {
				return errors.New("action completion already recorded")
			}
			action.Complete = true
			action.Receipt.State = contracts.MitigationActionReceipt_STATE_APPLIED
			action.Receipt.Uid, action.Receipt.ResourceVersion = uid, rv
			return j.save(ctx, next)
		}
	}
	return errors.New("action completion requires durable intent")
}
func (j *selectionJournal) settle(ctx context.Context, id string, report *contracts.MitigationReport, used uint32) (*contracts.MitigationReport, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	// Recovery may settle frozen/expired attempts, but never under a stale epoch.
	if j.frozen {
		return nil, errSelectionFenced
	}
	if err := j.currentEpochLocked(ctx, j.epoch); err != nil {
		return nil, err
	}
	next := j.next()
	s := next.SelectionLedger
	a, err := selectionAttempt(s, id)
	if err != nil {
		return nil, err
	}
	if report == nil || report.IncidentId != s.IncidentId || report.SessionId != s.SessionId || report.RequestId != a.RequestId || report.AttemptId != a.AttemptId || report.AcceptanceEpoch != a.AcceptanceEpoch || used > a.ReservedBatches {
		return nil, errors.New("invalid selection settlement identity/budget")
	}
	candidate := proto.Clone(report).(*contracts.MitigationReport)
	if a.Report != nil {
		candidate.RemainingBatches = a.Report.RemainingBatches
		if used != a.ChargedBatches || !proto.Equal(candidate, a.Report) {
			return nil, errors.New("terminal selection report is immutable")
		}
		return proto.Clone(a.Report).(*contracts.MitigationReport), nil
	}
	if s.DispatchEpoch != j.epoch {
		return nil, errSelectionFenced
	}
	// An uncertain request spends the full reservation. Recovery cannot release
	// budget on the assumption that a lost API reply meant no effect.
	for _, action := range a.Actions {
		if !action.Complete {
			if candidate.Status == contracts.MitigationReport_STATUS_SUCCESS {
				return nil, errors.New("uncertain action cannot authorize success")
			}
			used = a.ReservedBatches
			s.WritesFrozen = true
			candidate.WritesFrozen = true
			if action.Receipt != nil && action.Receipt.Resource != nil {
				found := false
				for _, ref := range candidate.Residuals {
					found = found || proto.Equal(ref, action.Receipt.Resource)
				}
				if !found {
					candidate.Residuals = append(candidate.Residuals, proto.Clone(action.Receipt.Resource).(*contracts.ObjectRef))
				}
			}
		}
	}
	if s.WritesFrozen && !candidate.WritesFrozen {
		return nil, errors.New("frozen settlement must retain write prohibition")
	}
	if (s.WritesFrozen || !j.now().Before(s.OriginalDeadline.AsTime())) && candidate.Status == contracts.MitigationReport_STATUS_SUCCESS {
		return nil, errors.New("frozen/expired selection cannot succeed")
	}
	a.ChargedBatches = used
	s.UsedBatches += used
	s.ReservedBatches -= a.ReservedBatches
	candidate.RemainingBatches = s.TotalBatches - s.UsedBatches - s.ReservedBatches
	a.Report = candidate
	if err := j.save(ctx, next); err != nil {
		return nil, err
	}
	return proto.Clone(candidate).(*contracts.MitigationReport), nil
}

// adopt is called only after the mutation owner has revoked/drained the old
// epoch. provenQuiescent is trusted driver evidence, never a worker/tool claim.
// When proof is absent it durably freezes *all* mutation paths, including act.
func (j *selectionJournal) adopt(ctx context.Context, source string, provenQuiescent bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.frozen {
		return errSelectionFenced
	}
	if source == "" || j.epoch == math.MaxUint64 {
		return errors.New("invalid selection adoption")
	}
	next := j.next()
	s := next.SelectionLedger
	s.DispatchEpoch++
	if len(s.SourceHistory) == 0 || s.SourceHistory[len(s.SourceHistory)-1] != source {
		s.SourceHistory = append(s.SourceHistory, source)
	}
	s.OwnerQuiescent = provenQuiescent
	s.WritesFrozen = s.WritesFrozen || !provenQuiescent
	if err := j.save(ctx, next); err != nil {
		return err
	}
	j.epoch = s.DispatchEpoch
	return nil
}
func (j *selectionJournal) chargeLegacy(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.writable(j.epoch); err != nil {
		return err
	}
	next := j.next()
	s := next.SelectionLedger
	if s.UsedBatches+s.ReservedBatches >= s.TotalBatches {
		return errors.New("response loop budget exhausted")
	}
	s.UsedBatches++
	return j.save(ctx, next)
}

// Reports awaiting delivery remain an outbox until the resumed provider session
// acknowledges them. Reading the outbox never acknowledges, settles or executes.
func (j *selectionJournal) pendingReports() []*contracts.MitigationReport {
	j.mu.Lock()
	defer j.mu.Unlock()
	reports := make([]*contracts.MitigationReport, 0)
	for _, a := range j.state.SelectionLedger.Attempts {
		if a.Report != nil && !a.DeliveryAcknowledged {
			reports = append(reports, proto.Clone(a.Report).(*contracts.MitigationReport))
		}
	}
	return reports
}
func (j *selectionJournal) acknowledge(ctx context.Context, id, providerSession string, closure bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.currentEpochLocked(ctx, j.epoch); err != nil {
		return err
	}
	if j.frozen {
		return errSelectionFenced
	}
	next := j.next()
	s := next.SelectionLedger
	if s.DispatchEpoch != j.epoch || s.ProviderSessionId != providerSession {
		return errSelectionFenced
	}
	a, err := selectionAttempt(s, id)
	if err != nil {
		return err
	}
	if a.Report == nil {
		return errors.New("cannot acknowledge unsettled selection")
	}
	if closure {
		if !a.ClosureStarted {
			return errors.New("closure acknowledgment requires handoff")
		}
		a.ClosureAcknowledged = true
	} else {
		a.DeliveryAcknowledged = true
	}
	return j.save(ctx, next)
}
