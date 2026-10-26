package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	outcomeclass "sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/shellpath"
)

// DerivedOutcome is produced from controller facts, not from a worker claim.
type DerivedOutcome struct {
	Record             *contracts.OutcomeRecord
	RequiresReflection bool
}

// OutcomeDeriver must independently check health and diagnosis evidence before
// issuing a success document. The broker will reject an absent deriver.
type OutcomeDeriver interface {
	Derive(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error)
}

type OutcomeDeriverFunc func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error)

func (f OutcomeDeriverFunc) Derive(ctx context.Context, closure IncidentClosure, repairCommit, backend, model string) (DerivedOutcome, error) {
	return f(ctx, closure, repairCommit, backend, model)
}

// ReflectionWorker launches a registered agent with a fresh proposal checkout
// based on the committed outcome. Its reported paths are never authoritative.
type ReflectionWorker interface {
	Reflect(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error)
}

// ReflectionFeedback is Go's retry state for one reflection dispatch. Attempt
// is 1-based; after a broker rejection Diagnostics are that rejection's typed
// validator findings.
type ReflectionFeedback struct {
	DispatchAt     time.Time
	Attempt        uint32
	Diagnostics    []*contracts.MemoryDiagnostic
	CauseWork      []*contracts.ReflectionCauseWork
	UnassignedWork []*contracts.ReflectionWorkItem
}

type ReflectionDecision struct {
	LearningDecision string
	NoChangeReason   string
	DeclineReason    string
	PlaybookIDs      []string
	Error            string
	FailureReason    string
	SDKGaps          []*contracts.SdkGap
	Usage            *UsageMetrics
}

type ReflectionWorkerFunc func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error)

func (f ReflectionWorkerFunc) Reflect(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, proposalCommit, outcomeCommit string, feedback ReflectionFeedback) (ReflectionDecision, error) {
	return f(ctx, closure, workspace, outcome, proposalCommit, outcomeCommit, feedback)
}

// NativeIncidentBroker is the restartable Go closure coordinator. It is only
// safe to select when a full OutcomeDeriver and ReflectionWorker are configured;
// no partial closure can be acknowledged.
type NativeIncidentBroker struct {
	CodePath    CodePathConfig
	ShellReader shellpath.Reader

	Now                  func() time.Time
	Repository           *broker.Broker
	LedgerRoot           string
	Deriver              OutcomeDeriver
	Reflector            ReflectionWorker
	ResponderBackend     string
	ResponderModel       string
	SessionRoot          string
	SessionRetention     time.Duration
	PhaseLog             io.Writer
	ForceReflection      bool
	Programs             program.ProgramRegistry
	LateFindingsVerifier interface {
		VerifyPull(context.Context, string, uint32, string, []string, []string) error
	}
}

type ControllerRolloutExpectation struct {
	IncidentID                string
	ReflectionCommit          string
	BeforeDetectorFingerprint string
	AfterDetectorFingerprint  string
}

// ControllerRolloutReceipt comes from trusted installation code after build
// and observation of the restarted controller's detector revision.
type ControllerRolloutReceipt struct {
	Expectation                 ControllerRolloutExpectation
	BuildArtifact               string
	ControllerRevision          string
	ObservedDetectorFingerprint string
	Success                     bool
	VerifiedAt                  time.Time
}

// ControllerRolloutHandoff is durable evidence for the supervisor to restart
// from an accepted detector revision before acknowledging a closure.
type ControllerRolloutHandoff struct {
	Expectation ControllerRolloutExpectation
	Receipt     ClosureReceipt
}

type closurePhaseContextKey struct{}

func recordClosurePhase(ctx context.Context, phase string) error {
	observe, _ := ctx.Value(closurePhaseContextKey{}).(func(string) error)
	if observe != nil {
		return observe(phase)
	}
	return nil
}

func (n NativeIncidentBroker) recordPhase(ledger *contracts.BrokerLedger, phase string) error {
	if ledger.ClosurePhaseTimestamps == nil {
		ledger.ClosurePhaseTimestamps = make(map[string]*timestamppb.Timestamp)
	}
	if _, recorded := ledger.ClosurePhaseTimestamps[phase]; recorded {
		return nil
	}
	at := n.clock()
	ledger.ClosurePhaseTimestamps[phase] = contractTimestamp(at)
	if err := n.saveLedger(ledger); err != nil {
		return err
	}
	if n.PhaseLog != nil {
		_ = json.NewEncoder(n.PhaseLog).Encode(map[string]any{"incident_id": ledger.IncidentId, "closure_phase": phase, "at": at})
	}
	return nil
}

func (n NativeIncidentBroker) PrepareIncident(ctx context.Context, incidentID string) (IncidentWorkspace, error) {
	if n.Repository == nil {
		return IncidentWorkspace{}, errors.New("Go repository broker is not configured")
	}
	proposal, err := n.Repository.CreateProposal(ctx, incidentID)
	if err != nil {
		return IncidentWorkspace{}, err
	}
	return IncidentWorkspace{IncidentID: incidentID, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}, nil
}

func (n NativeIncidentBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	if n.Repository == nil || n.Deriver == nil || n.ResponderBackend == "" || n.ResponderModel == "" {
		return ClosureReceipt{}, errors.New("native broker requires repository, outcome deriver, and responder identity")
	}
	if err := validateIncidentRequest(closure.Request); err != nil {
		return ClosureReceipt{}, fmt.Errorf("invalid closure request: %w", err)
	}
	if closure.Result != nil {
		if err := validateIncidentResultFor(*closure.Result, closure.Request); err != nil {
			return ClosureReceipt{}, fmt.Errorf("invalid responder result: %w", err)
		}
		if pull := closure.Result.LateFindingsPull; pull != nil {
			if n.LateFindingsVerifier == nil {
				return ClosureReceipt{}, errors.New("late findings pull verifier is required")
			}
			if err := n.LateFindingsVerifier.VerifyPull(ctx, closure.Request.IncidentID, pull.PullCount, pull.ReportDigest, pull.DetectorIDs, pull.PlaybookIDs); err != nil {
				return ClosureReceipt{}, fmt.Errorf("verify controller-authored late findings pull: %w", err)
			}
		}
	}
	var receipt ClosureReceipt
	err := n.withLock(func() error {
		id := closure.Request.IncidentID
		ledger, err := n.readLedger(id)
		if err != nil {
			return err
		}
		if err := n.recordPhase(ledger, "closure_start"); err != nil {
			return err
		}
		ctx = context.WithValue(ctx, closurePhaseContextKey{}, func(phase string) error { return n.recordPhase(ledger, phase) })
		projected, err := incidentClosureContract(closure)
		if err != nil {
			return fmt.Errorf("project closure: %w", err)
		}
		if ledger.Closure != nil && !proto.Equal(ledger.Closure, projected) {
			return errors.New("closure differs from persisted broker evidence")
		}
		prepared, err := n.Repository.ProposalInfo(id, broker.ProposalPhase)
		if err != nil && ledger.Receipt == nil {
			return fmt.Errorf("prepared proposal is missing: %w", err)
		}
		if err == nil && (closure.Request.RepositoryWorktree != prepared.Path || closure.Request.RepositoryBaseCommit != prepared.BaseCommit) {
			return errors.New("closure workspace does not match broker-prepared proposal")
		}
		if ledger.Closure == nil {
			ledger.Closure = projected
			if closure.Result != nil {
			}
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}
		if ledger.Receipt != nil {
			receipt = *closureReceiptState(ledger.Receipt)
			return nil
		}

		if ledger.ProposalCommit == "" {
			if accepted, ok, err := n.Repository.AcceptedCommit(ctx, id, broker.ProposalPhase); err != nil {
				return err
			} else if ok {
				ledger.ProposalCommit = accepted.Commit
			}
			if ledger.ProposalCommit == "" {
				changed, err := n.Repository.ChangedPaths(ctx, id, broker.ProposalPhase)
				if err != nil {
					return err
				}
				// An unverified (timed-out) closure never publishes its repair.
				verified := len(closure.FinalDetectorStates) > 0
				if len(broker.SourceRepairPaths(changed)) > 0 && verified {
					accepted, err := n.Repository.Publish(ctx, id, broker.Responder, broker.ProposalPhase)
					if err != nil {
						return err
					}
					ledger.ProposalCommit = accepted.Commit
				} else if verified && closure.Request.RepairPolicy == "commit" {
					return errors.New("commit repair policy requires a nonempty validated proposal")
				}
			}
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}

		if ledger.OutcomeDocument == nil {
			derived, err := n.Deriver.Derive(ctx, closure, ledger.ProposalCommit, n.ResponderBackend, n.ResponderModel)
			if err != nil {
				return fmt.Errorf("derive independent outcome: %w", err)
			}
			if derived.Record == nil || derived.Record.GetIncidentId() != id {
				return errors.New("outcome deriver returned an invalid typed record")
			}
			ledger.OutcomeDocument = proto.Clone(derived.Record).(*contracts.OutcomeRecord)
			ledger.OutcomeDocument.CodePath = codePathContract(n.CodePath)
			ledger.RequiresReflection = derived.RequiresReflection
			if n.SessionRoot != "" {
				capture, captureErr := memory.IncidentCorpusDir(n.SessionRoot, id)
				if captureErr == nil {
					validate.ObservedRepairOwners(ledger.OutcomeDocument, capture)
					validate.DropReplacedOwnedPods(ledger.OutcomeDocument, capture)
					ledger.OutcomeDocument.UndetectedCauses = validate.CaptureUndetectedCauses(ledger.OutcomeDocument, capture)
				}
			}
			// Every verified closure settles current evidence before committing its
			// authoritative outcome. Prior repair text is never an eligibility proof.
			if derived.Record.GetClassification() == "success" {
				ledger.OutcomeDocument.ReflectionEligibility = n.prepareCauseWork(ctx, ledger.OutcomeDocument, IncidentWorkspace{IncidentID: id, Worktree: closure.Request.RepositoryWorktree, BaseCommit: closure.Request.RepositoryBaseCommit})
				ledger.RequiresReflection = true
			} else {
				indices := partialRepairCauses(ledger.OutcomeDocument)
				ledger.RequiresReflection = len(indices) > 0 || hasResponderSession(closure) || stageOnlyClosure(closure)
				if ledger.RequiresReflection {
					eligibility := n.prepareCauseWork(ctx, ledger.OutcomeDocument, IncidentWorkspace{IncidentID: id, Worktree: closure.Request.RepositoryWorktree, BaseCommit: closure.Request.RepositoryBaseCommit})
					ledger.OutcomeDocument.ReflectionEligibility = partialReflectionEligibility(eligibility, indices, false)
					ledger.OutcomeDocument.ReflectionEligibility.Unassigned = unverifiedCauseWork(ledger.OutcomeDocument, indices)
				} else {
					ledger.ReflectionDecision = &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "no_session"}
				}
			}
			if !hasResponderSession(closure) && !stageOnlyClosure(closure) {
				ledger.OutcomeDocument.ReflectionSkipReason = "no_session"
			}
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}
		if ledger.OutcomeCommit == "" {
			accepted, err := n.Repository.RecordOutcome(ctx, id, proto.Clone(ledger.OutcomeDocument).(*contracts.OutcomeRecord))
			if err != nil {
				return err
			}
			ledger.OutcomeCommit = accepted.Commit
			if err := n.recordPhase(ledger, "outcome_write"); err != nil {
				return err
			}
			if err := n.recordPhase(ledger, "broker_commit"); err != nil {
				return err
			}
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}

		// The persisted decision prevents a restart from changing learning policy.
		// A failed attempt's reflection is queued behind the acknowledgment so
		// the incident's follow-up starts from the outcome commit at once.
		if ledger.RequiresReflection && !ledger.ReflectionProcessed && ledger.OutcomeDocument.GetClassification() != outcomeclass.Success {
			if !ledger.ReflectionQueued {
				ledger.ReflectionQueued = true
				ledger.ReflectionQueuedAt = contractTimestamp(n.clock())
				if err := n.recordPhase(ledger, "reflection_queued"); err != nil {
					return err
				}
			}
		} else if ledger.RequiresReflection && !ledger.ReflectionProcessed {
			if err := n.reflectLocked(ctx, ledger); err != nil {
				return err
			}
			// Persist this marker together with its receipt below. A checkpoint
			// declaring processing complete without a receipt cannot authorize ACK.
		}
		if ledger.OutcomeCommit == "" {
			return errors.New("closure has no authoritative outcome commit")
		}
		receipt = ClosureReceipt{
			IncidentID: id, Worktree: closure.Request.RepositoryWorktree,
			BaseCommit: closure.Request.RepositoryBaseCommit, ProposalCommit: ledger.ProposalCommit,
			OutcomeCommit: ledger.OutcomeCommit, ReflectionCommit: ledger.ReflectionCommit,
		}
		digest := sha256.Sum256([]byte(strings.Join([]string{id, receipt.ProposalCommit, receipt.OutcomeCommit, receipt.ReflectionCommit}, "\x00")))
		receipt.AckToken = fmt.Sprintf("%x", digest)
		ledger.Receipt = closureReceiptContract(&receipt)
		return n.saveLedger(ledger)
	})
	return receipt, err
}

// reflectLocked runs reflection and records its typed SDK gaps and rejections
// through the broker. Reflection follows the original outcome commit; these
// appends never change incident evidence or the commit that identifies the
// reflection task across restarts. Callers hold the broker lock.
func (n NativeIncidentBroker) reflectLocked(ctx context.Context, ledger *contracts.BrokerLedger) error {
	id := ledger.IncidentId
	reflectionPaths, err := n.processReflection(ctx, ledger)
	if err != nil {
		return err
	}
	if err := n.setRolloutExpectation(ctx, ledger, reflectionPaths); err != nil {
		return err
	}
	ledger.ReflectionProcessed = true
	var gaps []*contracts.SdkGap
	for _, decision := range ledger.GetReflectionResults() {
		gaps = append(gaps, decision.GetSdkGaps()...)
	}
	gaps = append(gaps, ledger.GetReflectionDecision().GetSdkGaps()...)
	if len(gaps) > 0 {
		if _, err := n.Repository.RecordSDKGaps(ctx, id, gaps); err != nil {
			return fmt.Errorf("record reflection SDK gaps: %w", err)
		}
	}
	if len(ledger.ReflectionRejections) > 0 {
		if _, err := n.Repository.RecordReflectionRejections(ctx, id, ledger.ReflectionRejections); err != nil {
			return fmt.Errorf("record reflection rejections: %w", err)
		}
	}
	return nil
}

// QueuedReflections lists acknowledged incidents whose queued reflection has
// not finished or whose accepted detector change awaits its controller
// rollout, oldest first. Ledger writes are atomic renames, so this reads
// without the broker lock that a running reflection holds.
func (n NativeIncidentBroker) QueuedReflections() ([]string, error) {
	entries, err := os.ReadDir(n.LedgerRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var queued []*contracts.BrokerLedger
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(n.LedgerRoot, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		ledger, err := parseLedger(path, data)
		if err != nil {
			return nil, fmt.Errorf("broker ledger %s: %w", entry.Name(), err)
		}
		if ledger.ReflectionQueued && ledger.Acknowledged && (!ledger.ReflectionProcessed || reflectionRolloutPending(ledger)) {
			queued = append(queued, ledger)
		}
	}
	slices.SortFunc(queued, func(a, b *contracts.BrokerLedger) int {
		if c := a.GetReflectionQueuedAt().AsTime().Compare(b.GetReflectionQueuedAt().AsTime()); c != 0 {
			return c
		}
		return strings.Compare(a.IncidentId, b.IncidentId)
	})
	ids := make([]string, len(queued))
	for i, ledger := range queued {
		ids[i] = ledger.IncidentId
	}
	return ids, nil
}

// ProcessQueuedReflection runs one acknowledged incident's queued reflection
// under the broker lock, so memory publication stays serialized with every
// closure. It is idempotent: a finished or unqueued ledger is a no-op.
func (n NativeIncidentBroker) ProcessQueuedReflection(ctx context.Context, incidentID string) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(incidentID)
		if err != nil {
			return err
		}
		if !ledger.ReflectionQueued || ledger.ReflectionProcessed || !ledger.Acknowledged {
			return nil
		}
		if err := n.reflectLocked(ctx, ledger); err != nil {
			return err
		}
		if err := n.Repository.ReleaseProposal(incidentID, broker.ReflectionPhase); err != nil {
			return err
		}
		if err := n.recordPhase(ledger, "reflection_committed"); err != nil {
			return err
		}
		return n.saveLedger(ledger)
	})
}

// QueuedReflectionRolloutPending reports whether an incident's queued
// reflection accepted a detector change that no restarted controller has
// recorded yet. Like QueuedReflections, it reads without the broker lock.
func (n NativeIncidentBroker) QueuedReflectionRolloutPending(incidentID string) (bool, error) {
	ledger, err := n.readLedger(incidentID)
	if err != nil {
		return false, err
	}
	return ledger.ReflectionQueued && reflectionRolloutPending(ledger), nil
}

func reflectionRolloutPending(ledger *contracts.BrokerLedger) bool {
	return ledger.RolloutExpectation != nil && !successfulRollout(rolloutReceiptsState(ledger.RolloutReceipts))
}

// AbandonQueuedReflection ends a queued reflection whose retries are spent.
// The outcome commit stands; the ledger records why no reflection followed.
func (n NativeIncidentBroker) AbandonQueuedReflection(_ context.Context, incidentID, reason string) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(incidentID)
		if err != nil {
			return err
		}
		if !ledger.ReflectionQueued || ledger.ReflectionProcessed {
			return nil
		}
		ledger.ReflectionProcessed = true
		ledger.ReflectionSkippedReason = "queued_reflection_abandoned: " + reason
		if err := n.Repository.ReleaseProposal(incidentID, broker.ReflectionPhase); err != nil {
			return err
		}
		if err := n.recordPhase(ledger, "reflection_abandoned"); err != nil {
			return err
		}
		return n.saveLedger(ledger)
	})
}

// processReflection is a driver: durable pure transitions choose every effect,
// and the private adapter performs repository/worker/validator I/O.
func (n NativeIncidentBroker) processReflection(ctx context.Context, ledger *contracts.BrokerLedger) ([]string, error) {
	update := func(event lifecyclecore.ReflectionEvent) error {
		next, _ := lifecyclecore.StepReflection(ledger, event)
		if proto.Equal(next, ledger) {
			return nil
		}
		proto.Reset(ledger)
		proto.Merge(ledger, next)
		return n.saveLedger(ledger)
	}
	if err := update(lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionBegin, MaxRetries: broker.MaxValidationRetries}); err != nil {
		return nil, err
	}
	driver := lifecycleio.ReflectionDriver{Repository: n.Repository, SessionRoot: n.SessionRoot, Now: n.clock, Phase: func(phase string) error { return n.recordPhase(ledger, phase) }, Reflect: func(ctx context.Context, effect lifecyclecore.ReflectionEffect) (*contracts.ReflectionDecision, error) {
		workspace := IncidentWorkspace{IncidentID: effect.IncidentID, Worktree: effect.Workspace.GetCheckoutPath(), BaseCommit: effect.Workspace.GetBaseCommit()}
		selected, err := NewCodePathRegistry().Resolve(n.CodePath)
		if err != nil {
			return nil, err
		}
		if selected.Selection.MemoryValidation != "go" {
			if n.Reflector == nil {
				return nil, errors.New("shell learning requires a registered reflection worker")
			}
			decision, err := n.Reflector.Reflect(ctx, incidentClosureState(effect.Closure), workspace, effect.Outcome, effect.ProposalCommit, effect.OutcomeCommit, ReflectionFeedback{Attempt: effect.Attempt, Diagnostics: effect.Diagnostics, DispatchAt: controllerTime(effect.DispatchAt)})
			if err != nil {
				return nil, err
			}
			return logicalReflectionDecision(decision), nil
		}

		if effect.Outcome.GetClassification() != outcomeclass.Success {
			effect.Outcome = partialReflectionOutcome(effect.Outcome)
			trimPartialReflectionClosure(effect.Closure, effect.Outcome)
		}
		eligibility := &contracts.ReflectionEligibility{}
		if effect.Outcome.GetReflectionEligibility() != nil {
			proto.Merge(eligibility, effect.Outcome.GetReflectionEligibility())
		}
		work, unassigned := eligibility.GetCauses(), eligibility.GetUnassigned()
		positive, negative := settledProgramCases(eligibility)
		origins := settledCaptureOrigins(eligibility)
		capture := ""
		if n.SessionRoot != "" {
			capture, _ = memory.IncidentCorpusDir(n.SessionRoot, effect.IncidentID)
		}
		prepared := false
		negativePrepared := false
		if len(positive) > 0 {
			if capture != "" {
				paths, err := memory.RecordRepeatCorpus(workspace.Worktree, capture, effect.IncidentID, positive, origins)
				prepared = err == nil && len(paths) > 0
			}
			if !prepared {
				for _, cause := range work {
					for _, item := range cause.Items {
						if item.Disposition == "covered" {
							item.Disposition, item.Reason, item.Question = "question", "uncertain", "The exact repair is covered, but captured peer evidence could not be recorded. Review the case preparation failure."
						}
					}
				}
			}
		}
		if capture != "" && len(negative) > 0 {
			if _, err := memory.RecordFixedNoMatchCorpus(workspace.Worktree, capture, effect.IncidentID, negative, origins); err != nil {
				return nil, err
			}
			negativePrepared = true
		}
		if effect.Attempt == 1 && !n.ForceReflection && !hasResponderSession(incidentClosureState(effect.Closure)) && !stageOnlyClosure(incidentClosureState(effect.Closure)) && (prepared || negativePrepared) && !causeWorkNeedsModel(work) && !reflectionItemsNeedModel(unassigned) {
			return &contracts.ReflectionDecision{LearningDecision: "updated"}, nil
		}
		if n.Reflector == nil {
			return nil, errors.New("unsettled learning requires a registered reflection worker")
		}
		decision, err := n.Reflector.Reflect(ctx, incidentClosureState(effect.Closure), workspace, effect.Outcome, effect.ProposalCommit, effect.OutcomeCommit, ReflectionFeedback{Attempt: effect.Attempt, Diagnostics: effect.Diagnostics, DispatchAt: controllerTime(effect.DispatchAt), CauseWork: work, UnassignedWork: unassigned})
		if err != nil {
			return nil, fmt.Errorf("reflection worker: %w", err)
		}
		// A failed/no-op closure with no independently repaired cause may
		// explain its evidence gap, but cannot publish README-only learning or
		// turn the responder's unverified repair claims into executable memory.
		if effect.Outcome.GetClassification() != outcomeclass.Success && len(validate.VerifiedReflectionCauses(effect.Outcome)) == 0 && decision.LearningDecision == "updated" {
			decision.LearningDecision = "failed"
			decision.Error = "reflection without a verified repaired cause or controller-observed undetected transition requires no_change with an evidence-gap reason or a typed SDK gap"
		}
		if prepared {
			if _, err := memory.RecordRepeatCorpus(workspace.Worktree, capture, effect.IncidentID, positive, origins); err != nil {
				return nil, err
			}
		}
		// Keep Go-owned negative evidence even if the agent deleted or relabeled it.
		if capture != "" && len(negative) > 0 {
			if _, err := memory.RecordFixedNoMatchCorpus(workspace.Worktree, capture, effect.IncidentID, negative, origins); err != nil {
				return nil, err
			}
		}
		return logicalReflectionDecision(decision), nil
	}}
	for {
		_, effects := lifecyclecore.StepReflection(ledger, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionDrive})
		if len(effects) == 0 {
			return append([]string(nil), ledger.GetReflectionPaths()...), nil
		}
		event, err := driver.Execute(ctx, effects[0])
		if err != nil {
			return nil, err
		}
		if err := update(event); err != nil {
			return nil, err
		}
	}
}

func (n NativeIncidentBroker) clock() time.Time {
	if n.Now != nil {
		return n.Now().UTC()
	}
	return time.Now().UTC()
}

func (n NativeIncidentBroker) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(receipt.IncidentID)
		if err != nil {
			return err
		}
		if ledger.Receipt == nil || *closureReceiptState(ledger.Receipt) != receipt {
			return errors.New("closure receipt does not match durable broker state")
		}
		if ledger.RolloutExpectation != nil && !successfulRollout(rolloutReceiptsState(ledger.RolloutReceipts)) {
			return errors.New("controller rollout has not been independently verified")
		}
		if err := n.Repository.ReleaseProposal(receipt.IncidentID, broker.ProposalPhase); err != nil {
			return err
		}
		if !ledger.ReflectionQueued || ledger.ReflectionProcessed {
			if err := n.Repository.ReleaseProposal(receipt.IncidentID, broker.ReflectionPhase); err != nil {
				return err
			}
		}
		if err := n.recordPhase(ledger, "closure_ack"); err != nil {
			return err
		}
		ledger.Acknowledged = true
		if ledger.AcknowledgedAt == nil {
			ledger.AcknowledgedAt = contractTimestamp(n.clock())
		}
		if err := n.saveLedger(ledger); err != nil {
			return err
		}
		return nil
	})
}

const DefaultSessionRetention = 7 * 24 * time.Hour

// PruneIncidentSessions removes only acknowledged sessions whose durable
// acknowledgment is older than the configured retention window. Open incidents
// retain their private session data for restart and reflection recovery.
func (n NativeIncidentBroker) PruneIncidentSessions(now time.Time) error {
	if n.SessionRoot == "" {
		return nil
	}
	retention := n.SessionRetention
	if retention <= 0 {
		retention = DefaultSessionRetention
	}
	return n.withLock(func() error {
		entries, err := os.ReadDir(n.LedgerRoot)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") || !entry.Type().IsRegular() {
				continue
			}
			path := filepath.Join(n.LedgerRoot, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			ledger, err := parseLedger(path, data)
			if err != nil {
				return fmt.Errorf("read session retention ledger %s: %w", entry.Name(), err)
			}
			if ledger.IncidentId == "" || filepath.Base(n.ledgerPath(ledger.IncidentId)) != entry.Name() {
				return fmt.Errorf("session retention ledger %s has mismatched identity", entry.Name())
			}
			// A queued reflection still resumes the session it would delete.
			queued := ledger.ReflectionQueued && !ledger.ReflectionProcessed
			if ledger.Acknowledged && !queued && ledger.AcknowledgedAt != nil && !now.Before(ledger.AcknowledgedAt.AsTime().Add(retention)) {
				if err := memory.RemoveIncidentSession(n.SessionRoot, ledger.IncidentId); err != nil {
					return fmt.Errorf("prune session for %s: %w", ledger.IncidentId, err)
				}
			}
		}
		return nil
	})
}

func (n NativeIncidentBroker) setRolloutExpectation(ctx context.Context, ledger *contracts.BrokerLedger, paths []string) error {
	if ledger.ReflectionCommit == "" {
		return nil
	}
	selected, err := NewCodePathRegistryWithDependencies(CodePathDependencies{
		RebuildController: func(ctx context.Context, _ string) error { return n.prepareControllerRollout(ctx, ledger, paths) },
		PullAcceptedCommit: func(ctx context.Context, commit string) error {
			if ledger.ClosurePhaseTimestamps["shell_runner_activated"] != nil {
				return nil
			}
			if n.ShellReader == nil {
				return errors.New("shell runner activation reader is required")
			}
			bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			at, err := shellpath.AwaitActivation(bounded, n.ShellReader, commit)
			if err != nil {
				return err
			}
			if ledger.ClosurePhaseTimestamps == nil {
				ledger.ClosurePhaseTimestamps = map[string]*timestamppb.Timestamp{}
			}
			ledger.ClosurePhaseTimestamps["shell_runner_activated"] = timestamppb.New(at)
			return n.saveLedger(ledger)
		},
	}).Resolve(n.CodePath)
	if err != nil {
		return err
	}
	return selected.Activation.Activate(ctx, ledger.ReflectionCommit)
}
func (n NativeIncidentBroker) prepareControllerRollout(ctx context.Context, ledger *contracts.BrokerLedger, paths []string) error {
	required, err := n.Repository.RequiresControllerRollout(ctx, ledger.OutcomeCommit, ledger.ReflectionCommit, paths)
	if err != nil {
		return err
	}
	if !required {
		return nil
	}
	if ledger.OutcomeCommit == "" || ledger.ReflectionCommit == "" {
		return errors.New("controller rollout requires outcome and reflection commits")
	}
	before, err := n.Repository.DiagnosticsFingerprint(ctx, ledger.OutcomeCommit)
	if err != nil {
		return err
	}
	after, err := n.Repository.DiagnosticsFingerprint(ctx, ledger.ReflectionCommit)
	if err != nil {
		return err
	}
	if before == after {
		return errors.New("detector update did not change diagnostics tree")
	}
	expected := ControllerRolloutExpectation{IncidentID: ledger.IncidentId, ReflectionCommit: ledger.ReflectionCommit, BeforeDetectorFingerprint: before, AfterDetectorFingerprint: after}
	if ledger.RolloutExpectation != nil && *rolloutExpectationState(ledger.RolloutExpectation) != expected {
		return errors.New("durable controller rollout expectation changed")
	}
	ledger.RolloutExpectation = rolloutExpectationContract(&expected)
	return n.saveLedger(ledger)
}

// PendingControllerRollout reports an accepted detector change until its
// matching controller build and restart has been independently observed.
func (n NativeIncidentBroker) PendingControllerRollout(reflectionCommit string) (ControllerRolloutExpectation, bool, error) {
	var expected ControllerRolloutExpectation
	var pending bool
	err := n.withLock(func() error {
		entries, err := os.ReadDir(n.LedgerRoot)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(n.LedgerRoot, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			ledger, err := parseLedger(path, data)
			if err != nil {
				return err
			}
			if ledger.ReflectionCommit != reflectionCommit || ledger.RolloutExpectation == nil {
				continue
			}
			if pending {
				return errors.New("multiple incidents require the same reflection rollout")
			}
			if !successfulRollout(rolloutReceiptsState(ledger.RolloutReceipts)) {
				expected, pending = *rolloutExpectationState(ledger.RolloutExpectation), true
			}
		}
		return nil
	})
	return expected, pending, err
}

// RolloutPending is used by the controller effect pump before starting ACK.
// A pending rollout keeps the persisted receipt committed for restart replay.
func (n NativeIncidentBroker) RolloutPending(receipt ClosureReceipt) (bool, error) {
	if receipt.ReflectionCommit == "" {
		return false, nil
	}
	expectation, pending, err := n.PendingControllerRollout(receipt.ReflectionCommit)
	if err != nil || !pending {
		return pending, err
	}
	if expectation.IncidentID != receipt.IncidentID {
		return false, errors.New("rollout expectation belongs to another incident")
	}
	return true, nil
}

// PendingControllerRollouts survives the old controller process. A supervisor
// builds and launches the accepted revision; the new lease holder records a
// matching observation before the durable closure receipt can be acknowledged.
func (n NativeIncidentBroker) PendingControllerRollouts() ([]ControllerRolloutHandoff, error) {
	var handoffs []ControllerRolloutHandoff
	err := n.withLock(func() error {
		entries, err := os.ReadDir(n.LedgerRoot)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(n.LedgerRoot, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			ledger, err := parseLedger(path, data)
			if err != nil {
				return fmt.Errorf("broker ledger %s: %w", entry.Name(), err)
			}
			if ledger.IncidentId == "" || filepath.Base(n.ledgerPath(ledger.IncidentId)) != entry.Name() {
				return fmt.Errorf("broker ledger %s has mismatched identity", entry.Name())
			}
			if ledger.RolloutExpectation == nil || ledger.Receipt == nil || successfulRollout(rolloutReceiptsState(ledger.RolloutReceipts)) {
				continue
			}
			if ledger.RolloutExpectation.IncidentId != ledger.IncidentId || ledger.RolloutExpectation.ReflectionCommit != ledger.ReflectionCommit || ledger.Receipt.OutcomeCommit == "" || ledger.Receipt.AckToken == "" {
				return fmt.Errorf("broker ledger %s has incomplete rollout handoff", entry.Name())
			}
			handoffs = append(handoffs, ControllerRolloutHandoff{Expectation: *rolloutExpectationState(ledger.RolloutExpectation), Receipt: *closureReceiptState(ledger.Receipt)})
		}
		return nil
	})
	return handoffs, err
}

// RecordControllerRollout accepts a correlated observation from trusted
// installation code. Failed attempts remain pending; one success closes it.
func (n NativeIncidentBroker) RecordControllerRollout(receipt ControllerRolloutReceipt) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(receipt.Expectation.IncidentID)
		if err != nil {
			return err
		}
		if ledger.RolloutExpectation == nil || *rolloutExpectationState(ledger.RolloutExpectation) != receipt.Expectation {
			return errors.New("controller rollout receipt does not match accepted detector transition")
		}
		if receipt.VerifiedAt.IsZero() {
			return errors.New("controller rollout verification time is required")
		}
		if receipt.Success {
			// A later accepted revision carries its own diagnostics tree; the
			// trusted startup check has already verified it contains the reflection.
			exact := receipt.ControllerRevision == receipt.Expectation.ReflectionCommit
			if receipt.BuildArtifact == "" || receipt.ControllerRevision == "" || receipt.ObservedDetectorFingerprint == "" || (exact && receipt.ObservedDetectorFingerprint != receipt.Expectation.AfterDetectorFingerprint) {
				return errors.New("successful rollout lacks matching build and restarted detector fingerprint")
			}
			if successfulRollout(rolloutReceiptsState(ledger.RolloutReceipts)) {
				return errors.New("controller rollout already succeeded")
			}
		}
		ledger.RolloutReceipts = append(ledger.RolloutReceipts, rolloutReceiptContract(receipt))
		if receipt.Success {
			if err := n.recordPhase(ledger, "controller_restart_relaunch"); err != nil {
				return err
			}
			if err := n.recordPhase(ledger, "program_registry_rebuild"); err != nil {
				return err
			}
		}
		return n.saveLedger(ledger)
	})
}

func successfulRollout(receipts []ControllerRolloutReceipt) bool {
	for _, receipt := range receipts {
		if receipt.Success {
			return true
		}
	}
	return false
}

func (n NativeIncidentBroker) withLock(fn func() error) error {
	if n.Repository == nil || n.LedgerRoot == "" {
		return errors.New("native broker repository and ledger root are required")
	}
	if err := os.MkdirAll(n.LedgerRoot, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(n.LedgerRoot, "broker.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return fn()
}

// parseLedger decodes the ProtoJSON ledger stored at path. A document that
// predates the ProtoJSON cutover fails with a message naming the file and the
// recovery step; there is deliberately no legacy decoder.
func parseLedger(path string, data []byte) (*contracts.BrokerLedger, error) {
	ledger := &contracts.BrokerLedger{}
	if err := wire.UnmarshalDurable(data, ledger); err != nil {
		if isPreCutoverFormat(err) {
			return nil, fmt.Errorf("broker ledger %s predates the ProtoJSON cutover and cannot be read (%v); archive or remove the ledger file, then restart the controller", path, err)
		}
		return nil, err
	}
	return ledger, nil
}

func (n NativeIncidentBroker) readLedger(incidentID string) (*contracts.BrokerLedger, error) {
	if strings.TrimSpace(incidentID) == "" {
		return nil, errors.New("incident ID is required")
	}
	data, err := os.ReadFile(n.ledgerPath(incidentID))
	if os.IsNotExist(err) {
		return &contracts.BrokerLedger{IncidentId: incidentID}, nil
	}
	if err != nil {
		return nil, err
	}
	ledger, err := parseLedger(n.ledgerPath(incidentID), data)
	if err != nil {
		return nil, fmt.Errorf("broker ledger: %w", err)
	}
	if ledger.IncidentId != incidentID {
		return nil, errors.New("broker ledger incident ID mismatch")
	}
	return ledger, nil
}

func (n NativeIncidentBroker) saveLedger(ledger *contracts.BrokerLedger) error {
	data, err := wire.Canonical(ledger)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(n.LedgerRoot, ".ledger-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), n.ledgerPath(ledger.IncidentId)); err != nil {
		return err
	}
	directory, err := os.Open(n.LedgerRoot)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (n NativeIncidentBroker) ledgerPath(incidentID string) string {
	path, _ := broker.NativeLedgerFile(n.LedgerRoot, incidentID)
	return path
}

// committedRepairHealthReceipt authenticates the narrow post-commit health
// continuation against durable native evidence and broker Git attribution.
func (n NativeIncidentBroker) committedRepairHealthReceipt(ctx context.Context, closure IncidentClosure, receipt *ClosureReceipt) (ClosureReceipt, error) {
	var recovered ClosureReceipt
	err := n.withLock(func() error {
		if n.Repository == nil || closure.Request.IncidentID == "" {
			return ErrStaleHealthInvestigation
		}
		ledger, err := n.readLedger(closure.Request.IncidentID)
		if err != nil {
			return err
		}
		projected, err := incidentClosureContract(closure)
		if err != nil {
			return err
		}
		stored := closureReceiptState(ledger.Receipt)
		if stored == nil || stored.IncidentID != closure.Request.IncidentID || stored.BaseCommit != closure.Request.RepositoryBaseCommit || stored.ProposalCommit == "" || stored.OutcomeCommit == "" || stored.AckToken == "" || !proto.Equal(ledger.Closure, projected) || ledger.OutcomeDocument == nil || len(closure.FinalDetectorStates) == 0 {
			return ErrStaleHealthInvestigation
		}
		if receipt != nil && *receipt != *stored {
			return ErrStaleHealthInvestigation
		}
		if ledger.ProposalCommit != stored.ProposalCommit || ledger.OutcomeCommit != stored.OutcomeCommit {
			return ErrStaleHealthInvestigation
		}
		for phase, commit := range map[broker.Phase]string{broker.ProposalPhase: stored.ProposalCommit, broker.OutcomePhase: stored.OutcomeCommit} {
			accepted, ok, err := n.Repository.AcceptedCommit(ctx, stored.IncidentID, phase)
			if err != nil {
				return err
			}
			if !ok || accepted.Commit != commit {
				return ErrStaleHealthInvestigation
			}
		}
		recovered = *stored
		return nil
	})
	return recovered, err
}

// committedOutcomeClosure authenticates a historical outcome before allowing
// restart to resume reflection/receipt work. Exact frozen evidence and the
// broker's accepted Git commit are both required; no receipt is needed yet.
func (n NativeIncidentBroker) committedOutcomeClosure(ctx context.Context, closure IncidentClosure, receipt *ClosureReceipt) (bool, error) {
	if n.Repository == nil || closure.Request.IncidentID == "" {
		return false, nil
	}
	// Ledger writes use fsync + atomic rename. Read the immutable accepted
	// outcome checkpoint without the effect lock: reflection may still own that
	// lock while its Job runs, and health checks must not wait for the worker.
	ledger, err := n.readLedger(closure.Request.IncidentID)
	if err != nil {
		return false, err
	}
	projected, err := incidentClosureContract(closure)
	if err != nil {
		return false, err
	}
	if ledger.OutcomeDocument == nil || ledger.OutcomeDocument.GetIncidentId() != closure.Request.IncidentID || !proto.Equal(ledger.Closure, projected) {
		return false, nil
	}
	if receipt != nil && (ledger.Receipt == nil || !proto.Equal(ledger.Receipt, closureReceiptContract(receipt))) {
		return false, nil
	}
	if ledger.ReflectionProcessed && ledger.Receipt == nil {
		return false, nil
	}
	accepted := false
	if ledger.OutcomeCommit != "" {
		_, accepted, err = n.Repository.AcceptedCommitAt(ctx, closure.Request.IncidentID, broker.OutcomePhase, ledger.OutcomeCommit)
	} else {
		// Git acceptance can survive a crash before its ledger marker is saved.
		_, accepted, err = n.Repository.AcceptedCommit(ctx, closure.Request.IncidentID, broker.OutcomePhase)
	}
	if err != nil || !accepted {
		return accepted, err
	}
	return true, nil
}

func (n NativeIncidentBroker) checkHistoricalClosureHealth(ctx context.Context, closure IncidentClosure) error {
	ledger, err := n.readLedger(closure.Request.IncidentID)
	if err != nil {
		return err
	}
	verifiedRevision := closure.Request.SourceCommit
	if ledger.ReflectionCommit != "" {
		_, accepted, err := n.Repository.AcceptedCommitAt(ctx, closure.Request.IncidentID, broker.ReflectionPhase, ledger.ReflectionCommit)
		if err != nil {
			return err
		}
		if accepted {
			// Its own validated library publication cannot invalidate its ACK.
			// Later objective/policy advances still compare against that commit.
			verifiedRevision = ledger.ReflectionCommit
		}
	}
	diagnostics, err := n.Repository.ReflectionHealthDiagnostics(ctx, verifiedRevision)
	if err != nil {
		return err
	}
	if len(diagnostics) > 0 {
		// Record the rejection before the controller durably retires ownership,
		// including when leadership is lost before the worker finishes.
		attempt, _ := lifecyclecore.ReflectionFeedback(ledger)
		rejection := &contracts.ProposalRejection{Attempt: attempt, Diagnostics: diagnostics, RejectedAt: contractTimestamp(n.clock())}
		for _, prior := range ledger.GetReflectionRejections() {
			if len(prior.Diagnostics) == 1 && proto.Equal(prior.Diagnostics[0], diagnostics[0]) {
				rejection = prior
				break
			}
		}
		_, err = n.Repository.RecordReflectionRejections(ctx, closure.Request.IncidentID, []*contracts.ProposalRejection{rejection})
		if err != nil {
			return err
		}
		return ErrStaleHealthInvestigation
	}
	return nil
}

func logicalReflectionDecision(decision ReflectionDecision) *contracts.ReflectionDecision {
	result := &contracts.ReflectionDecision{DeclineReason: decision.DeclineReason, LearningDecision: decision.LearningDecision, NoChangeReason: decision.NoChangeReason, Error: decision.Error, FailureReason: decision.FailureReason, SdkGaps: decision.SDKGaps, PlaybookIds: decision.PlaybookIDs}
	if decision.Usage != nil {
		result.Usage = proto.Clone(decision.Usage).(*contracts.UsageMetrics)
	}
	return result
}
