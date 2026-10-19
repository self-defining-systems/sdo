package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
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
	Reflect(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string) (ReflectionDecision, error)
}

type ReflectionDecision struct {
	LearningDecision string `json:"learning_decision"`
	NoChangeReason   string `json:"no_change_reason,omitempty"`
}

type ReflectionWorkerFunc func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string) (ReflectionDecision, error)

func (f ReflectionWorkerFunc) Reflect(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, proposalCommit, outcomeCommit string) (ReflectionDecision, error) {
	return f(ctx, closure, workspace, outcome, proposalCommit, outcomeCommit)
}

// NativeIncidentBroker is the restartable Go closure coordinator. It is only
// safe to select when a full OutcomeDeriver and ReflectionWorker are configured;
// no partial closure can be acknowledged.
type NativeIncidentBroker struct {
	Repository           *broker.Broker
	LedgerRoot           string
	Deriver              OutcomeDeriver
	Reflector            ReflectionWorker
	ResponderBackend     string
	ResponderModel       string
	SessionRoot          string
	SessionRetention     time.Duration
	LateFindingsVerifier interface {
		VerifyPull(context.Context, string, uint32, string, []string, []string) error
	}
}

type ControllerRolloutExpectation struct {
	IncidentID                string `json:"incident_id"`
	ReflectionCommit          string `json:"reflection_commit"`
	BeforeDetectorFingerprint string `json:"before_detector_fingerprint"`
	AfterDetectorFingerprint  string `json:"after_detector_fingerprint"`
}

// ControllerRolloutReceipt comes from trusted installation code after build
// and observation of the restarted controller's detector revision.
type ControllerRolloutReceipt struct {
	Expectation                 ControllerRolloutExpectation `json:"expectation"`
	BuildArtifact               string                       `json:"build_artifact"`
	ControllerRevision          string                       `json:"controller_revision"`
	ObservedDetectorFingerprint string                       `json:"observed_detector_fingerprint"`
	Success                     bool                         `json:"success"`
	VerifiedAt                  time.Time                    `json:"verified_at"`
}

// ControllerRolloutHandoff is durable evidence for the supervisor to restart
// from an accepted detector revision before acknowledging a closure.
type ControllerRolloutHandoff struct {
	Expectation ControllerRolloutExpectation
	Receipt     ClosureReceipt
}

type nativeLedger struct {
	IncidentID          string                        `json:"incident_id"`
	Closure             json.RawMessage               `json:"closure,omitempty"`
	ProposalCommit      string                        `json:"proposal_commit,omitempty"`
	OutcomeDocument     json.RawMessage               `json:"outcome_document,omitempty"`
	RequiresReflection  bool                          `json:"requires_reflection,omitempty"`
	OutcomeCommit       string                        `json:"outcome_commit,omitempty"`
	ReflectionProcessed bool                          `json:"reflection_processed,omitempty"`
	ReflectionDecision  *ReflectionDecision           `json:"reflection_decision,omitempty"`
	ReflectionCommit    string                        `json:"reflection_commit,omitempty"`
	RolloutExpectation  *ControllerRolloutExpectation `json:"rollout_expectation,omitempty"`
	RolloutReceipts     []ControllerRolloutReceipt    `json:"rollout_receipts,omitempty"`
	Receipt             *ClosureReceipt               `json:"receipt,omitempty"`
	Acknowledged        bool                          `json:"acknowledged,omitempty"`
	AcknowledgedAt      time.Time                     `json:"acknowledged_at,omitempty"`
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
	if err := closure.Request.Validate(); err != nil {
		return ClosureReceipt{}, fmt.Errorf("invalid closure request: %w", err)
	}
	if closure.Result != nil {
		if err := closure.Result.ValidateFor(closure.Request); err != nil {
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
		encoded, err := json.Marshal(closure)
		if err != nil {
			return err
		}
		if len(ledger.Closure) > 0 && !bytes.Equal(ledger.Closure, encoded) {
			return errors.New("closure differs from persisted broker evidence")
		}
		prepared, err := n.Repository.ProposalInfo(id, broker.ProposalPhase)
		if err != nil && ledger.Receipt == nil {
			return fmt.Errorf("prepared proposal is missing: %w", err)
		}
		if err == nil && (closure.Request.RepositoryWorktree != prepared.Path || closure.Request.RepositoryBaseCommit != prepared.BaseCommit) {
			return errors.New("closure workspace does not match broker-prepared proposal")
		}
		if len(ledger.Closure) == 0 {
			ledger.Closure = encoded
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}
		if ledger.Receipt != nil {
			receipt = *ledger.Receipt
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
				if len(changed) > 0 {
					accepted, err := n.Repository.Publish(ctx, id, broker.Responder, broker.ProposalPhase)
					if err != nil {
						return err
					}
					ledger.ProposalCommit = accepted.Commit
				} else if closure.Request.RepairPolicy == "commit" {
					return errors.New("commit repair policy requires a nonempty validated proposal")
				}
			}
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}

		if len(ledger.OutcomeDocument) == 0 {
			derived, err := n.Deriver.Derive(ctx, closure, ledger.ProposalCommit, n.ResponderBackend, n.ResponderModel)
			if err != nil {
				return fmt.Errorf("derive independent outcome: %w", err)
			}
			if derived.Record == nil || derived.Record.GetIncidentId() != id {
				return errors.New("outcome deriver returned an invalid typed record")
			}
			if derived.RequiresReflection && derived.Record.GetClassification() != "success" {
				return errors.New("outcome reflection requires successful classification")
			}
			encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(derived.Record)
			if err != nil {
				return fmt.Errorf("encode outcome ledger: %w", err)
			}
			ledger.OutcomeDocument = encoded
			ledger.RequiresReflection = derived.RequiresReflection
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}
		if ledger.OutcomeCommit == "" {
			record := new(contracts.OutcomeRecord)
			if err := protojson.Unmarshal(ledger.OutcomeDocument, record); err != nil {
				return fmt.Errorf("decode outcome ledger: %w", err)
			}
			accepted, err := n.Repository.RecordOutcome(ctx, id, record)
			if err != nil {
				return err
			}
			ledger.OutcomeCommit = accepted.Commit
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
		}

		// The persisted decision prevents a restart from changing learning policy.
		if ledger.RequiresReflection && !ledger.ReflectionProcessed {
			if n.Reflector == nil {
				return errors.New("verified success requires a registered reflection worker")
			}
			var reflectionPaths []string
			if accepted, ok, err := n.Repository.AcceptedCommit(ctx, id, broker.ReflectionPhase); err != nil {
				return err
			} else if ok {
				ledger.ReflectionCommit = accepted.Commit
				reflectionPaths = accepted.ChangedPaths
			} else {
				proposal, err := n.Repository.CreateProposalForPhase(ctx, id, broker.ReflectionPhase)
				if err != nil {
					return err
				}
				paths, err := n.Repository.ChangedPaths(ctx, id, broker.ReflectionPhase)
				if err != nil {
					return err
				}
				if len(paths) == 0 {
					workspace := IncidentWorkspace{IncidentID: id, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}
					record := new(contracts.OutcomeRecord)
					if err := protojson.Unmarshal(ledger.OutcomeDocument, record); err != nil {
						return fmt.Errorf("decode reflection outcome: %w", err)
					}
					decision, err := n.Reflector.Reflect(ctx, closure, workspace, record, ledger.ProposalCommit, ledger.OutcomeCommit)
					if err != nil {
						return fmt.Errorf("reflection worker: %w", err)
					}
					ledger.ReflectionDecision = &decision
					if err := n.saveLedger(ledger); err != nil {
						return err
					}
					paths, err = n.Repository.ChangedPaths(ctx, id, broker.ReflectionPhase)
					if err != nil {
						return err
					}
				}
				if ledger.ReflectionDecision == nil {
					if err := n.Repository.ReleaseProposal(id, broker.ReflectionPhase); err != nil {
						return err
					}
					return errors.New("reflection changes lack a durable worker decision; proposal reset")
				}
				if len(paths) > 0 {
					if ledger.ReflectionDecision.LearningDecision != "updated" {
						if err := n.Repository.ReleaseProposal(id, broker.ReflectionPhase); err != nil {
							return err
						}
						ledger.ReflectionDecision = nil
						if err := n.saveLedger(ledger); err != nil {
							return err
						}
						return errors.New("reflection changed files without declaring updated learning")
					}
					accepted, err := n.Repository.Publish(ctx, id, broker.Responder, broker.ReflectionPhase)
					if err != nil {
						if resetErr := n.Repository.ReleaseProposal(id, broker.ReflectionPhase); resetErr != nil {
							return fmt.Errorf("reflection rejected: %v; reset proposal: %w", err, resetErr)
						}
						return err
					}
					ledger.ReflectionCommit = accepted.Commit
					reflectionPaths = accepted.ChangedPaths
				} else if ledger.ReflectionDecision.LearningDecision != "no_change" || strings.TrimSpace(ledger.ReflectionDecision.NoChangeReason) == "" {
					return errors.New("no-op reflection requires no_change decision and concrete reason")
				}
			}
			if err := n.setRolloutExpectation(ctx, &ledger, reflectionPaths); err != nil {
				return err
			}
			ledger.ReflectionProcessed = true
			if err := n.saveLedger(ledger); err != nil {
				return err
			}
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
		ledger.Receipt = &receipt
		return n.saveLedger(ledger)
	})
	return receipt, err
}

func (n NativeIncidentBroker) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(receipt.IncidentID)
		if err != nil {
			return err
		}
		if ledger.Receipt == nil || *ledger.Receipt != receipt {
			return errors.New("closure receipt does not match durable broker state")
		}
		if ledger.RolloutExpectation != nil && !successfulRollout(ledger.RolloutReceipts) {
			return errors.New("controller rollout has not been independently verified")
		}
		if err := n.Repository.ReleaseProposal(receipt.IncidentID, broker.ProposalPhase); err != nil {
			return err
		}
		if err := n.Repository.ReleaseProposal(receipt.IncidentID, broker.ReflectionPhase); err != nil {
			return err
		}
		ledger.Acknowledged = true
		if ledger.AcknowledgedAt.IsZero() {
			ledger.AcknowledgedAt = time.Now().UTC()
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
			data, err := os.ReadFile(filepath.Join(n.LedgerRoot, entry.Name()))
			if err != nil {
				return err
			}
			var ledger nativeLedger
			if err := json.Unmarshal(data, &ledger); err != nil {
				return fmt.Errorf("read session retention ledger %s: %w", entry.Name(), err)
			}
			if ledger.IncidentID == "" || filepath.Base(n.ledgerPath(ledger.IncidentID)) != entry.Name() {
				return fmt.Errorf("session retention ledger %s has mismatched identity", entry.Name())
			}
			if ledger.Acknowledged && !ledger.AcknowledgedAt.IsZero() && !now.Before(ledger.AcknowledgedAt.Add(retention)) {
				if err := memory.RemoveIncidentSession(n.SessionRoot, ledger.IncidentID); err != nil {
					return fmt.Errorf("prune session for %s: %w", ledger.IncidentID, err)
				}
			}
		}
		return nil
	})
}

func (n NativeIncidentBroker) setRolloutExpectation(ctx context.Context, ledger *nativeLedger, paths []string) error {
	if !layout.RequiresControllerRollout(paths) {
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
	expected := ControllerRolloutExpectation{IncidentID: ledger.IncidentID, ReflectionCommit: ledger.ReflectionCommit, BeforeDetectorFingerprint: before, AfterDetectorFingerprint: after}
	if ledger.RolloutExpectation != nil && *ledger.RolloutExpectation != expected {
		return errors.New("durable controller rollout expectation changed")
	}
	ledger.RolloutExpectation = &expected
	return n.saveLedger(*ledger)
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
			data, err := os.ReadFile(filepath.Join(n.LedgerRoot, entry.Name()))
			if err != nil {
				return err
			}
			var ledger nativeLedger
			if err := json.Unmarshal(data, &ledger); err != nil {
				return err
			}
			if ledger.ReflectionCommit != reflectionCommit || ledger.RolloutExpectation == nil {
				continue
			}
			if pending {
				return errors.New("multiple incidents require the same reflection rollout")
			}
			if !successfulRollout(ledger.RolloutReceipts) {
				expected, pending = *ledger.RolloutExpectation, true
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
			data, err := os.ReadFile(filepath.Join(n.LedgerRoot, entry.Name()))
			if err != nil {
				return err
			}
			var ledger nativeLedger
			if err := json.Unmarshal(data, &ledger); err != nil {
				return fmt.Errorf("broker ledger %s: %w", entry.Name(), err)
			}
			if ledger.IncidentID == "" || filepath.Base(n.ledgerPath(ledger.IncidentID)) != entry.Name() {
				return fmt.Errorf("broker ledger %s has mismatched identity", entry.Name())
			}
			if ledger.RolloutExpectation == nil || ledger.Receipt == nil || successfulRollout(ledger.RolloutReceipts) {
				continue
			}
			if ledger.RolloutExpectation.IncidentID != ledger.IncidentID || ledger.RolloutExpectation.ReflectionCommit != ledger.Receipt.ReflectionCommit || ledger.Receipt.OutcomeCommit == "" || ledger.Receipt.AckToken == "" {
				return fmt.Errorf("broker ledger %s has incomplete rollout handoff", entry.Name())
			}
			handoffs = append(handoffs, ControllerRolloutHandoff{Expectation: *ledger.RolloutExpectation, Receipt: *ledger.Receipt})
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
		if ledger.RolloutExpectation == nil || *ledger.RolloutExpectation != receipt.Expectation {
			return errors.New("controller rollout receipt does not match accepted detector transition")
		}
		if receipt.VerifiedAt.IsZero() {
			return errors.New("controller rollout verification time is required")
		}
		if receipt.Success {
			if receipt.BuildArtifact == "" || receipt.ControllerRevision == "" || receipt.ObservedDetectorFingerprint != receipt.Expectation.AfterDetectorFingerprint {
				return errors.New("successful rollout lacks matching build and restarted detector fingerprint")
			}
			if successfulRollout(ledger.RolloutReceipts) {
				return errors.New("controller rollout already succeeded")
			}
		}
		ledger.RolloutReceipts = append(ledger.RolloutReceipts, receipt)
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

func (n NativeIncidentBroker) ReleaseIncident(_ context.Context, incidentID string) error {
	return n.withLock(func() error {
		ledger, err := n.readLedger(incidentID)
		if err != nil {
			return err
		}
		if len(ledger.Closure) > 0 {
			return nil
		}
		return n.Repository.ReleaseProposal(incidentID, broker.ProposalPhase)
	})
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

func (n NativeIncidentBroker) readLedger(incidentID string) (nativeLedger, error) {
	if strings.TrimSpace(incidentID) == "" {
		return nativeLedger{}, errors.New("incident ID is required")
	}
	ledger := nativeLedger{IncidentID: incidentID}
	data, err := os.ReadFile(n.ledgerPath(incidentID))
	if os.IsNotExist(err) {
		return ledger, nil
	}
	if err != nil {
		return ledger, err
	}
	if err := json.Unmarshal(data, &ledger); err != nil {
		return nativeLedger{}, fmt.Errorf("broker ledger: %w", err)
	}
	if ledger.IncidentID != incidentID {
		return nativeLedger{}, errors.New("broker ledger incident ID mismatch")
	}
	return ledger, nil
}

func (n NativeIncidentBroker) saveLedger(ledger nativeLedger) error {
	data, err := json.Marshal(ledger)
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
	if err := os.Rename(file.Name(), n.ledgerPath(ledger.IncidentID)); err != nil {
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
