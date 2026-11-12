package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// What became of the source a responder committed (ledger source_repair and
// the outcome's source_repair.status).
const (
	sourcePending   = "pending"
	sourceDeployed  = "deployed"
	sourcePublished = "published"
	sourceHeld      = "held"
	sourceNone      = "none"
)

// SourceGate reports work that a source deploy after a closure must not
// overlap. The controller sets it, and installation wiring may add checks (a
// pending namespace onboarding); a broker without one is never busy.
type SourceGate struct {
	mu     sync.Mutex
	busy   func() string
	checks []func() string
}

// Add registers one more reason a deploy must wait, read after the controller's own.
func (g *SourceGate) Add(check func() string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.checks = append(g.checks, check)
}

// Set installs the controller's view of its own activity.
func (g *SourceGate) Set(busy func() string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.busy = busy
}

// Busy is the reason the deploy must wait, or "".
func (g *SourceGate) Busy() string {
	g.mu.Lock()
	busy, checks := g.busy, append([]func() string(nil), g.checks...)
	g.mu.Unlock()
	if busy != nil {
		if reason := busy(); reason != "" {
			return reason
		}
	}
	for _, check := range checks {
		if reason := check(); reason != "" {
			return reason
		}
	}
	return ""
}

// SourceDeferredError means a source publication did not start: a deploy
// needs the controller idle and paused. It is not a failed attempt; the queue
// tries the incident again after a fixed interval.
type SourceDeferredError struct{ Reason string }

func (e *SourceDeferredError) Error() string {
	return "source publication deferred: " + e.Reason
}

// Deferred marks the error for the controller's queue.
func (*SourceDeferredError) Deferred() bool { return true }

// planSourceRepair decides at closure what source publication is owed. A
// closure that is not verified publishes nothing. Uncommitted app files alone
// still owe a (discarding) publication: it records them, and a later turn may
// still commit them.
func (n NativeIncidentBroker) planSourceRepair(ctx context.Context, id string, verified, commitPolicy bool) (status, detail string, err error) {
	if !verified {
		return sourceNone, "", nil
	}
	partition, err := n.Repository.ProposalPartition(ctx, id, broker.ProposalPhase)
	var failure *validate.Failure
	if errors.As(err, &failure) {
		// The agent rewrote history below its base: nothing is publishable.
		return sourceNone, failure.Error(), nil
	}
	if err != nil {
		return "", "", err
	}
	if len(broker.SourceRepairPaths(partition.SDO))+len(partition.Source)+len(partition.Scratch) > 0 {
		return sourcePending, "", nil
	}
	if commitPolicy {
		return "", "", errors.New("commit repair policy requires a nonempty validated proposal")
	}
	return sourceNone, "", nil
}

// publishSource publishes the source a closed incident's responder kept: it
// deploys and verifies it (or accepts it plainly where nothing can deploy),
// appends the result to the outcome, and releases the proposal checkout. A
// rejection by the broker's rules is final and recorded as a hold; a deferral
// or an unexpected error leaves the publication pending for a retry.
func (n NativeIncidentBroker) publishSource(ctx context.Context, id string) error {
	var ledger *contracts.BrokerLedger
	err := n.withLock(func() (err error) {
		ledger, err = n.readLedger(id)
		return err
	})
	if err != nil || ledger.SourceRepair != sourcePending {
		return err
	}
	var (
		repair    SourceRepair
		uncovered []string
	)
	partition, err := n.Repository.ProposalPartition(ctx, id, broker.ProposalPhase)
	var failure *validate.Failure
	switch {
	case errors.Is(err, os.ErrNotExist):
		repair = SourceRepair{Held: "the responder's proposal checkout is no longer available"}
	case errors.As(err, &failure):
		repair = SourceRepair{}
		ledger.SourceDetail = failure.Error()
	case err != nil:
		return err
	default:
		uncovered = partition.Scratch
		paths := append(broker.SourceRepairPaths(partition.SDO), partition.Source...)
		if _, held := ledger.GetClosurePhaseTimestamps()["source_repair_held"]; held {
			// A hold is final for this closure: a retry never deploys again.
			repair = SourceRepair{Held: "held by an earlier attempt of this closure"}
		} else if len(paths) > 0 {
			repair, err = n.acceptSourceRepair(ctx, id, paths)
			var conflict *broker.MemoryConflict
			switch {
			case errors.As(err, &failure):
				repair, err = SourceRepair{Held: failure.Error()}, nil
			case errors.As(err, &conflict):
				// The change cannot be rebased onto the branch head. Keep it for a
				// person on the side branch; nothing is deployed or retried.
				branch, holdErr := n.Repository.HoldSource(ctx, id)
				if holdErr != nil {
					slog.Warn("source_conflict_not_kept", "incident_id", id, "error", holdErr)
				}
				detail := fmt.Sprintf("source_conflict: %s", strings.Join(conflict.Conflict.GetConflictingPaths(), ", "))
				if branch != "" {
					detail += " (kept on " + branch + ")"
				}
				repair, err = SourceRepair{Held: detail}, nil
			}
			if err != nil {
				return err
			}
			if repair.Deferred != "" {
				return &SourceDeferredError{Reason: repair.Deferred}
			}
		}
	}
	return n.withLock(func() error {
		ledger, err := n.readLedger(id)
		if err != nil {
			return err
		}
		if ledger.SourceRepair != sourcePending {
			return nil
		}
		switch {
		case repair.Held != "":
			ledger.SourceRepair, ledger.SourceDetail = sourceHeld, repair.Held
			if err := n.recordPhase(ledger, "source_repair_held"); err != nil {
				return err
			}
		case repair.Deployed:
			ledger.SourceRepair, ledger.SourceCommit, ledger.ProposalCommit = sourceDeployed, repair.Commit, repair.Commit
			if err := n.recordPhase(ledger, "source_repair_deployed"); err != nil {
				return err
			}
		case repair.Commit != "":
			// Accepted without a deployment: nothing SDO owns could apply it.
			if repair.Undeployed {
				slog.Warn("source_repair_published_undeployed", "incident_id", id, "commit", repair.Commit)
				if err := n.recordPhase(ledger, "source_repair_published_undeployed"); err != nil {
					return err
				}
			}
			ledger.SourceRepair, ledger.SourceCommit, ledger.ProposalCommit = sourcePublished, repair.Commit, repair.Commit
		default:
			ledger.SourceRepair = sourceNone
		}
		if len(uncovered) > 0 {
			ledger.SourceUncaptured = uncovered
			slog.Warn("source_repair_uncaptured", "incident_id", id, "paths", len(uncovered))
			if err := n.recordPhase(ledger, "source_repair_uncaptured"); err != nil {
				return err
			}
		}
		if _, err := n.Repository.RecordSourceRepair(ctx, id, sourceRepairContract(ledger)); err != nil {
			return fmt.Errorf("record source repair on the outcome: %w", err)
		}
		if err := n.Repository.ReleaseProposal(id, broker.ProposalPhase); err != nil {
			return err
		}
		if !ledger.RequiresReflection {
			// A source-only queue entry ends with its publication.
			ledger.ReflectionProcessed = true
		}
		if err := n.recordPhase(ledger, "source_repair_"+ledger.SourceRepair); err != nil {
			return err
		}
		return n.saveLedger(ledger)
	})
}

// sourceRepairContract is the ledger's source result as the outcome records it.
func sourceRepairContract(ledger *contracts.BrokerLedger) *contracts.SourceRepair {
	return &contracts.SourceRepair{
		Status: ledger.GetSourceRepair(), Commit: ledger.GetSourceCommit(), Detail: ledger.GetSourceDetail(),
		UncapturedPaths: append([]string(nil), ledger.GetSourceUncaptured()...),
	}
}
