package broker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/validate"
)

// MaxValidationRetries bounds how often Go re-dispatches an agent task after
// the broker rejected its proposal with validator diagnostics. A task is
// therefore dispatched at most 1+MaxValidationRetries times.
const MaxValidationRetries = 5

// RejectionDiagnostics returns the typed validator diagnostics carried by a
// broker rejection. Errors that are not validator failures (infrastructure,
// isolated-job failures) report false and are not retried.
func RejectionDiagnostics(err error) ([]*contracts.MemoryDiagnostic, bool) {
	var conflict *MemoryConflict
	if errors.As(err, &conflict) {
		return []*contracts.MemoryDiagnostic{conflict.Diagnostic()}, true
	}
	var failure *validate.Failure
	if !errors.As(err, &failure) || len(failure.Diagnostics) == 0 {
		return nil, false
	}
	return validate.ProtoDiagnostics(failure.Diagnostics), true
}

// NewProposalRejection records the 1-based attempt the broker rejected.
func NewProposalRejection(attempt uint32, diagnostics []*contracts.MemoryDiagnostic) *contracts.ProposalRejection {
	return &contracts.ProposalRejection{Attempt: attempt, Diagnostics: diagnostics, RejectedAt: timestamppb.Now()}
}

// RetriesExhausted reports whether the recorded rejections spent the bound.
func RetriesExhausted(rejections []*contracts.ProposalRejection) bool {
	return len(rejections) > MaxValidationRetries
}

// Rejections returns the durable rejection log of a proposal that has no
// incident ledger. A proposal never rejected has an empty log.
func (b *Broker) Rejections(proposalID string) (*contracts.ProposalRejectionLog, error) {
	var log *contracts.ProposalRejectionLog
	err := b.locked(func() error {
		var err error
		log, err = b.readRejections(proposalID)
		return err
	})
	return log, err
}

// RecordRejection appends one rejected attempt to the proposal's durable log
// and marks it terminal once the retry bound is spent. The attempt number is
// derived from the log, so a restart can neither reset nor repeat attempts.
func (b *Broker) RecordRejection(proposalID string, diagnostics []*contracts.MemoryDiagnostic) (*contracts.ProposalRejectionLog, error) {
	var log *contracts.ProposalRejectionLog
	err := b.locked(func() error {
		var err error
		log, err = b.readRejections(proposalID)
		if err != nil {
			return err
		}
		if log.Terminal {
			return nil
		}
		log.Rejections = append(log.Rejections, NewProposalRejection(uint32(len(log.Rejections)+1), diagnostics))
		log.Terminal = RetriesExhausted(log.Rejections)
		return b.writeRejections(log)
	})
	return log, err
}

func (b *Broker) rejectionsPath(proposalID string) string {
	digest := sha256.Sum256([]byte(proposalID))
	return filepath.Join(b.workspaceRoot, "rejections", fmt.Sprintf("%x.json", digest))
}

func (b *Broker) readRejections(proposalID string) (*contracts.ProposalRejectionLog, error) {
	data, err := os.ReadFile(b.rejectionsPath(proposalID))
	if errors.Is(err, os.ErrNotExist) {
		return &contracts.ProposalRejectionLog{ProposalId: proposalID}, nil
	}
	if err != nil {
		return nil, err
	}
	log := &contracts.ProposalRejectionLog{}
	if err := wire.Unmarshal(data, log); err != nil {
		return nil, err
	}
	if log.ProposalId != proposalID {
		return nil, errors.New("proposal rejection log identity mismatch")
	}
	return log, nil
}

func (b *Broker) writeRejections(log *contracts.ProposalRejectionLog) error {
	path := b.rejectionsPath(log.ProposalId)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	data, err := wire.Marshal(log)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".rejections-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
