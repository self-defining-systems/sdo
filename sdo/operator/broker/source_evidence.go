package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func validateSourceEvidence(value SourceEvidence) error {
	if value.AcceptedCommit != "" {
		return errors.New("deployment evidence cannot claim an accepted commit")
	}
	if !validGitID(value.ProposalTreeDigest) || value.ObservedRevision != value.ProposalTreeDigest || value.StrategyID == "" || !value.Ready || len(value.Images) == 0 || len(value.ResourceIDs) == 0 {
		return errors.New("trusted source evidence is incomplete or does not match the deployed tree")
	}
	seen := map[string]bool{}
	for _, image := range value.Images {
		parts := strings.Split(image, "@sha256:")
		if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
			return fmt.Errorf("source evidence image is not immutable: %q", image)
		}
		if _, err := hex.DecodeString(parts[1]); err != nil {
			return fmt.Errorf("source evidence image digest: %w", err)
		}
	}
	for _, resource := range value.ResourceIDs {
		if resource == "" || seen[resource] {
			return errors.New("source evidence resource IDs must be unique and nonempty")
		}
		seen[resource] = true
	}
	return nil
}

func validGitID(value string) bool {
	if (len(value) != 40 && len(value) != 64) || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// SourceEvidence returns only evidence bound to an accepted source commit. A
// pending record without a commit is never presented as deployment success.
func (b *Broker) SourceEvidence(ctx context.Context, incidentID string) (SourceEvidence, bool, error) {
	var value SourceEvidence
	var found bool
	err := b.locked(func() error {
		receipt, accepted, err := b.findAccepted(ctx, incidentID, InitialPhase)
		if err != nil || !accepted {
			return err
		}
		value, found, err = b.sourceEvidenceLocked(ctx, incidentID, receipt)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("accepted source commit has no trusted deployment evidence")
		}
		return nil
	})
	return value, found, err
}

func (b *Broker) sourceEvidenceLocked(ctx context.Context, incidentID string, receipt Receipt) (SourceEvidence, bool, error) {
	data, err := os.ReadFile(b.sourceEvidencePath(incidentID))
	if errors.Is(err, os.ErrNotExist) {
		return SourceEvidence{}, false, nil
	}
	if err != nil {
		return SourceEvidence{}, false, err
	}
	var value SourceEvidence
	if err := json.Unmarshal(data, &value); err != nil {
		return SourceEvidence{}, false, fmt.Errorf("decode source evidence: %w", err)
	}
	persistedCommit := value.AcceptedCommit
	if value.AcceptedCommit != "" && value.AcceptedCommit != receipt.Commit {
		return SourceEvidence{}, false, errors.New("source evidence points to another accepted commit")
	}
	value.AcceptedCommit = ""
	if err := validateSourceEvidence(value); err != nil {
		return SourceEvidence{}, false, err
	}
	acceptedTree, err := gitText(ctx, b.target, "rev-parse", receipt.Commit+"^{tree}")
	if err != nil {
		return SourceEvidence{}, false, err
	}
	if value.ProposalTreeDigest != acceptedTree {
		return SourceEvidence{}, false, errors.New("accepted source tree differs from trusted deployment evidence")
	}
	value.AcceptedCommit = receipt.Commit
	if persistedCommit == "" {
		if err := b.writeSourceEvidence(incidentID, value); err != nil {
			return SourceEvidence{}, false, fmt.Errorf("reconcile accepted source evidence: %w", err)
		}
	}
	return value, true, nil
}

func (b *Broker) sourceEvidencePath(incidentID string) string {
	digest := sha256.Sum256([]byte(incidentID))
	return filepath.Join(b.workspaceRoot, "source-evidence", fmt.Sprintf("%x.json", digest))
}

// writeSourceEvidence atomically replaces one broker-owned ledger record. Call
// while holding the broker lock; fsync makes the pre-commit record durable.
func (b *Broker) writeSourceEvidence(incidentID string, value SourceEvidence) error {
	path := b.sourceEvidencePath(incidentID)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".source-evidence-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
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
