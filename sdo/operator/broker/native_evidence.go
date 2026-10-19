package broker

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"sdo.dev/operator/contracts/wire"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const DefaultNativeWorktreeRoot = "/workspace/worktrees"

// NativeLedgerRoot and NativeLedgerPath are the physical broker storage edge.
// Callers use the returned opaque path and never reconstruct the layout.
func NativeLedgerRoot(worktreeRoot string) (string, error) {
	if !filepath.IsAbs(worktreeRoot) || worktreeRoot == "/" {
		return "", errors.New("native broker worktree root must be absolute")
	}
	return filepath.Join(worktreeRoot, "native-ledger"), nil
}

func NativeLedgerPath(worktreeRoot, incidentID string) (string, error) {
	root, err := NativeLedgerRoot(worktreeRoot)
	if err != nil {
		return "", err
	}
	return NativeLedgerFile(root, incidentID)
}

func NativeLedgerFile(root, incidentID string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", errors.New("native ledger root must be absolute")
	}
	if strings.TrimSpace(incidentID) == "" {
		return "", errors.New("incident ID is required")
	}
	sum := sha256.Sum256([]byte(incidentID))
	return filepath.Join(root, fmt.Sprintf("%x.json", sum)), nil
}

// ValidateNativeLedger parses the ledger as the canonical ProtoJSON
// BrokerLedger and rejects a mismatched incident before evidence leaves broker
// storage. The payload stays raw ProtoJSON for benchmark-specific analysis.
func ValidateNativeLedger(data []byte, incidentID string) (json.RawMessage, error) {
	if len(data) == 0 || len(data) > 2<<20 {
		return nil, errors.New("native ledger evidence is empty or exceeds limit")
	}
	ledger := &contracts.BrokerLedger{}
	if err := wire.Unmarshal(data, ledger); err != nil {
		return nil, fmt.Errorf("decode native ledger: %w", err)
	}
	if ledger.GetIncidentId() != incidentID {
		return nil, errors.New("native ledger incident ID mismatch")
	}
	if ledger.GetClosure() != nil && ledger.GetClosure().GetRequest().GetIncidentId() != incidentID {
		return nil, errors.New("native closure incident ID mismatch")
	}
	return append(json.RawMessage(nil), data...), nil
}
