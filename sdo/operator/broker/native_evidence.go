package broker

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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

// ValidateNativeLedger rejects a mismatched incident before evidence leaves
// broker storage. The payload remains raw for benchmark-specific analysis.
func ValidateNativeLedger(data []byte, incidentID string) (json.RawMessage, error) {
	if len(data) == 0 || len(data) > 2<<20 {
		return nil, errors.New("native ledger evidence is empty or exceeds limit")
	}
	var record struct {
		IncidentID string          `json:"incident_id"`
		Closure    json.RawMessage `json:"closure"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decode native ledger: %w", err)
	}
	if record.IncidentID != incidentID {
		return nil, errors.New("native ledger incident ID mismatch")
	}
	if len(record.Closure) > 0 {
		var closure struct {
			Request struct {
				IncidentID string `json:"incident_id"`
			} `json:"request"`
		}
		if err := json.Unmarshal(record.Closure, &closure); err != nil {
			return nil, fmt.Errorf("decode native closure: %w", err)
		}
		if closure.Request.IncidentID != incidentID {
			return nil, errors.New("native closure incident ID mismatch")
		}
	}
	return append(json.RawMessage(nil), data...), nil
}
