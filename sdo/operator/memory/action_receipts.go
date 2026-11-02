package memory

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// IncidentActionReceipts reads the same append-only command journal the worker
// merges at return. A missing final report never erases completed receipts.
func IncidentActionReceipts(repositoryMount, incidentID string) ([]*contracts.RepairActionReceipt, bool, string, error) {
	relative, err := PrepareIncidentSession(repositoryMount, "", incidentID)
	if err != nil {
		return nil, true, "", err
	}
	f, err := os.Open(filepath.Join(repositoryMount, relative, "scratch", "repair-actions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, "", nil
	}
	if err != nil {
		return nil, true, "", err
	}
	defer f.Close()
	hash := sha256.New()
	reader := io.TeeReader(io.LimitReader(f, 4<<20+1), hash)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	type row struct {
		State   string          `json:"state"`
		Receipt json.RawMessage `json:"receipt"`
	}
	rows := map[string]*contracts.RepairActionReceipt{}
	states := map[string]string{}
	order := []string{}
	for scanner.Scan() {
		var entry row
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, true, "", err
		}
		receipt := new(contracts.RepairActionReceipt)
		if err := wire.UnmarshalDurable(entry.Receipt, receipt); err != nil {
			return nil, true, "", err
		}
		// The journal is agent-writable: owner chains come only from SDO's
		// own observations, never this file.
		receipt.Owners = nil
		if states[receipt.ActionId] == "" {
			order = append(order, receipt.ActionId)
		}
		rows[receipt.ActionId] = receipt
		states[receipt.ActionId] = entry.State
	}
	if err := scanner.Err(); err != nil {
		return nil, true, "", err
	}
	// Exactly bounded files are fine; an over-budget journal cannot prove safety.
	if info, err := f.Stat(); err != nil || info.Size() > 4<<20 {
		return nil, true, "", fmt.Errorf("action journal exceeds observation budget")
	}
	result := []*contracts.RepairActionReceipt{}
	pending := false
	for _, id := range order {
		receipt := rows[id]
		pending = pending || states[id] != "finished" || receipt.FailureReason == "outcome_unknown"
		result = append(result, receipt)
	}
	return result, pending, fmt.Sprintf("%x", hash.Sum(nil)), nil
}
