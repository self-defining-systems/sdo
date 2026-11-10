package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"sdo.dev/operator/sdk/program"
)

// Backup bounds. One backup holds at most maxBackupBytes; one incident's
// backups together at most maxIncidentBackupBytes. A larger scope needs a
// narrower selector.
const (
	maxBackupBytes         = 64 << 20
	maxIncidentBackupBytes = 256 << 20
)

// errBackupMissing is a backup ID this incident never recorded.
var errBackupMissing = errors.New("no such backup in this incident")

// BackupStore keeps the dumps StoreBackup takes, per incident. It is
// controller state: never under .sdo/, never published, never learned from.
type BackupStore interface {
	// Put stores one verified dump and its record and returns the record
	// with its Path. It refuses an ID the incident already used and a dump
	// beyond the per-incident cap.
	Put(record program.StoreBackupRecord, dump []byte) (program.StoreBackupRecord, error)
	// Get returns one record and its dump after checking the dump's digest.
	Get(incident, id string) (program.StoreBackupRecord, []byte, error)
}

// FileBackupStore keeps backups under Root/<sha256(incident)>/<id>/. In the
// controller Root is .sdo-runtime/backups on the repository volume: a PVC that
// outlives the controller Pod, so a restore still works after a restart, and
// outside the .sdo/ tree the broker publishes.
type FileBackupStore struct {
	Root string
}

func incidentDirectory(incident string) string {
	sum := sha256.Sum256([]byte(incident))
	return hex.EncodeToString(sum[:])
}

func (s FileBackupStore) Put(record program.StoreBackupRecord, dump []byte) (program.StoreBackupRecord, error) {
	if s.Root == "" || record.IncidentID == "" || record.ID == "" {
		return record, fmt.Errorf("backup storage requires a root, an incident and an ID")
	}
	incident := filepath.Join(s.Root, incidentDirectory(record.IncidentID))
	if err := os.MkdirAll(incident, 0o700); err != nil {
		return record, fmt.Errorf("backup storage unavailable: %w", err)
	}
	final := filepath.Join(incident, record.ID)
	if _, err := os.Stat(final); err == nil {
		return record, fmt.Errorf("backup %s already exists in this incident; backups are immutable, choose a new ID", record.ID)
	}
	used, err := s.incidentBytes(incident)
	if err != nil {
		return record, err
	}
	if used+int64(len(dump)) > maxIncidentBackupBytes {
		return record, fmt.Errorf("backup %s (%d bytes) would exceed this incident's backup cap of %d bytes (%d used)", record.ID, len(dump), maxIncidentBackupBytes, used)
	}
	record.Path = filepath.Join(incidentDirectory(record.IncidentID), record.ID, "dump")
	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return record, err
	}
	staging, err := os.MkdirTemp(incident, ".staging-")
	if err != nil {
		return record, fmt.Errorf("backup storage unavailable: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err := os.WriteFile(filepath.Join(staging, "dump"), dump, 0o600); err != nil {
		return record, fmt.Errorf("backup write failed: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, "record.json"), encoded, 0o600); err != nil {
		return record, fmt.Errorf("backup write failed: %w", err)
	}
	if err := os.Rename(staging, final); err != nil {
		return record, fmt.Errorf("backup write failed: %w", err)
	}
	return record, nil
}

func (s FileBackupStore) incidentBytes(incident string) (int64, error) {
	entries, err := os.ReadDir(incident)
	if err != nil {
		return 0, fmt.Errorf("backup storage unavailable: %w", err)
	}
	var used int64
	for _, entry := range entries {
		info, err := os.Stat(filepath.Join(incident, entry.Name(), "dump"))
		if err == nil {
			used += info.Size()
		}
	}
	return used, nil
}

func (s FileBackupStore) Get(incident, id string) (program.StoreBackupRecord, []byte, error) {
	var record program.StoreBackupRecord
	if s.Root == "" || incident == "" || id == "" || filepath.Base(id) != id {
		return record, nil, errBackupMissing
	}
	directory := filepath.Join(s.Root, incidentDirectory(incident), id)
	encoded, err := os.ReadFile(filepath.Join(directory, "record.json"))
	if errors.Is(err, os.ErrNotExist) {
		return record, nil, errBackupMissing
	}
	if err != nil {
		return record, nil, fmt.Errorf("backup %s unreadable: %w", id, err)
	}
	if err := json.Unmarshal(encoded, &record); err != nil {
		return record, nil, fmt.Errorf("backup %s record unreadable: %w", id, err)
	}
	dump, err := os.ReadFile(filepath.Join(directory, "dump"))
	if err != nil {
		return record, nil, fmt.Errorf("backup %s dump unreadable: %w", id, err)
	}
	return record, dump, checkBackup(record, dump)
}

func checkBackup(record program.StoreBackupRecord, dump []byte) error {
	sum := sha256.Sum256(dump)
	if hex.EncodeToString(sum[:]) != record.SHA256 || int64(len(dump)) != record.Bytes {
		return fmt.Errorf("backup %s does not match its recorded size and SHA-256; it cannot be trusted", record.ID)
	}
	return nil
}
