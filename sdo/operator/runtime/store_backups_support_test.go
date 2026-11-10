package runtime

import (
	"fmt"
	"path/filepath"
	"sync"

	"sdo.dev/operator/sdk/program"
)

// memoryBackupStore keeps backups in process, for tests and simulation.
type memoryBackupStore struct {
	mu    sync.Mutex
	items map[string]memoryBackup
}

type memoryBackup struct {
	record program.StoreBackupRecord
	dump   []byte
}

func (s *memoryBackupStore) Put(record program.StoreBackupRecord, dump []byte) (program.StoreBackupRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = map[string]memoryBackup{}
	}
	key := record.IncidentID + "/" + record.ID
	if _, ok := s.items[key]; ok {
		return record, fmt.Errorf("backup %s already exists in this incident; backups are immutable, choose a new ID", record.ID)
	}
	record.Path = filepath.Join(incidentDirectory(record.IncidentID), record.ID, "dump")
	s.items[key] = memoryBackup{record: record, dump: append([]byte(nil), dump...)}
	return record, nil
}

func (s *memoryBackupStore) Get(incident, id string) (program.StoreBackupRecord, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[incident+"/"+id]
	if !ok {
		return program.StoreBackupRecord{}, nil, errBackupMissing
	}
	return item.record, item.dump, checkBackup(item.record, item.dump)
}
