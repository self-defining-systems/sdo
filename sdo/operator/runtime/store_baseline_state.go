package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// storeBaselineStateVersion names the persisted record's format; another
// version is discarded and the stores are baselined again.
const storeBaselineStateVersion = "sdo.store-baselines/v2"

// StoreBaselineStatePath is the controller's record of its promoted store
// baselines: per-cluster controller state below the lane-private repository
// mount (beside the reachability baseline), outside every operational-memory
// tree and outside the evidence allowlist.
func StoreBaselineStatePath(stateRoot string) string {
	return filepath.Join(stateRoot, ".sdo-runtime", "store-baselines.json")
}

// StoreBaselineState persists the promoted store-state baselines of a
// PodReadBaseline so a restarted controller keeps them. Only store-state reads
// (the Go-installed, value-free storestate queries) are recorded; restore text
// (StorePayloads) is never written and is captured again from the live store.
//
// A restarted controller keeps a record under the rule a running controller
// keeps its in-memory reference: the last promoted healthy reading of the same
// query declaration stays the reference until a settled healthy stretch
// replaces it. An accepted change (new source inputs, an upgrade that replaced
// the store Pods) therefore never blinds the store detector right after the
// change; what the change did to the store shows as clues against the earlier
// healthy reference until the new deployment's first settled stretch.
type StoreBaselineState struct {
	// Path is the record (StoreBaselineStatePath).
	Path string
}

// storeBaselineRecord is the persisted form. Results hold each store query's
// promoted readings (Query and Readings only), History its settled healthy
// readings.
type storeBaselineRecord struct {
	Version string                    `json:"version"`
	Results map[string]podread.Result `json:"results"`
	History map[string]podread.Result `json:"history,omitempty"`
}

// restoredStore is one persisted store baseline awaiting its first current
// reading, which decides whether it still describes the store.
type restoredStore struct {
	result, history podread.Result
}

// Restore binds the holder to state and loads the record a previous
// controller wrote. Each store baseline is adopted on the first current
// reading of its query when that reading has the same query declaration
// (adoptRestored). Unreadable state is recomputable, so it is reported to
// onError and the holder starts empty.
func (b *PodReadBaseline) Restore(state *StoreBaselineState, onError func(error)) {
	if b == nil || state == nil {
		return
	}
	b.mu.Lock()
	b.State = state
	b.mu.Unlock()
	record, err := readStoreBaselineState(state.Path)
	if err != nil {
		if onError != nil {
			onError(fmt.Errorf("discarding unreadable store baseline state: %w", err))
		}
		return
	}
	if record == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, result := range record.Results {
		if _, _, ok := storestate.ParseQueryID(id); !ok || len(result.Readings) == 0 {
			continue
		}
		if b.restored == nil {
			b.restored = map[string]restoredStore{}
		}
		b.restored[id] = restoredStore{result: result, history: record.History[id]}
	}
}

func readStoreBaselineState(path string) (*storeBaselineRecord, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record storeBaselineRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("%s has trailing data", path)
	}
	if record.Version != storeBaselineStateVersion {
		return nil, fmt.Errorf("%s has version %q, want %q", path, record.Version, storeBaselineStateVersion)
	}
	return &record, nil
}

// adoptRestored decides each restored store baseline whose query has a
// current reading: it becomes the promoted reference when the current result
// is the same query declaration, and is discarded otherwise, the rule attach
// applies to an in-memory reference. A reference promoted since the restart
// wins. Restore text is captured from the store Pods the current reading
// names, which may have replaced the baseline's; each entry's fact value
// guards its use. Undecided entries (the query not read yet, or the read
// failed as a whole) wait for a later reading.
func (b *PodReadBaseline) adoptRestored(results map[string]podread.Result) {
	b.mu.Lock()
	if len(b.restored) == 0 {
		b.mu.Unlock()
		return
	}
	capture := map[string]podread.Result{}
	for id, restored := range b.restored {
		current, ok := results[id]
		if !ok || current.Error != "" || len(current.Readings) == 0 {
			continue
		}
		delete(b.restored, id)
		_, component, _ := storestate.ParseQueryID(id)
		if !reflect.DeepEqual(restored.result.Query, current.Query) {
			slog.Info("store_baseline_discarded", "component", component, "reason", "query declaration changed since the baseline was promoted")
			continue
		}
		if _, promoted := b.results[id]; promoted {
			continue
		}
		b.results[id] = restored.result
		if _, ok := b.history[id]; !ok && reflect.DeepEqual(restored.history.Query, restored.result.Query) {
			b.history[id] = restored.history
		}
		capture[id] = current
		// Harnesses that inject only once every store has a reference wait for this line.
		slog.Info("store_baseline_promoted", "component", component, "restored", true)
	}
	b.mu.Unlock()
	b.Payloads.promoted(capture)
}

// persist writes the promoted store-state baselines. A failure is logged: the
// record is recomputable and only saves a restarted controller the settle
// wait.
func (b *PodReadBaseline) persist() {
	b.mu.RLock()
	state := b.State
	record := storeBaselineRecord{Version: storeBaselineStateVersion, Results: map[string]podread.Result{}, History: map[string]podread.Result{}}
	for _, id := range slices.Sorted(maps.Keys(b.results)) {
		if _, _, ok := storestate.ParseQueryID(id); !ok {
			continue
		}
		record.Results[id] = podread.Result{Query: b.results[id].Query, Readings: b.results[id].Readings}
		if history, ok := b.history[id]; ok {
			record.History[id] = podread.Result{Query: history.Query, Readings: history.Readings}
		}
	}
	b.mu.RUnlock()
	if state == nil || len(record.Results) == 0 {
		return
	}
	if err := writeStoreBaselineState(state.Path, record); err != nil {
		slog.Warn("store_baseline_persist_failed", "error", err)
	}
}

func writeStoreBaselineState(path string, record storeBaselineRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".store-baselines-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(append(data, '\n')); err != nil {
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
	return os.Rename(file.Name(), path)
}
