package runtime

import (
	"bytes"
	"context"
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
	"sync"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// storeBaselineStateVersion names the persisted record's format; another
// version is discarded and the stores are baselined again.
const storeBaselineStateVersion = "sdo.store-baselines/v1"

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
type StoreBaselineState struct {
	// Path is the record (StoreBaselineStatePath).
	Path string
	// SourceInputs returns the accepted source inputs now
	// (memory.HealthRevision.SourceInputs: everything a build or deployment
	// consumes, without operational memory). A record bound to other inputs
	// describes another deployment and is discarded.
	SourceInputs func() (string, error)
}

// storeBaselineRecord is the persisted form. Results hold each store query's
// promoted readings (Query and Readings only), History its settled healthy
// readings.
type storeBaselineRecord struct {
	Version      string                    `json:"version"`
	SourceInputs string                    `json:"source_inputs"`
	Results      map[string]podread.Result `json:"results"`
	History      map[string]podread.Result `json:"history,omitempty"`
}

// restoredStore is one persisted store baseline awaiting its first current
// reading, which decides whether it still describes the store.
type restoredStore struct {
	result, history podread.Result
}

// Restore binds the holder to state and loads the record a previous
// controller wrote. A record bound to other accepted source inputs is
// discarded at once; each remaining store baseline is adopted on the first
// current reading of its query only when that reading is of the very same
// store Pods (adoptRestored). Unreadable state is recomputable, so it is
// reported to onError and the holder starts empty.
func (b *PodReadBaseline) Restore(state *StoreBaselineState, onError func(error)) {
	if b == nil || state == nil {
		return
	}
	b.mu.Lock()
	b.State = state
	b.mu.Unlock()
	record, err := readStoreBaselineState(state.Path)
	if err == nil && record != nil {
		var inputs string
		if inputs, err = state.SourceInputs(); err == nil && inputs != record.SourceInputs {
			slog.Info("store_baseline_discarded", "reason", "accepted source inputs changed", "stores", len(record.Results))
			record = nil
		}
	}
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
// is the same query declaration over the same store Pods (every Pod's name and
// UID, none missing and none added), and is discarded otherwise. A reference
// promoted since the restart wins. Undecided entries (the query not read yet,
// or the read failed as a whole) wait for a later reading.
func (b *PodReadBaseline) adoptRestored(results map[string]podread.Result) {
	b.mu.Lock()
	if len(b.restored) == 0 {
		b.mu.Unlock()
		return
	}
	adopted := map[string]podread.Result{}
	for id, restored := range b.restored {
		current, ok := results[id]
		if !ok || current.Error != "" || len(current.Readings) == 0 {
			continue
		}
		delete(b.restored, id)
		_, component, _ := storestate.ParseQueryID(id)
		if !reflect.DeepEqual(restored.result.Query, current.Query) || !sameStorePods(restored.result.Readings, current.Readings) {
			slog.Info("store_baseline_discarded", "component", component, "reason", "store Pods or query changed since the baseline was promoted")
			continue
		}
		if _, promoted := b.results[id]; promoted {
			continue
		}
		b.results[id] = restored.result
		if _, ok := b.history[id]; !ok && reflect.DeepEqual(restored.history.Query, restored.result.Query) {
			b.history[id] = restored.history
		}
		adopted[id] = restored.result
		// Harnesses that inject only once every store has a reference wait for this line.
		slog.Info("store_baseline_promoted", "component", component, "restored", true)
	}
	b.mu.Unlock()
	b.Payloads.promoted(adopted)
}

// sameStorePods reports whether current reads exactly the Pods the baseline
// was taken from, each with a known and unchanged UID.
func sameStorePods(baseline, current []podread.Reading) bool {
	pods := map[string]string{}
	for _, r := range baseline {
		if r.UID == "" {
			return false
		}
		pods[r.Pod] = r.UID
	}
	seen := map[string]bool{}
	for _, r := range current {
		if r.UID == "" || pods[r.Pod] != r.UID {
			return false
		}
		seen[r.Pod] = true
	}
	return len(seen) == len(pods)
}

// persist writes the promoted store-state baselines, bound to the accepted
// source inputs now. A failure is logged: the record is recomputable and only
// saves a restarted controller the settle wait.
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
	var err error
	if record.SourceInputs, err = state.SourceInputs(); err == nil {
		err = writeStoreBaselineState(state.Path, record)
	}
	if err != nil {
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

// acceptedSourceInputs returns the source inputs (memory.SourceInputs) of the
// accepted commit at each call, computed once per commit.
func acceptedSourceInputs(ctx context.Context, root string, commit func() string) func() (string, error) {
	var mu sync.Mutex
	var cachedCommit, cachedInputs string
	return func() (string, error) {
		current := commit()
		mu.Lock()
		defer mu.Unlock()
		if current != cachedCommit || cachedInputs == "" {
			inputs, err := memory.SourceInputs(ctx, root, current)
			if err != nil {
				return "", fmt.Errorf("source inputs of %s: %w", current, err)
			}
			cachedCommit, cachedInputs = current, inputs
		}
		return cachedInputs, nil
	}
}
