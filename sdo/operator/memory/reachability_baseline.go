package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/servicehealth"
)

// LegacyReachabilityBaselinePath held one cluster's reachability capture inside
// accepted memory. Captures are per-cluster controller state now: readers ignore
// this file, health preparation removes it, and the validator rejects writing it.
const LegacyReachabilityBaselinePath = ".sdo/diagnostics/detectors/health/service-reachability/baseline.json"

// ReachabilityBaselineStatePath is per-cluster controller state below the
// lane-private repository mount, outside every operational-memory tree. Each
// cluster measures its own record; shared memory never carries it.
func ReachabilityBaselineStatePath(stateRoot string) string {
	return filepath.Join(stateRoot, ".sdo-runtime", "reachability-baseline.json")
}

func DecodeReachabilityBaseline(data []byte) (*servicehealth.ReachabilityBaseline, error) {
	var b servicehealth.ReachabilityBaseline
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&b); err != nil {
		return nil, fmt.Errorf("decode reachability baseline: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("reachability baseline has trailing data")
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	return &b, nil
}

// ReadReachabilityBaselineState reads this cluster's record. A missing record
// preserves ordinary reachability detection; malformed data is an error.
func ReadReachabilityBaselineState(stateRoot string) (*servicehealth.ReachabilityBaseline, error) {
	data, err := os.ReadFile(ReachabilityBaselineStatePath(stateRoot))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return DecodeReachabilityBaseline(data)
}

// WriteReachabilityBaselineState atomically replaces this cluster's record.
func WriteReachabilityBaselineState(stateRoot string, b *servicehealth.ReachabilityBaseline) error {
	if err := b.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	path := ReachabilityBaselineStatePath(stateRoot)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".reachability-baseline-*")
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
