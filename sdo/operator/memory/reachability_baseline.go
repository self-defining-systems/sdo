package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/servicehealth"
)

// ReachabilityBaselinePath is health-judge-owned diagnostic data, alongside
// the immutable generic detector policy. It contains no executable code.
const ReachabilityBaselinePath = ".sdo/diagnostics/detectors/health/service-reachability/baseline.json"

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

// ReadReachabilityBaseline reads only an immutable accepted revision. A missing
// record preserves ordinary reachability detection; malformed data fails closed.
func ReadReachabilityBaseline(ctx context.Context, root, revision string) (*servicehealth.ReachabilityBaseline, error) {
	if _, err := healthGit(ctx, root, "cat-file", "-e", revision+":"+ReachabilityBaselinePath); err != nil {
		// Distinguish absent data from a missing/unreadable accepted revision.
		if _, headErr := healthGit(ctx, root, "rev-parse", "--verify", revision+"^{commit}"); headErr != nil {
			return nil, headErr
		}
		return nil, nil
	}
	data, err := healthGit(ctx, root, "show", revision+":"+ReachabilityBaselinePath)
	if err != nil {
		return nil, err
	}
	return DecodeReachabilityBaseline(data)
}

// WriteReachabilityBaseline writes an isolated health-judge proposal only.
// The caller must publish it through the broker; accepted repositories are never
// passed here by the runtime baseline gate.
func WriteReachabilityBaseline(root string, b *servicehealth.ReachabilityBaseline) error {
	if err := b.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(root, ReachabilityBaselinePath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
