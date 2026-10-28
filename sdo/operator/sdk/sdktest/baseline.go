package sdktest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
)

// LoadSnapshots reads a recorded cluster state from dir: every *.json file is
// one Snapshot (see the json tags on Snapshot), returned in file-name order.
// An empty directory or a file with unknown fields is an error, so a baseline
// that silently stops checking anything cannot pass.
func LoadSnapshots(dir string) ([]Snapshot, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.json snapshot files in %s", dir)
	}
	snapshots := make([]Snapshot, 0, len(paths))
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		snapshot, err := decodeSnapshotStrict(raw)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", filepath.Base(path), err)
		}
		if err := snapshot.ValidateObservation(); err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", filepath.Base(path), err)
		}
		snapshot.SourceName = filepath.Base(path)
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

// HealthyBaselineViolations replays the detector on recorded snapshots of the
// healthy application and returns actionable messages for active findings and
// unavailable observations, using the production observation checks.
// The snapshots are healthy by construction, so any active finding is a false
// positive that would open an incident before any fault exists. An error cannot
// prove quietness and is also a violation. Resolved findings are ignored.
func HealthyBaselineViolations(detector sdk.Detector, snapshots []Snapshot) []string {
	spec := detector.Spec()
	violations := make([]string, 0)
	seen := map[string]bool{}
	for _, snapshot := range snapshots {
		findings, err := sdk.DetectObserved(context.Background(), detector, snapshot)
		if err != nil {
			message := fmt.Sprintf("detector %q failed on healthy baseline snapshot %s (namespace %q): %v; every located binding must have available evidence and a conclusive diagnosis", spec.ID, snapshot.SourceName, snapshot.Namespace(), err)
			if !seen[message] {
				seen[message] = true
				violations = append(violations, message)
			}
			continue
		}
		for _, finding := range findings {
			if finding.Status != sdk.FindingActive {
				continue
			}
			message := fmt.Sprintf(
				"detector %q reported an active finding on healthy baseline snapshot %s (namespace %q): rule %q on %s %s/%s: %s; %s",
				spec.ID, snapshot.SourceName, snapshot.Namespace(), finding.RuleID, finding.PrimaryResource.Kind,
				finding.PrimaryResource.Namespace, finding.PrimaryResource.Name,
				strings.TrimSpace(finding.Summary), healthyBaselineRemedy,
			)
			if !seen[message] {
				seen[message] = true
				violations = append(violations, message)
			}
		}
	}
	return violations
}

const healthyBaselineRemedy = "the healthy baseline is a recorded cluster state with no fault, so an incident " +
	"detector must stay quiet on it; key the predicate on the fault condition itself (a state the healthy " +
	"application never has) instead of a shape that ordinary healthy resources also have"
