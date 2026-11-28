package builder

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// findingAdmissionFixtures holds one directory per captured state the
// finding-admission test evaluates learned detectors on.
const findingAdmissionFixtures = "finding-admission"

// FindingAdmissionRule names a generated finding-admission failure line.
const FindingAdmissionRule = "detector.finding-admission"

const findingAdmissionTest = `package generated

import (
    "os"
    "path/filepath"
    "testing"
    "sdo.dev/operator/core"
    "sdo.dev/operator/sdk"
    "sdo.dev/operator/sdk/sdktest"
)

func TestLearnedDetectorFindingsAreAdmitted(t *testing.T) {
    dirs, _ := filepath.Glob("testdata/` + findingAdmissionFixtures + `/*")
    var snapshots []core.NamedSnapshot
    for _, dir := range dirs {
        if info, err := os.Stat(dir); err != nil || !info.IsDir() { continue }
        loaded, err := sdktest.LoadSnapshots(dir)
        if err != nil { t.Logf("capture %s not evaluated: %v", filepath.Base(dir), err); continue }
        for _, snapshot := range loaded { snapshots = append(snapshots, core.NamedSnapshot{Name: snapshot.SourceName, View: snapshot}) }
    }
    for _, detector := range All() {
        if detector.Spec().Class != sdk.DetectorClassLearned { continue }
        for _, violation := range core.FindingAdmissionViolations(detector, snapshots) {
            t.Error("` + FindingAdmissionRule + `[" + detector.Spec().ID + "]: " + violation)
        }
    }
}
`

// generateFindingAdmissionTest stages the incident's captured states (its
// open@* snapshots and the recovered one), each in its own directory, and a
// test that passes every finding a learned detector returns on them through
// the controller's own admission. A finding the controller would refuse every
// cycle fails the check at publication. The healthy baseline is not staged:
// any finding there is already the healthy-baseline gate's, which separates
// faults the accepted base already had. With no capture it writes nothing.
func generateFindingAdmissionTest(capture, root string) error {
	var paths []string
	if capture != "" {
		opens, err := filepath.Glob(filepath.Join(capture, "open@*.json"))
		if err != nil {
			return err
		}
		for _, path := range opens {
			if !strings.HasSuffix(path, "-recordings.json") {
				paths = append(paths, path)
			}
		}
		if _, err := os.Stat(filepath.Join(capture, "fixed.json")); err == nil {
			paths = append(paths, filepath.Join(capture, "fixed.json"))
		}
	}
	if len(paths) == 0 {
		return nil
	}
	for index, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue // an unreadable state is not evaluated; other checks report it
		}
		dir := filepath.Join(root, "generated", "testdata", findingAdmissionFixtures, fmt.Sprintf("%03d", index))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(path)), data, 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(root, "generated", "finding_admission_test.go"), []byte(findingAdmissionTest), 0o644)
}
