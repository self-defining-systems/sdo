package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/datacheck"
)

// DataCheckDetectorPrefix starts the ID of every Go-installed data-check
// detector; the rest of the ID is the check's ID.
const DataCheckDetectorPrefix = "data-"

// DataCheckDetectorID is the health detector Go installs for a data check.
func DataCheckDetectorID(check string) string { return DataCheckDetectorPrefix + check }

// DataCheckOfDetector returns the data check a Go-installed detector ID runs.
func DataCheckOfDetector(id string) (string, bool) {
	check, ok := strings.CutPrefix(id, DataCheckDetectorPrefix)
	return check, ok && datacheck.ValidID(check)
}

// prepareDataCheckDetectors installs one health detector per data check file
// (.sdo/diagnostics/data/<id>.yaml) as data-<id>, built from the file's text,
// and removes the Go-installed data-check detectors whose file is gone. The
// detector is a function of the file, so a changed file replaces the
// detector Go installed for it; a detector of that ID that Go did not
// install is never overwritten. A data-check detector is a health detector:
// incident closure waits for it like for any other.
func prepareDataCheckDetectors(root string) error {
	directory := filepath.Join(root, layout.DataChecksDir)
	entries, err := os.ReadDir(directory)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	wanted := map[string]bool{}
	for _, entry := range entries {
		kind, check, ok := layout.Classify(layout.DataChecksDir + "/" + entry.Name())
		if entry.IsDir() || !ok || kind != layout.KindDataCheck {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		parsed, err := datacheck.Parse(data)
		if err == nil {
			err = parsed.Validate(check)
		}
		if err != nil {
			return fmt.Errorf("data check %s: %w", check, err)
		}
		id := DataCheckDetectorID(check)
		if err := replaceBootstrapDetector(root, id); err != nil {
			return err
		}
		interval := parsed.ReadInterval()
		manifest := genericManifest(id, fmt.Sprintf("%ds", int(interval/time.Second)), []layout.Watch{{APIVersion: datacheck.Watch.APIVersion, Kind: datacheck.Watch.Kind}})
		if err := writeGenericDetector(root, manifest, dataCheckSource(id, check, string(data), interval)); err != nil {
			return err
		}
		wanted[id] = true
	}
	return pruneDataCheckDetectors(root, wanted)
}

// replaceBootstrapDetector clears the Go-installed package id so it can be
// written afresh; a package Go did not install is an error.
func replaceBootstrapDetector(root, id string) error {
	data, err := os.ReadFile(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, id)))
	if os.IsNotExist(err) {
		if _, statErr := os.Stat(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, id))); os.IsNotExist(statErr) {
			return nil
		}
	}
	if err == nil {
		if manifest, parseErr := layout.ParseDetectorManifest(data); parseErr == nil && manifest.OriginatingCommit == "lifecycle-bootstrap" {
			return removeBootstrapDetector(root, id)
		}
	}
	return fmt.Errorf("health detector %s is not Go's data-check detector; rename the data check or that detector", id)
}

// pruneDataCheckDetectors removes Go-installed data-check packages no data
// check file backs. Authored data-* packages are never touched.
func pruneDataCheckDetectors(root string, wanted map[string]bool) error {
	entries, err := os.ReadDir(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "")))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, ok := DataCheckOfDetector(entry.Name()); !entry.IsDir() || !ok || wanted[entry.Name()] {
			continue
		}
		if err := removeBootstrapDetector(root, entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

// dataCheckSource embeds the check's text in the detector, so the detector is
// bound to the accepted file it was built from.
func dataCheckSource(id, check, text string, interval time.Duration) string {
	return fmt.Sprintf(`package synthetic

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/datacheck"
)

func New() sdk.Detector {
	return datacheck.NewDetector(sdk.DetectorSpec{
		%s
		Watches: []sdk.WatchKind{datacheck.Watch},
		Interval: %d * time.Second,
	}, %q, %q)
}
`, genericSpec(id), int(interval/time.Second), check, text)
}
