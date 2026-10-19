package layout

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// legacyDetector is a schema-version 1 registration: a per-detector manifest
// plus the package directory that version 2 derives from the detector's ID.
type legacyDetector struct {
	DetectorManifest `yaml:",inline"`
	Package          string `yaml:"package"`
}

type legacyIndex struct {
	APIVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	SDKVersion string           `yaml:"sdkVersion"`
	Detectors  []legacyDetector `yaml:"detectors"`
}

type migrationStep struct {
	from, to string
	manifest DetectorManifest
	class    Class
}

// Migrate brings the operational memory checked out at root to
// CurrentSchemaVersion and reports whether it changed anything. It is the
// operation the Upkeep actor commits for existing repositories.
//
// Schema version 1 kept every detector in .sdo/diagnostics/manifest.yaml.
// Version 2 gives each detector <detector dir>/manifest.yaml. Migration splits
// the shared manifest, moves a package whose directory differs from its detector
// ID to the canonical directory, removes the shared manifest, and stamps the new schema version. It plans
// every step before touching the checkout, so an ambiguous repository is left
// unchanged.
func Migrate(root string) (bool, error) {
	version, err := readSchemaVersion(root)
	if err != nil {
		return false, err
	}
	legacyPath := filepath.Join(root, filepath.FromSlash(LegacyDetectorManifestFile))
	legacyData, legacyErr := os.ReadFile(legacyPath)
	hasLegacy := legacyErr == nil
	if legacyErr != nil && !errors.Is(legacyErr, fs.ErrNotExist) {
		return false, fmt.Errorf("read legacy detector manifest: %w", legacyErr)
	}
	switch {
	case version == CurrentSchemaVersion && !hasLegacy:
		return false, nil
	case version == CurrentSchemaVersion:
		return false, errors.New("schema version 2 repository still has a legacy detector manifest")
	case version != 0 && version != 1:
		return false, fmt.Errorf("cannot migrate operational memory schema version %d", version)
	}
	var steps []migrationStep
	if hasLegacy {
		if steps, err = planMigration(root, legacyData); err != nil {
			return false, err
		}
	}
	for _, step := range steps {
		if step.from != step.to {
			if err := os.MkdirAll(filepath.Dir(step.to), 0o755); err != nil {
				return false, err
			}
			if err := os.Rename(step.from, step.to); err != nil {
				return false, fmt.Errorf("move detector %s: %w", step.manifest.ID, err)
			}
		}
		if err := WriteDetector(root, Detector{Class: step.class, Manifest: step.manifest}); err != nil {
			return false, err
		}
	}
	if hasLegacy {
		if err := os.Remove(legacyPath); err != nil {
			return false, err
		}
	}
	target := filepath.Join(root, filepath.FromSlash(SchemaVersionFile))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return false, err
	}
	return true, os.WriteFile(target, []byte(strconv.Itoa(CurrentSchemaVersion)+"\n"), 0o644)
}

func readSchemaVersion(root string) (int, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(SchemaVersionFile)))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid schema-version %q", strings.TrimSpace(string(data)))
	}
	return version, nil
}

// LegacyDetectors returns the registrations of a schema-version 1 repository's
// shared detector manifest, with the current header, as migration would write
// them. It reports no detectors when the repository has no shared manifest.
func LegacyDetectors(root string) ([]DetectorManifest, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(LegacyDetectorManifestFile)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	legacy, err := parseLegacy(data)
	if err != nil {
		return nil, err
	}
	manifests := make([]DetectorManifest, 0, len(legacy))
	for _, detector := range legacy {
		manifests = append(manifests, currentHeader(detector.DetectorManifest))
	}
	return manifests, nil
}

func currentHeader(manifest DetectorManifest) DetectorManifest {
	manifest.APIVersion, manifest.Kind, manifest.SDKVersion = APIVersion, DetectorManifestKind, SDKVersion
	return manifest
}

func parseLegacy(data []byte) ([]legacyDetector, error) {
	if err := RejectDuplicateKeys(data); err != nil {
		return nil, err
	}
	var legacy legacyIndex
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&legacy); err != nil {
		return nil, fmt.Errorf("legacy detector manifest: %w", err)
	}
	if legacy.APIVersion != APIVersion || legacy.Kind != DetectorManifestKind {
		return nil, errors.New("legacy detector manifest has an invalid header")
	}
	return legacy.Detectors, nil
}

func planMigration(root string, data []byte) ([]migrationStep, error) {
	legacy, err := parseLegacy(data)
	if err != nil {
		return nil, err
	}
	var steps []migrationStep
	seen := map[string]bool{}
	for _, detector := range legacy {
		class := detector.DetectorClass()
		if !class.valid() || !ValidDetectorID(detector.ID) {
			return nil, fmt.Errorf("legacy detector %q has invalid class %q or id", detector.ID, detector.Class)
		}
		key := class.Dir() + "/" + detector.ID
		if seen[key] {
			return nil, fmt.Errorf("legacy detector %q is registered twice", detector.ID)
		}
		seen[key] = true
		legacyPackage := path.Clean(strings.TrimPrefix(detector.Package, "./"))
		if !strings.HasPrefix(legacyPackage, "detectors/"+class.Dir()+"/") || strings.Contains(legacyPackage, "..") {
			return nil, fmt.Errorf("legacy detector %s package %q is outside its owned directory", detector.ID, detector.Package)
		}
		from := filepath.Join(root, filepath.FromSlash(DiagnosticsDir), filepath.FromSlash(legacyPackage))
		to := filepath.Join(root, filepath.FromSlash(DetectorDir(class, detector.ID)))
		if info, err := os.Lstat(from); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("legacy detector %s package directory %s is missing", detector.ID, legacyPackage)
		}
		if from != to {
			if _, err := os.Lstat(to); err == nil {
				return nil, fmt.Errorf("cannot move detector %s: %s already exists", detector.ID, DetectorDir(class, detector.ID))
			}
		}
		manifest := currentHeader(detector.DetectorManifest)
		steps = append(steps, migrationStep{from: from, to: to, manifest: manifest, class: class})
	}
	return steps, nil
}
