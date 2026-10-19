package layout

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
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
// ID to the canonical directory, and removes the shared manifest.
//
// Version 3 refers to playbooks only by logical ID. Migration rewrites physical
// playbook paths in detector Go source and manifests to IDs (see
// RewritePlaybookReferences) and stamps the schema version. Every step is planned,
// including all rewritten contents, before the checkout is touched, so an
// ambiguous repository is left unchanged.
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
	case version >= 2 && hasLegacy:
		return false, fmt.Errorf("schema version %d repository still has a legacy detector manifest", version)
	case version == CurrentSchemaVersion:
		return false, nil
	case version < 0 || version > CurrentSchemaVersion:
		return false, fmt.Errorf("cannot migrate operational memory schema version %d", version)
	}
	var steps []migrationStep
	if hasLegacy {
		if steps, err = planMigration(root, legacyData); err != nil {
			return false, err
		}
	}
	rewrites, err := planPlaybookRewrites(root, steps)
	if err != nil {
		return false, err
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
	for _, rewrite := range rewrites {
		if err := os.WriteFile(rewrite.path, rewrite.data, rewrite.mode); err != nil {
			return false, err
		}
	}
	target := filepath.Join(root, filepath.FromSlash(SchemaVersionFile))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return false, err
	}
	return true, os.WriteFile(target, []byte(strconv.Itoa(CurrentSchemaVersion)+"\n"), 0o644)
}

type fileRewrite struct {
	path string
	data []byte
	mode fs.FileMode
}

var (
	quotedPlaybookReadme = regexp.MustCompile(`"\.sdo/playbooks/([a-z][a-z0-9_-]*)/README\.md"`)
	rawPlaybookReadme    = regexp.MustCompile("`\\.sdo/playbooks/([a-z][a-z0-9_-]*)/README\\.md`")
)

// RewritePlaybookReferences replaces physical playbook paths in detector source
// with logical references: a string literal holding exactly a playbook README
// path becomes the playbook ID, any other embedded README path becomes the ID,
// and an embedded script path becomes script:<id>/<name>. Other physical paths
// are left for `memory check` to report.
func RewritePlaybookReferences(source string) string {
	source = quotedPlaybookReadme.ReplaceAllString(source, `"$1"`)
	source = rawPlaybookReadme.ReplaceAllString(source, "`$1`")
	source = embeddedScriptPath.ReplaceAllString(source, "script:$1/$2")
	return embeddedPlaybookPath.ReplaceAllString(source, "$1")
}

// planPlaybookRewrites computes the version 3 contents of every detector
// manifest and Go source file. Detectors that version 1 migration moves are read
// from their current location and written to their final one.
func planPlaybookRewrites(root string, steps []migrationStep) ([]fileRewrite, error) {
	type location struct{ from, to string }
	var locations []location
	if len(steps) > 0 {
		for _, step := range steps {
			locations = append(locations, location{step.from, step.to})
		}
	} else {
		for _, class := range []Class{ClassHealth, ClassIncident} {
			parent := filepath.Join(root, filepath.FromSlash(DetectorsDir), class.Dir())
			entries, err := os.ReadDir(parent)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			for _, entry := range entries {
				dir := filepath.Join(parent, entry.Name())
				locations = append(locations, location{dir, dir})
			}
		}
	}
	var rewrites []fileRewrite
	for _, loc := range locations {
		if info, err := os.Lstat(loc.from); err != nil || !info.IsDir() {
			if err == nil {
				err = errors.New("not a directory")
			}
			return nil, fmt.Errorf("detector package %s: %w", loc.from, err)
		}
		err := filepath.WalkDir(loc.from, func(p string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("detector package contains symlink %s", p)
			}
			if entry.IsDir() {
				return nil
			}
			rel, _ := filepath.Rel(loc.from, p)
			target := filepath.Join(loc.to, rel)
			switch {
			case strings.HasSuffix(entry.Name(), ".go"):
			case rel == ManifestFileName && len(steps) == 0:
			default:
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			var updated []byte
			if rel == ManifestFileName {
				manifest, err := ParseDetectorManifest(data)
				if err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
				ids, err := PlaybookIDs(manifest.PossiblePlaybooks)
				if err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
				if slices.Equal(ids, manifest.PossiblePlaybooks) {
					return nil
				}
				manifest.PossiblePlaybooks = ids
				if updated, err = MarshalDetectorManifest(manifest); err != nil {
					return err
				}
			} else {
				updated = []byte(RewritePlaybookReferences(string(data)))
				if bytes.Equal(updated, data) {
					return nil
				}
			}
			rewrites = append(rewrites, fileRewrite{path: target, data: updated, mode: info.Mode().Perm()})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return rewrites, nil
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

// AcceptedDetectors returns the detector registrations of a repository of any
// supported schema version as migration to CurrentSchemaVersion would write
// them: legacy or per-detector manifests, read without validation, with
// playbook references reduced to logical IDs. Upkeep ownership uses it to
// check that a migration preserved the accepted detectors.
func AcceptedDetectors(root string) ([]DetectorManifest, error) {
	manifests, err := LegacyDetectors(root)
	if err != nil {
		return nil, err
	}
	if manifests == nil {
		for _, class := range []Class{ClassHealth, ClassIncident} {
			parent := filepath.Join(root, filepath.FromSlash(DetectorsDir), class.Dir())
			entries, err := os.ReadDir(parent)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			for _, entry := range entries {
				data, err := os.ReadFile(filepath.Join(parent, entry.Name(), ManifestFileName))
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				if err != nil {
					return nil, err
				}
				manifest, err := ParseDetectorManifest(data)
				if err != nil {
					return nil, fmt.Errorf("detector %s: %w", entry.Name(), err)
				}
				manifests = append(manifests, manifest)
			}
		}
	}
	for i := range manifests {
		if manifests[i].PossiblePlaybooks, err = PlaybookIDs(manifests[i].PossiblePlaybooks); err != nil {
			return nil, fmt.Errorf("detector %s: %w", manifests[i].ID, err)
		}
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
		if manifest.PossiblePlaybooks, err = PlaybookIDs(manifest.PossiblePlaybooks); err != nil {
			return nil, fmt.Errorf("legacy detector %s: %w", detector.ID, err)
		}
		steps = append(steps, migrationStep{from: from, to: to, manifest: manifest, class: class})
	}
	return steps, nil
}
