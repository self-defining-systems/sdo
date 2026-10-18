package layout

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// MaxArtifactBytes bounds every memory file the layout reads.
const MaxArtifactBytes = 256 * 1024

// Tree is the parsed content of a repository's operational memory. Outcomes are
// stored one document per incident and are read through the outcome store, not
// here. Diagnostics traffic and module files are classified but not parsed.
type Tree struct {
	SchemaVersion int
	Goal          *Document
	Architecture  *Document
	PlaybookIndex string
	Detectors     []Detector
	Playbooks     []Playbook
}

// File is a file inside a detector package or playbook, named relative to it.
type File struct {
	Name    string
	Content []byte
}

// Detector is one detector package and its manifest.
type Detector struct {
	Class    Class
	Manifest DetectorManifest
	Files    []File
}

// Package is the detector package path relative to the diagnostics module.
func (d Detector) Package() string { return DetectorPackage(d.Class, d.Manifest.ID) }

// Playbook is one responder playbook and its scripts.
type Playbook struct {
	ID       string
	Document Document
	Scripts  []File
}

// ReadTree parses the operational memory below root. It fails on schema
// versions other than CurrentSchemaVersion, symlinks, oversized or unrecognized
// files, and malformed manifests or documents. It does not require any
// particular artifact to be present beyond schema-version.
func ReadTree(root string) (*Tree, error) {
	version, err := readSchemaVersion(root)
	if err != nil {
		return nil, err
	}
	if version != CurrentSchemaVersion {
		return nil, fmt.Errorf("unsupported operational memory schema-version %d; need %d (run the Upkeep migration)", version, CurrentSchemaVersion)
	}
	c := newCollector()
	c.tree.SchemaVersion = version
	if err := scan(filepath.Join(root, Dir), Dir, func(rel string, kind Kind, id string, read func() ([]byte, error)) error {
		return c.add(rel, kind, id, read)
	}); err != nil {
		return nil, err
	}
	return c.finish()
}

// ReadDetectors parses the detector packages below a diagnostics directory. A
// missing detectors directory yields no detectors.
func ReadDetectors(diagnosticsDir string) ([]Detector, error) {
	c := newCollector()
	err := scan(filepath.Join(diagnosticsDir, "detectors"), DetectorsDir, func(rel string, kind Kind, id string, read func() ([]byte, error)) error {
		if !kind.IsDetectorManifest() && !kind.IsDetectorSource() {
			return fmt.Errorf("unexpected file in detectors: %s", rel)
		}
		return c.add(rel, kind, id, read)
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tree, err := c.finish()
	if err != nil {
		return nil, err
	}
	return tree.Detectors, nil
}

// HasDetectors reports whether any detector registration exists below root,
// in either the current or the legacy form.
func HasDetectors(root string) (bool, error) {
	if _, err := os.Lstat(filepath.Join(root, LegacyDetectorManifestFile)); err == nil {
		return true, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	for _, class := range []Class{ClassHealth, ClassIncident} {
		entries, err := os.ReadDir(filepath.Join(root, DetectorsDir, class.Dir()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if _, err := os.Lstat(filepath.Join(root, DetectorManifestPath(class, entry.Name()))); err == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

// WriteDetector stores a detector's manifest and files below root, replacing
// existing files of the same name. It checks only that the paths are safe.
func WriteDetector(root string, detector Detector) error {
	if !detector.Class.valid() || !ValidDetectorID(detector.Manifest.ID) {
		return fmt.Errorf("invalid detector class %q or id %q", detector.Class, detector.Manifest.ID)
	}
	directory := filepath.Join(root, filepath.FromSlash(DetectorDir(detector.Class, detector.Manifest.ID)))
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	encoded, err := MarshalDetectorManifest(detector.Manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, ManifestFileName), encoded, 0o644); err != nil {
		return err
	}
	for _, file := range detector.Files {
		if file.Name == ManifestFileName || !fs.ValidPath(file.Name) || file.Name == "." {
			return fmt.Errorf("invalid detector file name %q", file.Name)
		}
		target := filepath.Join(directory, filepath.FromSlash(file.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, file.Content, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// RemoveDetector deletes a detector package, including its manifest.
func RemoveDetector(root string, class Class, id string) error {
	if !class.valid() || !ValidDetectorID(id) {
		return fmt.Errorf("invalid detector class %q or id %q", class, id)
	}
	return os.RemoveAll(filepath.Join(root, filepath.FromSlash(DetectorDir(class, id))))
}

type collector struct {
	tree      Tree
	detectors map[string]*Detector
	manifests map[string]bool
	playbooks map[string]*Playbook
	readmes   map[string]bool
}

func newCollector() *collector {
	return &collector{detectors: map[string]*Detector{}, manifests: map[string]bool{}, playbooks: map[string]*Playbook{}, readmes: map[string]bool{}}
}

func (c *collector) detector(class Class, id string) *Detector {
	key := class.Dir() + "/" + id
	if c.detectors[key] == nil {
		c.detectors[key] = &Detector{Class: class}
	}
	return c.detectors[key]
}

func (c *collector) playbook(id string) *Playbook {
	if c.playbooks[id] == nil {
		c.playbooks[id] = &Playbook{ID: id}
	}
	return c.playbooks[id]
}

func (c *collector) add(rel string, kind Kind, id string, read func() ([]byte, error)) error {
	switch kind {
	case KindLegacyDetectorManifest:
		return fmt.Errorf("legacy detector manifest %s is not part of schema version %d", rel, CurrentSchemaVersion)
	case KindGoal, KindArchitecture:
		data, err := read()
		if err != nil {
			return err
		}
		document, err := ParseDocument(data)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if kind == KindGoal {
			c.tree.Goal = &document
		} else {
			c.tree.Architecture = &document
		}
	case KindPlaybookIndex:
		data, err := read()
		if err != nil {
			return err
		}
		c.tree.PlaybookIndex = string(data)
	case KindPlaybook:
		data, err := read()
		if err != nil {
			return err
		}
		document, err := ParseDocument(data)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		c.playbook(id).Document = document
		c.readmes[id] = true
	case KindPlaybookScript:
		playbookID, name, _ := strings.Cut(id, "/")
		data, err := read()
		if err != nil {
			return err
		}
		playbook := c.playbook(playbookID)
		playbook.Scripts = append(playbook.Scripts, File{Name: name, Content: data})
	case KindHealthDetectorManifest, KindIncidentDetectorManifest:
		class, _ := kind.Class()
		data, err := read()
		if err != nil {
			return err
		}
		manifest, err := ParseDetectorManifest(data)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		c.detector(class, id).Manifest = manifest
		c.manifests[class.Dir()+"/"+id] = true
	case KindHealthDetectorSource, KindIncidentDetectorSource:
		class, _ := kind.Class()
		data, err := read()
		if err != nil {
			return err
		}
		prefix := DetectorDir(class, id) + "/"
		detector := c.detector(class, id)
		detector.Files = append(detector.Files, File{Name: strings.TrimPrefix(rel, prefix), Content: data})
	}
	return nil
}

func (c *collector) finish() (*Tree, error) {
	for key, detector := range c.detectors {
		class, id, _ := strings.Cut(key, "/")
		if !c.manifests[key] {
			return nil, fmt.Errorf("detector %s has no %s", key, ManifestFileName)
		}
		if err := detector.Manifest.Validate(); err != nil {
			return nil, fmt.Errorf("detector %s: %w", key, err)
		}
		if detector.Manifest.ID != id || detector.Manifest.Class != string(detector.Class) || detector.Class.Dir() != class {
			return nil, fmt.Errorf("detector manifest %s disagrees with its location", key)
		}
		sort.Slice(detector.Files, func(i, j int) bool { return detector.Files[i].Name < detector.Files[j].Name })
		c.tree.Detectors = append(c.tree.Detectors, *detector)
	}
	sort.Slice(c.tree.Detectors, func(i, j int) bool {
		a, b := c.tree.Detectors[i], c.tree.Detectors[j]
		if a.Class != b.Class {
			return a.Class == ClassHealth
		}
		return a.Manifest.ID < b.Manifest.ID
	})
	for id, playbook := range c.playbooks {
		if !c.readmes[id] {
			return nil, fmt.Errorf("playbook %s has no %s", id, PlaybookFileName)
		}
		sort.Slice(playbook.Scripts, func(i, j int) bool { return playbook.Scripts[i].Name < playbook.Scripts[j].Name })
		c.tree.Playbooks = append(c.tree.Playbooks, *playbook)
	}
	sort.Slice(c.tree.Playbooks, func(i, j int) bool { return c.tree.Playbooks[i].ID < c.tree.Playbooks[j].ID })
	return &c.tree, nil
}

// scan visits every file below directory, whose repository-relative path is
// prefix. It refuses symlinks and irregular files, and skips the outcomes
// directory, which holds one document per incident.
func scan(directory, prefix string, visit func(rel string, kind Kind, id string, read func() ([]byte, error)) error) error {
	if info, err := os.Lstat(directory); err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", prefix)
	}
	return filepath.WalkDir(directory, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel := path.Join(prefix, filepath.ToSlash(strings.TrimPrefix(strings.TrimPrefix(p, directory), string(filepath.Separator))))
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("symlink is forbidden in operational memory: %s", rel)
		}
		if entry.IsDir() {
			if rel == OutcomesDir {
				return fs.SkipDir
			}
			return nil
		}
		kind, id, ok := Classify(rel)
		if !ok {
			return fmt.Errorf("unrecognized file in operational memory: %s", rel)
		}
		return visit(rel, kind, id, func() ([]byte, error) { return readRegular(p, rel) })
	})
}

func readRegular(p, rel string) ([]byte, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("memory artifact %s is not a regular file", rel)
	}
	if info.Size() > MaxArtifactBytes {
		return nil, fmt.Errorf("memory artifact %s exceeds %d KiB", rel, MaxArtifactBytes/1024)
	}
	return os.ReadFile(p)
}
