package validate

import (
	"context"
	"errors"
	"fmt"
	"go/parser"
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// run is the state of one Validate call: the diagnostics so far and the parsed
// model of the candidate's operational memory.
type run struct {
	req   Request
	diags []Diagnostic

	// changed is the diff against Base, nil without Base.
	changed []string
	// memory reports whether operational memory is in scope: always without
	// Base, and only when the diff touches it with Base.
	memory bool

	parseFailed   map[string]bool
	schemaPresent bool
	schema        int
	legacyPath    string

	goal, arch *parsedDocument
	index      *textFile
	playbooks  map[string]*playbook
	detectors  map[string]*detector
}

type parsedDocument struct {
	path string
	doc  layout.Document
}

type textFile struct {
	path    string
	content []byte
	mode    fs.FileMode
}

type playbook struct {
	id     string
	readme *parsedDocument
	// hasReadme is true when the file exists, parsed or not.
	hasReadme bool
	scripts   []textFile
	programs  []textFile
}

type detector struct {
	class        layout.Class
	id           string
	manifest     *layout.DetectorManifest
	hasManifest  bool
	manifestAt   string
	manifestText []byte
	files        []textFile
}

func (d *detector) key() string { return d.class.Dir() + "/" + d.id }

func (d *detector) hasGoSource() bool {
	prefix := layout.DetectorDir(d.class, d.id) + "/"
	for _, f := range d.files {
		name := strings.TrimPrefix(f.path, prefix)
		if !strings.Contains(name, "/") && strings.HasSuffix(name, ".go") {
			return true
		}
	}
	return false
}

func (s *run) layers() Layer { return s.req.effectiveLayers() }

// add records a diagnostic when its layer is selected.
func (s *run) add(layer Layer, d Diagnostic) {
	if s.layers()&layer != 0 {
		s.diags = append(s.diags, d)
	}
}

// complete reports whether memory must hold every durable artifact. The
// bootstrap stages that precede the health judge accept partial memory.
func (s *run) complete() bool {
	if s.req.Phase != InitialPhase || s.req.Actor == HealthJudge {
		return true
	}
	return s.req.Actor == Upkeep && changesDetectors(s.changed)
}

func changesDetectors(paths []string) bool {
	for _, p := range paths {
		if kind, _, ok := layout.Classify(p); ok && (kind.IsDetectorManifest() || kind.IsDetectorSource() || kind == layout.KindLegacyDetectorManifest) {
			return true
		}
	}
	return false
}

func (s *run) load(ctx context.Context) error {
	s.playbooks = map[string]*playbook{}
	s.parseFailed = map[string]bool{}
	s.detectors = map[string]*detector{}
	s.memory = true
	if s.req.Base != "" {
		changed, err := ChangedPaths(ctx, s.req.Root, s.req.Base)
		if err != nil {
			return err
		}
		s.changed = changed
		s.memory = false
		for _, p := range changed {
			if layout.IsMemoryPath(p) {
				s.memory = true
			}
		}
		for _, p := range changed {
			if !layout.IsMemoryPath(p) {
				if err := s.checkChangedSource(p); err != nil {
					return err
				}
			}
			s.checkSecret(p)
		}
	}
	// A judge refresh revalidates the existing policy even when no file changed.
	if healthRevalidation(s.req, s.changed) {
		s.memory = true
	}
	if !s.memory {
		return nil
	}
	if err := s.walk(ctx); err != nil {
		return err
	}
	s.checkSchema()
	return nil
}

// checkChangedSource syntax-checks a changed file outside operational memory.
func (s *run) checkChangedSource(rel string) error {
	abs := filepath.Join(s.req.Root, filepath.FromSlash(rel))
	info, err := os.Lstat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		s.add(Structural, Diagnostic{Rule: RuleSymlink, Path: rel, Message: "changed path is a symlink", Hint: "commit a regular file"})
		return nil
	}
	if !info.Mode().IsRegular() {
		s.add(Structural, Diagnostic{Rule: RuleNotRegular, Path: rel, Message: "changed path is not a regular file"})
		return nil
	}
	switch strings.ToLower(filepath.Ext(rel)) {
	case ".yaml", ".yml":
		data, err := os.ReadFile(abs)
		if err != nil {
			return err
		}
		// Templated manifests are not YAML until rendered.
		if !strings.Contains(string(data), "{{") {
			if err := layout.RejectDuplicateKeys(data); err != nil {
				s.add(Structural, Diagnostic{Rule: RuleYAMLParse, Path: rel, Line: errorLine(err, 0), Message: err.Error()})
			}
		}
	case ".go":
		s.checkGo(rel, abs, nil)
	}
	return nil
}

func (s *run) checkSecret(rel string) {
	for _, part := range strings.Split(rel, "/") {
		lower := strings.ToLower(part)
		if lower == ".git" || lower == ".env" || strings.Contains(lower, "secret") {
			s.add(Structural, Diagnostic{Rule: RuleSecretPath, Path: rel, Message: fmt.Sprintf("secret-bearing path component %q", part), Hint: "never commit credentials or Git metadata"})
			return
		}
	}
}

func (s *run) checkGo(rel, abs string, content []byte) {
	var src any
	if content != nil {
		src = content
	}
	if _, err := parser.ParseFile(token.NewFileSet(), abs, src, parser.AllErrors); err != nil {
		line := 0
		message := err.Error()
		var list scanner.ErrorList
		if errors.As(err, &list) && len(list) > 0 {
			line = list[0].Pos.Line
			message = list[0].Msg
		}
		s.add(Structural, Diagnostic{Rule: RuleGoSyntax, Path: rel, Line: line, Message: "invalid Go syntax: " + message})
	}
}

func (s *run) walk(ctx context.Context) error {
	root := filepath.Join(s.req.Root, layout.Dir)
	info, err := os.Lstat(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		s.add(Structural, Diagnostic{Rule: RuleSymlink, Path: layout.Dir, Message: ".sdo must be a real directory", Hint: "replace it with a directory"})
		return nil
	}
	return filepath.WalkDir(root, func(abs string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(s.req.Root, abs)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(relative)
		if rel == layout.Dir {
			return nil
		}
		if s.req.Base == "" {
			s.checkSecret(rel)
		}
		switch {
		case entry.Type()&fs.ModeSymlink != 0:
			s.add(Structural, Diagnostic{Rule: RuleSymlink, Path: rel, Message: "symlinks are forbidden in operational memory", Hint: "commit the file itself"})
			return nil
		case entry.IsDir():
			return nil
		case !entry.Type().IsRegular():
			s.add(Structural, Diagnostic{Rule: RuleNotRegular, Path: rel, Message: "operational memory holds only regular files"})
			return nil
		}
		return s.visit(rel, abs)
	})
}

var invalidIDPath = regexp.MustCompile(`^\.sdo/(?:playbooks/([^/]+)/.+|diagnostics/detectors/(?:health|incidents)/([^/]+)/.+)$`)

func (s *run) visit(rel, abs string) error {
	kind, id, ok := layout.Classify(rel)
	if !ok {
		d := Diagnostic{Rule: RuleUnknownPath, Path: rel, Message: "path is not part of the operational-memory layout", Hint: "see docs/architecture.md for the .sdo/ layout, or run `sdo-operator memory ls`"}
		if m := invalidIDPath.FindStringSubmatch(rel); m != nil {
			d.Rule = RuleInvalidID
			d.Message = fmt.Sprintf("%q is not a valid artifact ID", m[1]+m[2])
			d.Hint = "playbook IDs match " + layout.PlaybookIDPattern + "; detector IDs match " + layout.DetectorIDPattern
		}
		s.add(Structural, d)
		return nil
	}
	if kind == layout.KindOutcome || kind == layout.KindLegacyOutcomes || kind == layout.KindRenderedLock || kind == layout.KindRenderedObject {
		return nil
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return err
	}
	if info.Size() > layout.MaxArtifactBytes && (kind != layout.KindCorpus || info.Size() > 8<<20) {
		s.add(Structural, Diagnostic{Rule: RuleTooLarge, Path: rel, Message: fmt.Sprintf("file is %d bytes; the limit is %d KiB", info.Size(), layout.MaxArtifactBytes/1024), Hint: "split or shorten the file"})
		s.markUnread(kind, id, rel)
		return nil
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return err
	}
	file := textFile{path: rel, content: data, mode: info.Mode()}
	switch kind {
	case layout.KindSchemaVersion:
		s.schemaPresent = true
		version, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			s.add(Structural, Diagnostic{Rule: RuleSchemaVersion, Path: rel, Line: 1, Message: "schema-version must be an integer", Hint: fmt.Sprintf("write %d", layout.CurrentSchemaVersion)})
			s.schema = -1
		} else {
			s.schema = version
		}
	case layout.KindGoal, layout.KindArchitecture:
		if doc := s.parseDocument(file); doc != nil {
			if kind == layout.KindGoal {
				s.goal = doc
			} else {
				s.arch = doc
			}
		}
	case layout.KindPlaybookIndex:
		s.index = &file
	case layout.KindPlaybook:
		book := s.playbook(id)
		book.hasReadme = true
		book.readme = s.parseDocument(file)
	case layout.KindPlaybookProgram:
		book := s.playbook(id)
		book.programs = append(book.programs, file)
		s.checkGo(rel, abs, data)
	case layout.KindLibrary:
		if strings.HasSuffix(rel, ".go") {
			s.checkGo(rel, abs, data)
		} else {
			s.checkYAML(file)
		}
	case layout.KindPlaybookScript:
		playbookID, _, _ := strings.Cut(id, "/")
		book := s.playbook(playbookID)
		book.scripts = append(book.scripts, file)
	case layout.KindHealthDetectorManifest, layout.KindIncidentDetectorManifest:
		class, _ := kind.Class()
		s.loadManifest(class, id, file)
	case layout.KindHealthDetectorSource, layout.KindIncidentDetectorSource:
		class, _ := kind.Class()
		det := s.detector(class, id)
		det.files = append(det.files, file)
		if strings.HasSuffix(rel, ".go") {
			s.checkGo(rel, abs, data)
		}
	case layout.KindTrafficGenerator:
		if strings.HasSuffix(rel, ".go") {
			s.checkGo(rel, abs, data)
		}
	case layout.KindTrafficWorkload, layout.KindLifecycleProvenance:
		s.checkYAML(file)
	case layout.KindLegacyDetectorManifest:
		s.legacyPath = rel
		s.checkYAML(file)
	}
	return nil
}

// markUnread records that a file exists although it was not parsed, so that
// rules about its absence do not fire on top of the real finding.
func (s *run) markUnread(kind layout.Kind, id, rel string) {
	switch kind {
	case layout.KindGoal, layout.KindArchitecture:
		s.parseFailed[rel] = true
	case layout.KindPlaybook:
		s.playbook(id).hasReadme = true
	case layout.KindHealthDetectorManifest, layout.KindIncidentDetectorManifest:
		class, _ := kind.Class()
		det := s.detector(class, id)
		det.hasManifest, det.manifestAt = true, rel
	}
}

func (s *run) playbook(id string) *playbook {
	if s.playbooks[id] == nil {
		s.playbooks[id] = &playbook{id: id}
	}
	return s.playbooks[id]
}

func (s *run) detector(class layout.Class, id string) *detector {
	d := &detector{class: class, id: id}
	if s.detectors[d.key()] == nil {
		s.detectors[d.key()] = d
	}
	return s.detectors[d.key()]
}

func (s *run) checkYAML(file textFile) {
	if err := layout.RejectDuplicateKeys(file.content); err != nil {
		s.add(Structural, Diagnostic{Rule: RuleYAMLParse, Path: file.path, Line: errorLine(err, 0), Message: err.Error()})
	}
}

func (s *run) parseDocument(file textFile) *parsedDocument {
	doc, err := layout.ParseDocument(file.content)
	if err != nil {
		s.parseFailed[file.path] = true
		s.add(Structural, Diagnostic{Rule: RuleDocumentParse, Path: file.path, Line: errorLine(err, 1), Message: err.Error(), Hint: "start the file with a `---` front matter block that sets schema_version: 1"})
		return nil
	}
	return &parsedDocument{path: file.path, doc: doc}
}

func (s *run) loadManifest(class layout.Class, id string, file textFile) {
	det := s.detector(class, id)
	det.hasManifest = true
	det.manifestAt = file.path
	det.manifestText = file.content
	manifest, err := layout.ParseDetectorManifest(file.content)
	if err != nil {
		s.add(Structural, Diagnostic{Rule: RuleManifestParse, Path: file.path, Line: errorLine(err, 0), Message: err.Error(), Hint: "valid keys: " + layout.ManifestKeys() + "; unknown or duplicate keys are rejected"})
		return
	}
	if err := manifest.Validate(); err != nil {
		s.add(Structural, Diagnostic{Rule: RuleManifestInvalid, Path: file.path, Message: err.Error()})
		return
	}
	if manifest.ID != id || manifest.DetectorClass() != class {
		s.add(Structural, Diagnostic{Rule: RuleManifestLocation, Path: file.path, Message: fmt.Sprintf("manifest says %s %q but lives in the %s detector directory %q", manifest.Class, manifest.ID, class, id), Hint: "the directory name is the detector ID and its parent selects the class"})
		return
	}
	det.manifest = &manifest
}

func (s *run) checkSchema() {
	complete := s.complete()
	switch {
	case s.schemaPresent && s.schema != layout.CurrentSchemaVersion && s.schema != -1:
		s.add(Structural, Diagnostic{Rule: RuleSchemaVersion, Path: layout.SchemaVersionFile, Line: 1, Message: fmt.Sprintf("schema-version is %d; this validator requires %d", s.schema, layout.CurrentSchemaVersion), Hint: "run the Upkeep schema migration"})
	case !s.schemaPresent && complete:
		s.add(Structural, Diagnostic{Rule: RuleSchemaVersion, Path: layout.SchemaVersionFile, Message: "schema-version is missing", Hint: fmt.Sprintf("create the file containing %d", layout.CurrentSchemaVersion)})
	}
	if s.legacyPath != "" && (!s.schemaPresent || s.schema == layout.CurrentSchemaVersion) {
		s.add(Structural, Diagnostic{Rule: RuleLegacyManifest, Path: s.legacyPath, Message: fmt.Sprintf("the shared detector manifest is not part of schema version %d", layout.CurrentSchemaVersion), Hint: "register each detector in its own manifest.yaml"})
	}
}

var lineNumber = regexp.MustCompile(`line (\d+)`)

// errorLine extracts the first "line N" from a parser error and adds offset,
// for text parsed from the middle of a file. It returns 0 when there is none.
func errorLine(err error, offset int) int {
	if m := lineNumber.FindStringSubmatch(err.Error()); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n + offset
	}
	return 0
}

func firstLine(content []byte, contains func(string) bool) int {
	for i, line := range strings.Split(string(content), "\n") {
		if contains(line) {
			return i + 1
		}
	}
	return 0
}
