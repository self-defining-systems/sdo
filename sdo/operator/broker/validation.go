package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
)

var placeholder = regexp.MustCompile(`<\w*[A-Z][A-Z0-9_]+>`)
var markdownLink = regexp.MustCompile(`\[[^]]+\]\(([^)]+)\)`)

func validateCandidate(ctx context.Context, candidate, baseline string, actor Actor, phase Phase, paths []string) error {
	memoryChanged := false
	for _, relative := range paths {
		if layout.IsMemoryPath(relative) {
			memoryChanged = true
		}
		path := filepath.Join(candidate, relative)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("changed path is not a regular file: %s", relative)
		}
		switch strings.ToLower(filepath.Ext(relative)) {
		case ".yaml", ".yml":
			if err := validateYAML(path); err != nil {
				return fmt.Errorf("%s: %w", relative, err)
			}
		case ".go":
			if _, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors); err != nil {
				return fmt.Errorf("invalid Go syntax in %s: %w", relative, err)
			}
		}
	}
	if !memoryChanged {
		return nil
	}
	if actor == Upkeep && changesDetectors(paths) {
		if err := validateUpkeepMigration(candidate, baseline, paths); err != nil {
			return err
		}
	}
	if phase == InitialPhase && actor != HealthJudge && !(actor == Upkeep && changesDetectors(paths)) {
		return validateInitialStage(ctx, candidate, baseline, actor, paths)
	}
	if err := validateMemoryTree(ctx, candidate); err != nil {
		return err
	}
	if changesDetectorManifest(paths) {
		if err := validateDetectorOwnership(candidate, baseline, actor, phase); err != nil {
			return err
		}
	}
	return nil
}

func changesDetectors(paths []string) bool {
	for _, path := range paths {
		if kind, _, ok := layout.Classify(path); ok && (kind.IsDetectorManifest() || kind.IsDetectorSource() || kind == layout.KindLegacyDetectorManifest) {
			return true
		}
	}
	return false
}

// validateUpkeepMigration lets Upkeep touch detectors only as part of a schema
// migration: the same change must stamp the new schema version, and the
// accepted detector registrations must survive it unchanged.
func validateUpkeepMigration(candidate, baseline string, paths []string) error {
	if !contains(paths, layout.SchemaVersionFile) {
		return errors.New("upkeep may change detectors only together with a schema-version migration")
	}
	if _, err := os.Stat(filepath.Join(baseline, layout.LegacyDetectorManifestFile)); err != nil {
		return errors.New("upkeep detector migration needs a legacy detector manifest in the accepted baseline")
	}
	legacy, err := layout.LegacyDetectors(baseline)
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	after, err := layout.ReadDetectors(filepath.Join(candidate, layout.DiagnosticsDir))
	if err != nil {
		return err
	}
	if len(legacy) != len(after) {
		return fmt.Errorf("migration changed the detector count from %d to %d", len(legacy), len(after))
	}
	before := map[string]layout.DetectorManifest{}
	for _, manifest := range legacy {
		before[manifest.ID] = manifest
	}
	for _, detector := range after {
		prior, ok := before[detector.Manifest.ID]
		if !ok || !reflect.DeepEqual(prior, detector.Manifest) {
			return fmt.Errorf("migration changed detector %s", detector.Manifest.ID)
		}
	}
	return nil
}

// Initial memory is accepted in role-owned stages. Validate each stage's
// durable files immediately; the health-judge stage validates the full tree.
func validateInitialStage(ctx context.Context, root, baseline string, actor Actor, paths []string) error {
	if err := rejectMemorySymlinks(filepath.Join(root, layout.Dir)); err != nil {
		return err
	}
	for _, path := range paths {
		kind, _, _ := layout.Classify(path)
		switch kind {
		case layout.KindGoal:
			if err := validateDocument(filepath.Join(root, path), "human"); err != nil {
				return fmt.Errorf("goal: %w", err)
			}
		case layout.KindArchitecture:
			if err := validateDocument(filepath.Join(root, path), ""); err != nil {
				return fmt.Errorf("architecture: %w", err)
			}
			if actor == Deployer {
				if err := validateInitialArchitecture(ctx, root, baseline); err != nil {
					return err
				}
			}
		case layout.KindSchemaVersion:
			version, err := os.ReadFile(filepath.Join(root, path))
			if err != nil || strings.TrimSpace(string(version)) != fmt.Sprint(layout.CurrentSchemaVersion) {
				return fmt.Errorf("operational memory schema-version must be %d", layout.CurrentSchemaVersion)
			}
		}
	}
	if actor == Human && !contains(paths, layout.GoalFile) {
		return errors.New("human initial proposal must include goal.md")
	}
	return nil
}

func validateInitialArchitecture(ctx context.Context, candidate, baseline string) error {
	sourceCommit, err := gitText(ctx, baseline, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	message, err := gitText(ctx, baseline, "log", "-1", "--format=%B")
	if err != nil {
		return err
	}
	if !strings.Contains("\n"+message+"\n", "\nSDO-Actor: deployer\n") || !strings.Contains("\n"+message+"\n", "\nSDO-Phase: initial\n") {
		return errors.New("initial architecture must follow a broker-accepted deployer source commit")
	}
	changed, err := gitText(ctx, baseline, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", sourceCommit)
	if err != nil {
		return err
	}
	hasSource := false
	for _, path := range strings.Split(changed, "\n") {
		if path != "" && !layout.IsMemoryPath(path) {
			hasSource = true
			break
		}
	}
	if !hasSource {
		return errors.New("initial architecture has no accepted deployer source commit")
	}
	data, err := os.ReadFile(filepath.Join(candidate, layout.ArchitectureFile))
	if err != nil {
		return err
	}
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 {
		return errors.New("architecture front matter is required")
	}
	var metadata struct {
		GeneratedAtCommit string `yaml:"generated_at_commit"`
	}
	if err := yaml.Unmarshal([]byte(parts[1]), &metadata); err != nil {
		return err
	}
	if metadata.GeneratedAtCommit != sourceCommit {
		return fmt.Errorf("architecture generated_at_commit must cite accepted source commit %s", sourceCommit)
	}
	return nil
}

func validateYAML(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if bytes.Contains(data, []byte("{{")) {
		return nil
	}
	return layout.RejectDuplicateKeys(data)
}

func validateDocument(path, owner string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	document, err := layout.ParseDocument(data)
	if err != nil {
		return err
	}
	if owner != "" && document.Meta["owner"] != owner {
		return fmt.Errorf("owner must be %s", owner)
	}
	return nil
}

func validateMemoryTree(ctx context.Context, root string) error {
	memoryRoot := filepath.Join(root, layout.Dir)
	if err := rejectMemorySymlinks(memoryRoot); err != nil {
		return err
	}
	tree, err := layout.ReadTree(root)
	if err != nil {
		return err
	}
	if tree.Goal == nil || tree.Goal.Meta["owner"] != "human" {
		return errors.New("goal: owner must be human")
	}
	if tree.Architecture == nil {
		return errors.New("architecture: missing")
	}
	if err := validatePlaybooks(ctx, tree); err != nil {
		return err
	}
	return validateDetectors(tree)
}

func rejectMemorySymlinks(memoryRoot string) error {
	return filepath.WalkDir(memoryRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is forbidden in operational memory: %s", path)
		}
		return nil
	})
}

func validatePlaybooks(ctx context.Context, tree *layout.Tree) error {
	if tree.PlaybookIndex == "" {
		return errors.New("playbook index is required")
	}
	linked := make(map[string]bool)
	for _, match := range markdownLink.FindAllStringSubmatch(tree.PlaybookIndex, -1) {
		link := strings.SplitN(match[1], "#", 2)[0]
		if strings.Contains(link, "://") {
			continue
		}
		clean := filepath.Clean(link)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("playbook index link escapes root: %s", link)
		}
		if !strings.HasSuffix(clean, layout.PlaybookFileName) {
			clean = filepath.Join(clean, layout.PlaybookFileName)
		}
		linked[filepath.ToSlash(clean)] = true
		if !hasPlaybookFile(tree, filepath.ToSlash(clean)) {
			return fmt.Errorf("playbook index link does not exist: %s", link)
		}
	}
	for _, playbook := range tree.Playbooks {
		if !linked[playbook.ID+"/"+layout.PlaybookFileName] {
			return fmt.Errorf("playbook %s is missing from index", playbook.ID)
		}
		faultClass := playbook.ID
		if playbook.ID == healthObjectivePlaybook {
			faultClass = "health-objective-failure"
		}
		if err := validatePlaybookDocument(playbook.Document, faultClass); err != nil {
			return fmt.Errorf("playbook %s: %w", playbook.ID, err)
		}
		for _, script := range playbook.Scripts {
			cmd := exec.CommandContext(ctx, "bash", "-n")
			cmd.Stdin = bytes.NewReader(script.Content)
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("invalid shell syntax: %s: %w", strings.TrimSpace(string(out)), err)
			}
		}
	}
	if len(tree.Playbooks) == 0 {
		return errors.New("at least one playbook is required")
	}
	return nil
}

func hasPlaybookFile(tree *layout.Tree, relative string) bool {
	for _, playbook := range tree.Playbooks {
		if playbook.ID+"/"+layout.PlaybookFileName == relative {
			return true
		}
	}
	return false
}

func validatePlaybookDocument(document layout.Document, faultClass string) error {
	if document.Meta["owner"] != "responder" {
		return errors.New("owner must be responder")
	}
	if document.Meta["fault_class"] != faultClass {
		return fmt.Errorf("fault_class must be %s", faultClass)
	}
	if document.Meta["originating_incident"] == nil || strings.TrimSpace(fmt.Sprint(document.Meta["originating_incident"])) == "" {
		return errors.New("originating_incident is required")
	}
	if !placeholder.MatchString(document.Body) {
		return errors.New("playbook must use a role placeholder")
	}
	return nil
}

func validateDetectors(tree *layout.Tree) error {
	if len(tree.Detectors) == 0 {
		return errors.New("memory registers no detectors")
	}
	playbooks := map[string]bool{}
	for _, playbook := range tree.Playbooks {
		playbooks[playbook.ID] = true
	}
	ids := map[string]bool{}
	for _, detector := range tree.Detectors {
		manifest := detector.Manifest
		if ids[manifest.ID] {
			return fmt.Errorf("empty or duplicate detector ID %q", manifest.ID)
		}
		ids[manifest.ID] = true
		hasGo := false
		for _, file := range detector.Files {
			hasGo = hasGo || (!strings.Contains(file.Name, "/") && strings.HasSuffix(file.Name, ".go"))
		}
		if !hasGo {
			return fmt.Errorf("detector %s package has no Go source", manifest.ID)
		}
		for _, reference := range manifest.PossiblePlaybooks {
			id, err := layout.PlaybookID(reference)
			if err != nil || !playbooks[id] {
				return fmt.Errorf("detector %s playbook does not exist: %s", manifest.ID, reference)
			}
		}
	}
	return nil
}

// validateDetectorOwnership compares the candidate's detector registrations
// with the accepted baseline: each actor may add or change only detectors of
// its own class, and responders may not rewrite provenance.
func validateDetectorOwnership(candidate, baseline string, actor Actor, phase Phase) error {
	if actor != Responder && actor != HealthJudge {
		return nil
	}
	after, err := layout.ReadDetectors(filepath.Join(candidate, layout.DiagnosticsDir))
	if err != nil {
		return err
	}
	hadDetectors, err := layout.HasDetectors(baseline)
	if err != nil {
		return err
	}
	if phase == InitialPhase && actor == HealthJudge && !hadDetectors {
		for _, detector := range after {
			if detector.Class != layout.ClassHealth {
				return fmt.Errorf("initial health judge cannot add %s detector %s", detector.Class, detector.Manifest.ID)
			}
		}
		return nil
	}
	before, err := layout.ReadDetectors(filepath.Join(baseline, layout.DiagnosticsDir))
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	protected := layout.ClassHealth
	if actor == HealthJudge {
		protected = layout.ClassIncident
	}
	old := map[string]layout.DetectorManifest{}
	for _, detector := range before {
		old[detector.Manifest.ID] = detector.Manifest
	}
	seen := map[string]bool{}
	for _, detector := range after {
		current := detector.Manifest
		seen[current.ID] = true
		prior, existed := old[current.ID]
		if existed && prior.DetectorClass() == protected && !reflect.DeepEqual(prior, current) {
			return fmt.Errorf("actor %s changed protected %s detector %s", actor, protected, current.ID)
		}
		if !existed && detector.Class == protected {
			return fmt.Errorf("actor %s added protected %s detector %s", actor, protected, current.ID)
		}
		if existed && actor == Responder && prior.DetectorClass() == layout.ClassIncident && (prior.OriginatingIncident != current.OriginatingIncident || prior.OriginatingCommit != current.OriginatingCommit) {
			return fmt.Errorf("responder rewrote detector %s provenance", current.ID)
		}
		if actor == Responder && detector.Class == layout.ClassIncident && (!existed || !reflect.DeepEqual(prior, current)) && current.Persistence.Firing != 1 {
			return fmt.Errorf("new or changed incident detector %s must fire on first match", current.ID)
		}
	}
	for _, detector := range before {
		if detector.Class == protected && !seen[detector.Manifest.ID] {
			return fmt.Errorf("actor %s removed protected %s detector %s", actor, protected, detector.Manifest.ID)
		}
	}
	return nil
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
