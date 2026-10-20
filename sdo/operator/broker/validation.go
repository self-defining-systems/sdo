package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory"
)

var placeholder = regexp.MustCompile(`<\w*[A-Z][A-Z0-9_]+>`)
var markdownLink = regexp.MustCompile(`\[[^]]+\]\(([^)]+)\)`)

type detectorManifest struct {
	APIVersion string                 `yaml:"apiVersion"`
	Kind       string                 `yaml:"kind"`
	SDKVersion string                 `yaml:"sdkVersion"`
	Detectors  []detectorRegistration `yaml:"detectors"`
}

type detectorRegistration struct {
	ID          string `yaml:"id"`
	Package     string `yaml:"package"`
	Constructor string `yaml:"constructor"`
	Class       string `yaml:"class"`
	Owner       string `yaml:"owner"`
	Watches     []struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Namespace  string `yaml:"namespace"`
	} `yaml:"watches"`
	Interval    string `yaml:"interval"`
	Persistence struct {
		Firing   int `yaml:"firing"`
		Clearing int `yaml:"clearing"`
	} `yaml:"persistence"`
	Batching struct {
		Severity string `yaml:"severity"`
		Debounce string `yaml:"debounce"`
	} `yaml:"batching"`
	PossiblePlaybooks   []string `yaml:"possiblePlaybooks"`
	OriginatingIncident string   `yaml:"originatingIncident"`
	OriginatingCommit   string   `yaml:"originatingCommit"`
}

func validateCandidate(ctx context.Context, candidate, baseline string, actor Actor, phase Phase, paths []string) error {
	memoryChanged := false
	for _, relative := range paths {
		if strings.HasPrefix(relative, ".sdo/") {
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
	if err := ValidateNewProgramCorpus(candidate, baseline); err != nil {
		return err
	}
	if phase == InitialPhase && actor != HealthJudge {
		return validateInitialStage(ctx, candidate, baseline, actor, paths)
	}
	if err := validateMemoryTree(ctx, candidate); err != nil {
		return err
	}
	if contains(paths, ".sdo/diagnostics/manifest.yaml") {
		if err := validateManifestOwnership(candidate, baseline, actor, phase); err != nil {
			return err
		}
	}
	return nil
}

// Initial memory is accepted in role-owned stages. Validate each stage's
// durable files immediately; the health-judge stage validates the full tree.
func validateInitialStage(ctx context.Context, root, baseline string, actor Actor, paths []string) error {
	memoryRoot := filepath.Join(root, ".sdo")
	if err := rejectMemorySymlinks(memoryRoot); err != nil {
		return err
	}
	for _, path := range paths {
		switch path {
		case ".sdo/goal.md":
			if err := validateMarkdown(filepath.Join(root, path), "human", "", false); err != nil {
				return fmt.Errorf("goal: %w", err)
			}
		case ".sdo/arch.md":
			if err := validateMarkdown(filepath.Join(root, path), "", "", false); err != nil {
				return fmt.Errorf("architecture: %w", err)
			}
			if actor == Deployer {
				if err := validateInitialArchitecture(ctx, root, baseline); err != nil {
					return err
				}
			}
		case ".sdo/schema-version":
			version, err := os.ReadFile(filepath.Join(root, path))
			if err != nil || strings.TrimSpace(string(version)) != "1" {
				return errors.New("operational memory schema-version must be 1")
			}
		}
	}
	if actor == Human && !contains(paths, ".sdo/goal.md") {
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
		if path != "" && !strings.HasPrefix(path, ".sdo/") {
			hasSource = true
			break
		}
	}
	if !hasSource {
		return errors.New("initial architecture has no accepted deployer source commit")
	}
	data, err := os.ReadFile(filepath.Join(candidate, ".sdo", "arch.md"))
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
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var node yaml.Node
		if err := decoder.Decode(&node); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("invalid YAML: %w", err)
		}
		if err := uniqueKeys(&node); err != nil {
			return err
		}
	}
}

func uniqueKeys(node *yaml.Node) error {
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i].Value
			if seen[key] {
				return fmt.Errorf("duplicate YAML key %q at line %d", key, node.Content[i].Line)
			}
			seen[key] = true
		}
	}
	for _, child := range node.Content {
		if err := uniqueKeys(child); err != nil {
			return err
		}
	}
	return nil
}

// ValidateMemoryTree applies the broker's operational-memory checks to a checkout, so a local draft is judged by
// the same playbook and front-matter rules the broker enforces before it commits.
func ValidateMemoryTree(ctx context.Context, root string) error { return validateMemoryTree(ctx, root) }

func validateMemoryTree(ctx context.Context, root string) error {
	memoryRoot := filepath.Join(root, ".sdo")
	if err := rejectMemorySymlinks(memoryRoot); err != nil {
		return err
	}
	if err := filepath.WalkDir(memoryRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(data)
		if err != nil {
			return fmt.Errorf("invalid Go source %s: %w", path, err)
		}
		if !bytes.Equal(data, formatted) {
			return fmt.Errorf("Go source %s is not gofmt formatted", path)
		}
		return nil
	}); err != nil {
		return err
	}
	version, err := os.ReadFile(filepath.Join(memoryRoot, "schema-version"))
	if err != nil || strings.TrimSpace(string(version)) != "1" {
		return errors.New("operational memory schema-version must be 1")
	}
	if err := validateMarkdown(filepath.Join(memoryRoot, "goal.md"), "human", "", false); err != nil {
		return fmt.Errorf("goal: %w", err)
	}
	if err := validateMarkdown(filepath.Join(memoryRoot, "arch.md"), "", "", false); err != nil {
		return fmt.Errorf("architecture: %w", err)
	}
	if err := validatePlaybooks(ctx, memoryRoot); err != nil {
		return err
	}
	_, err = readManifest(memoryRoot)
	return err
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

func validateMarkdown(path, owner, faultClass string, requirePlaceholder bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" {
		return errors.New("markdown front matter is required")
	}
	var metadata map[string]any
	if err := yaml.Unmarshal([]byte(parts[1]), &metadata); err != nil {
		return fmt.Errorf("front matter: %w", err)
	}
	if metadata["schema_version"] != 1 {
		return errors.New("schema_version must be 1")
	}
	if owner != "" && metadata["owner"] != owner {
		return fmt.Errorf("owner must be %s", owner)
	}
	if faultClass != "" {
		if metadata["fault_class"] != faultClass {
			return fmt.Errorf("fault_class must be %s", faultClass)
		}
		if strings.TrimSpace(fmt.Sprint(metadata["originating_incident"])) == "" || metadata["originating_incident"] == nil {
			return errors.New("originating_incident is required")
		}
	}
	if strings.TrimSpace(parts[2]) == "" {
		return errors.New("body must not be empty")
	}
	if requirePlaceholder && !placeholder.MatchString(parts[2]) {
		return errors.New("playbook must use a role placeholder")
	}
	return nil
}

func validatePlaybooks(ctx context.Context, memoryRoot string) error {
	root := filepath.Join(memoryRoot, "playbooks")
	index, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		return fmt.Errorf("playbook index is required: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	count := 0
	linked := make(map[string]bool)
	for _, match := range markdownLink.FindAllStringSubmatch(string(index), -1) {
		link := strings.SplitN(match[1], "#", 2)[0]
		if strings.Contains(link, "://") {
			continue
		}
		clean := filepath.Clean(link)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("playbook index link escapes root: %s", link)
		}
		if !strings.HasSuffix(clean, "README.md") {
			clean = filepath.Join(clean, "README.md")
		}
		linked[filepath.ToSlash(clean)] = true
		if _, err := os.Stat(filepath.Join(root, clean)); err != nil {
			return fmt.Errorf("playbook index link does not exist: %s", link)
		}
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		count++
		path := filepath.Join(root, entry.Name(), "README.md")
		if !linked[filepath.ToSlash(filepath.Join(entry.Name(), "README.md"))] {
			return fmt.Errorf("playbook %s is missing from index", entry.Name())
		}
		owner, faultClass := "responder", entry.Name()
		if entry.Name() == "health-objective" {
			faultClass = "health-objective-failure"
		}
		if err := validateMarkdown(path, owner, faultClass, true); err != nil {
			return fmt.Errorf("playbook %s: %w", entry.Name(), err)
		}
		scripts, err := os.ReadDir(filepath.Join(root, entry.Name(), "scripts"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, script := range scripts {
			if script.IsDir() || filepath.Ext(script.Name()) != ".sh" {
				return fmt.Errorf("playbook script must be a .sh file: %s", script.Name())
			}
			cmd := exec.CommandContext(ctx, "bash", "-n", filepath.Join(root, entry.Name(), "scripts", script.Name()))
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("invalid shell syntax: %s: %w", strings.TrimSpace(string(out)), err)
			}
		}
	}
	if count == 0 {
		return errors.New("at least one playbook is required")
	}
	return nil
}

func readManifest(memoryRoot string) (detectorManifest, error) {
	path := filepath.Join(memoryRoot, "diagnostics", "manifest.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return detectorManifest{}, fmt.Errorf("diagnostics manifest: %w", err)
	}
	if err := validateYAML(path); err != nil {
		return detectorManifest{}, err
	}
	var manifest detectorManifest
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return detectorManifest{}, fmt.Errorf("diagnostics manifest: %w", err)
	}
	if manifest.APIVersion != "sdo.dev/v1alpha1" || manifest.Kind != "DetectorManifest" || manifest.SDKVersion == "" || len(manifest.Detectors) == 0 {
		return detectorManifest{}, errors.New("invalid diagnostics manifest header or empty detectors")
	}
	ids := map[string]bool{}
	for _, d := range manifest.Detectors {
		if d.ID == "" || ids[d.ID] {
			return detectorManifest{}, fmt.Errorf("empty or duplicate detector ID %q", d.ID)
		}
		ids[d.ID] = true
		expectedDir, owner := "health", "health_judge"
		if d.Class == "incident" {
			expectedDir, owner = "incidents", "responder"
		} else if d.Class != "health" {
			return detectorManifest{}, fmt.Errorf("invalid detector class %q", d.Class)
		}
		if d.Owner != owner {
			return detectorManifest{}, fmt.Errorf("detector %s has wrong owner", d.ID)
		}
		packagePath := filepath.Clean(strings.TrimPrefix(d.Package, "./"))
		if filepath.IsAbs(packagePath) || !strings.HasPrefix(filepath.ToSlash(packagePath), "detectors/"+expectedDir+"/") || strings.Contains(packagePath, "..") {
			return detectorManifest{}, fmt.Errorf("detector %s package escapes owned directory", d.ID)
		}
		files, err := filepath.Glob(filepath.Join(memoryRoot, "diagnostics", packagePath, "*.go"))
		if err != nil || len(files) == 0 {
			return detectorManifest{}, fmt.Errorf("detector %s package has no Go source", d.ID)
		}
		if d.Class == "incident" && d.OriginatingIncident == "" {
			return detectorManifest{}, fmt.Errorf("incident detector %s must identify origin", d.ID)
		}
		if d.OriginatingCommit == "" || d.Interval == "" || d.Persistence.Firing < 1 || d.Persistence.Clearing < 1 || len(d.Watches) == 0 {
			return detectorManifest{}, fmt.Errorf("incomplete detector %s", d.ID)
		}
		for _, playbook := range d.PossiblePlaybooks {
			id, err := memory.PlaybookID(playbook)
			if err != nil {
				return detectorManifest{}, fmt.Errorf("detector %s has unsafe playbook", d.ID)
			}
			storedPath := playbook
			if storedPath == id {
				storedPath, err = memory.PlaybookPath(id)
				if err != nil {
					return detectorManifest{}, fmt.Errorf("detector %s has unsafe playbook: %w", d.ID, err)
				}
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(memoryRoot), storedPath)); err != nil {
				return detectorManifest{}, fmt.Errorf("detector %s playbook does not exist: %s", d.ID, playbook)
			}
		}
	}
	return manifest, nil
}

func validateManifestOwnership(candidate, baseline string, actor Actor, phase Phase) error {
	if actor != Responder && actor != HealthJudge {
		return nil
	}
	if phase == InitialPhase && actor == HealthJudge {
		if _, err := os.Stat(filepath.Join(baseline, ".sdo", "diagnostics", "manifest.yaml")); errors.Is(err, os.ErrNotExist) {
			after, err := readManifest(filepath.Join(candidate, ".sdo"))
			if err != nil {
				return err
			}
			for _, detector := range after.Detectors {
				if detector.Class != "health" {
					return fmt.Errorf("initial health judge cannot add %s detector %s", detector.Class, detector.ID)
				}
			}
			return nil
		}
	}
	before, err := readManifest(filepath.Join(baseline, ".sdo"))
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	after, err := readManifest(filepath.Join(candidate, ".sdo"))
	if err != nil {
		return err
	}
	if before.APIVersion != after.APIVersion || before.Kind != after.Kind || before.SDKVersion != after.SDKVersion {
		return errors.New("agent changed shared diagnostics manifest metadata")
	}
	protected := "health"
	if actor == HealthJudge {
		protected = "incident"
	}
	old := map[string]detectorRegistration{}
	for _, detector := range before.Detectors {
		old[detector.ID] = detector
	}
	seen := map[string]bool{}
	for _, detector := range after.Detectors {
		seen[detector.ID] = true
		prior, existed := old[detector.ID]
		if existed && prior.Class == protected && !reflect.DeepEqual(prior, detector) {
			return fmt.Errorf("actor %s changed protected %s detector %s", actor, protected, detector.ID)
		}
		if !existed && detector.Class == protected {
			return fmt.Errorf("actor %s added protected %s detector %s", actor, protected, detector.ID)
		}
		if existed && actor == Responder && prior.Class == "incident" && (prior.OriginatingIncident != detector.OriginatingIncident || prior.OriginatingCommit != detector.OriginatingCommit) {
			return fmt.Errorf("responder rewrote detector %s provenance", detector.ID)
		}
		if actor == Responder && detector.Class == "incident" && (!existed || !reflect.DeepEqual(prior, detector)) && detector.Persistence.Firing != 1 {
			return fmt.Errorf("new or changed incident detector %s must fire on first match", detector.ID)
		}
	}
	for _, detector := range before.Detectors {
		if detector.Class == protected && !seen[detector.ID] {
			return fmt.Errorf("actor %s removed protected %s detector %s", actor, protected, detector.ID)
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
