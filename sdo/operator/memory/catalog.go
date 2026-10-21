package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// CatalogReader is the logical diagnostics and playbook view. Only its storage
// adapter knows the current repository paths or file formats.
type CatalogReader interface {
	Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error)
}

func OpenCatalogReader(repositoryRoot string) CatalogReader { return NewJSONStore(repositoryRoot) }

var playbookIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// PlaybookID translates the current physical reference at the storage edge.
// New callers may already supply a logical ID; detector SDK shape is retained.
func PlaybookID(reference string) (string, error) {
	if playbookIDPattern.MatchString(reference) {
		return reference, nil
	}
	if strings.HasPrefix(reference, ".sdo/playbooks/") && strings.HasSuffix(reference, ".md") && !strings.Contains(strings.TrimPrefix(reference, ".sdo/playbooks/"), "/") {
		id := strings.TrimSuffix(strings.TrimPrefix(reference, ".sdo/playbooks/"), ".md")
		if playbookIDPattern.MatchString(id) {
			return id, nil
		}
	}
	const prefix, suffix = ".sdo/playbooks/", "/README.md"
	if !strings.HasPrefix(reference, prefix) || !strings.HasSuffix(reference, suffix) {
		return "", fmt.Errorf("invalid playbook reference %q", reference)
	}
	id := strings.TrimSuffix(strings.TrimPrefix(reference, prefix), suffix)
	if !playbookIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid playbook ID in %q", reference)
	}
	return id, nil
}

func PlaybookIDs(references []string) ([]string, error) {
	ids := make([]string, 0, len(references))
	for _, reference := range references {
		id, err := PlaybookID(reference)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func PlaybookPath(id string) (string, error) {
	if !playbookIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid playbook ID %q", id)
	}
	return ".sdo/playbooks/" + id + "/README.md", nil
}

// PlaybookScriptName removes the current storage prefix from a script
// reference while keeping it bound to its logical playbook.
func PlaybookScriptName(playbookID, reference string) (string, error) {
	if !playbookIDPattern.MatchString(playbookID) {
		return "", fmt.Errorf("invalid playbook ID %q", playbookID)
	}
	name := reference
	if strings.Contains(reference, "/") {
		prefix := ".sdo/playbooks/" + playbookID + "/scripts/"
		if !strings.HasPrefix(reference, prefix) {
			return "", fmt.Errorf("script %q does not belong to playbook %q", reference, playbookID)
		}
		name = strings.TrimPrefix(reference, prefix)
	}
	if name == "" || strings.Contains(name, "/") || !strings.HasSuffix(name, ".sh") || filepath.Base(name) != name {
		return "", fmt.Errorf("invalid playbook script %q", reference)
	}
	return name, nil
}

func ArtifactID(reference string) (string, error) {
	if playbookIDPattern.MatchString(reference) {
		return reference, nil
	}
	if id, err := PlaybookID(reference); err == nil {
		return id, nil
	}
	const detectorPrefix = ".sdo/diagnostics/detectors/"
	if strings.HasPrefix(reference, detectorPrefix) {
		parts := strings.Split(strings.TrimPrefix(reference, detectorPrefix), "/")
		if len(parts) == 3 && (parts[0] == "health" || parts[0] == "incidents") && detectorIDPattern.MatchString(parts[1]) && (parts[2] == "detector.go" || parts[2] == "detector_test.go") {
			return parts[1], nil
		}
	}
	return "", fmt.Errorf("unsupported operational artifact reference %q", reference)
}

// RequiresControllerRollout reports whether accepted operational-memory edits
// change the detector program that a running controller must reload.
func RequiresControllerRollout(changedPaths []string) bool {
	for _, path := range changedPaths {
		if path == ".sdo/diagnostics/manifest.yaml" || strings.HasPrefix(path, ".sdo/diagnostics/detectors/incidents/") ||
			(strings.HasSuffix(path, ".go") && (strings.HasPrefix(path, ".sdo/playbooks/") || strings.HasPrefix(path, ".sdo/lib/"))) {
			return true
		}
	}
	return false
}

type catalogManifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Detectors  []struct {
		ID      string `yaml:"id"`
		Class   string `yaml:"class"`
		Owner   string `yaml:"owner"`
		Package string `yaml:"package"`
		Watches []struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
		} `yaml:"watches"`
		Persistence struct {
			Firing int32 `yaml:"firing"`
		} `yaml:"persistence"`
		PossiblePlaybooks   []string `yaml:"possiblePlaybooks"`
		OriginatingIncident string   `yaml:"originatingIncident"`
	} `yaml:"detectors"`
}

var descriptionPattern = regexp.MustCompile(`Description:\s*"([^"]{0,500})"`)
var detectPattern = regexp.MustCompile(`func\s*\([^)]*\)\s*Detect\s*\(`)
var embeddedPlaybookPath = regexp.MustCompile(`\.sdo/playbooks/([a-z][a-z0-9_-]*)/README\.md`)
var embeddedScriptPath = regexp.MustCompile(`\.sdo/playbooks/([a-z][a-z0-9_-]*)/scripts/([a-z][a-z0-9_-]*\.sh)`)

func logicalMemoryText(value string) (string, error) {
	value = embeddedScriptPath.ReplaceAllString(value, "script:$1/$2")
	value = embeddedPlaybookPath.ReplaceAllString(value, "playbook:$1")
	if strings.Contains(value, ".sdo/") {
		return "", errors.New("physical operational memory path remains in logical worker content")
	}
	return value, nil
}

func (s *JSONStore) Snapshot(ctx context.Context) (*contracts.OperationalMemorySnapshot, error) {
	if s == nil || s.root == "" {
		return nil, errors.New("accepted repository is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return nil, fmt.Errorf("resolve accepted repository: %w", err)
	}
	git := func(args ...string) (string, error) {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("accepted memory git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	commit, err := git("rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	if len(commit) != 40 {
		return nil, errors.New("accepted memory needs a full Git commit")
	}
	if dirty, err := git("status", "--porcelain", "--", ".sdo"); err != nil {
		return nil, err
	} else if dirty != "" {
		return nil, errors.New("operational memory differs from the accepted commit")
	}
	read := func(relative string, optional bool) (string, error) {
		path := filepath.Join(root, relative)
		resolved, err := filepath.EvalSymlinks(path)
		if optional && errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve memory artifact %s: %w", relative, err)
		}
		rel, err := filepath.Rel(root, resolved)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("memory artifact %s escapes accepted repository", relative)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("memory artifact %s is not a regular file", relative)
		}
		if info.Size() > 256*1024 {
			return "", fmt.Errorf("memory artifact %s exceeds 256 KiB", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	version, err := read(".sdo/schema-version", false)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(version) != "1" {
		return nil, errors.New("unsupported operational memory version")
	}
	goal, err := read(".sdo/goal.md", false)
	if err != nil {
		return nil, err
	}
	architecture, err := read(".sdo/arch.md", false)
	if err != nil {
		return nil, err
	}
	index, err := read(".sdo/playbooks/README.md", true)
	if err != nil {
		return nil, err
	}
	manifestText, err := read(".sdo/diagnostics/manifest.yaml", false)
	if err != nil {
		return nil, err
	}
	var manifest catalogManifest
	if err := yaml.Unmarshal([]byte(manifestText), &manifest); err != nil {
		return nil, fmt.Errorf("decode diagnostics manifest: %w", err)
	}
	if manifest.APIVersion != "sdo.dev/v1alpha1" || manifest.Kind != "DetectorManifest" || len(manifest.Detectors) == 0 {
		return nil, errors.New("accepted diagnostics manifest is invalid")
	}
	goal, err = logicalMemoryText(goal)
	if err != nil {
		return nil, err
	}
	architecture, err = logicalMemoryText(architecture)
	if err != nil {
		return nil, err
	}
	_ = index // The physical Markdown index is represented by logical catalog entries below.
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: commit, GoalMarkdown: goal, ArchitectureMarkdown: architecture}
	for _, detector := range manifest.Detectors {
		if detector.ID == "" || detector.Package == "" || detector.Class == "" || detector.Owner == "" {
			return nil, errors.New("incomplete accepted detector")
		}
		packagePath := filepath.Clean(detector.Package)
		if filepath.IsAbs(packagePath) || packagePath == "." || packagePath == ".." || strings.HasPrefix(packagePath, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("detector %s package escapes diagnostics", detector.ID)
		}
		code, err := read(filepath.Join(".sdo/diagnostics", packagePath, "detector.go"), false)
		if err != nil {
			return nil, fmt.Errorf("detector %s: %w", detector.ID, err)
		}
		entry := &contracts.MemoryDetector{Id: detector.ID, DetectorClass: detector.Class, Owner: detector.Owner, Firing: detector.Persistence.Firing, OriginatingIncident: detector.OriginatingIncident, ReadsEvents: strings.Contains(code, ".Events(") || strings.Contains(code, ".RecentEventsFor(") || strings.Contains(code, "corev1.Event")}
		if match := descriptionPattern.FindStringSubmatch(code); len(match) == 2 {
			entry.Description = match[1]
		}
		predicate := code
		if start := detectPattern.FindStringIndex(code); start != nil {
			predicate = code[start[0]:]
		}
		if len(predicate) > 1400 {
			predicate = predicate[:1400]
		}
		predicate, err = logicalMemoryText(predicate)
		if err != nil {
			return nil, fmt.Errorf("detector %s: %w", detector.ID, err)
		}
		entry.PredicateExcerpt = predicate
		for _, watch := range detector.Watches {
			entry.Watches = append(entry.Watches, &contracts.MemoryWatch{ApiVersion: watch.APIVersion, Kind: watch.Kind})
		}
		for _, reference := range detector.PossiblePlaybooks {
			id, err := PlaybookID(reference)
			if err != nil {
				return nil, fmt.Errorf("detector %s: %w", detector.ID, err)
			}
			entry.PossiblePlaybookIds = append(entry.PossiblePlaybookIds, id)
		}
		snapshot.Detectors = append(snapshot.Detectors, entry)
	}
	playbookRoot := filepath.Join(root, ".sdo", "playbooks")
	entries, err := os.ReadDir(playbookRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("list accepted playbooks: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		reference, err := PlaybookPath(id)
		if err != nil {
			return nil, err
		}
		markdown, err := read(reference, false)
		if err != nil {
			return nil, err
		}
		markdown, err = logicalMemoryText(markdown)
		if err != nil {
			return nil, fmt.Errorf("playbook %s: %w", id, err)
		}
		playbook := &contracts.MemoryPlaybook{Id: id, Markdown: markdown}
		scriptRoot := filepath.Join(playbookRoot, id, "scripts")
		scripts, err := os.ReadDir(scriptRoot)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("list scripts for %s: %w", id, err)
		}
		for _, script := range scripts {
			if !strings.HasSuffix(script.Name(), ".sh") {
				return nil, fmt.Errorf("unsupported playbook script %q", script.Name())
			}
			content, err := read(filepath.Join(".sdo/playbooks", id, "scripts", script.Name()), false)
			if err != nil {
				return nil, err
			}
			content, err = logicalMemoryText(content)
			if err != nil {
				return nil, fmt.Errorf("script %s/%s: %w", id, script.Name(), err)
			}
			playbook.Scripts = append(playbook.Scripts, &contracts.MemoryScript{Name: script.Name(), Content: content})
		}
		snapshot.Playbooks = append(snapshot.Playbooks, playbook)
	}
	sort.Slice(snapshot.Playbooks, func(i, j int) bool { return snapshot.Playbooks[i].Id < snapshot.Playbooks[j].Id })
	indexLines := []string{"# Playbook catalog", ""}
	for _, playbook := range snapshot.Playbooks {
		indexLines = append(indexLines, "- "+playbook.Id)
	}
	snapshot.PlaybookIndexMarkdown = strings.Join(indexLines, "\n") + "\n"
	if proto.Size(snapshot) > 512*1024 {
		return nil, errors.New("operational memory snapshot exceeds worker task budget")
	}
	if finalCommit, err := git("rev-parse", "HEAD"); err != nil {
		return nil, err
	} else if finalCommit != commit {
		return nil, errors.New("accepted commit changed while reading operational memory")
	}
	if dirty, err := git("status", "--porcelain", "--", ".sdo"); err != nil {
		return nil, err
	} else if dirty != "" {
		return nil, errors.New("operational memory changed while building worker snapshot")
	}
	return snapshot, nil
}
