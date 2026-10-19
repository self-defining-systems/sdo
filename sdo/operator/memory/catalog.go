package memory

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// CatalogReader is the logical diagnostics and playbook view. Only its storage
// adapter knows the current repository paths or file formats.
type CatalogReader interface {
	Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error)
}

func OpenCatalogReader(repositoryRoot string) CatalogReader { return NewJSONStore(repositoryRoot) }

var descriptionPattern = regexp.MustCompile(`Description:\s*"([^"]{0,500})"`)
var detectPattern = regexp.MustCompile(`func\s*\([^)]*\)\s*Detect\s*\(`)

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
	if dirty, err := git("status", "--porcelain", "--", layout.Dir); err != nil {
		return nil, err
	} else if dirty != "" {
		return nil, errors.New("operational memory differs from the accepted commit")
	}
	tree, err := layout.ReadTree(root)
	if err != nil {
		return nil, fmt.Errorf("read accepted operational memory: %w", err)
	}
	if tree.Goal == nil || tree.Architecture == nil || len(tree.Detectors) == 0 {
		return nil, errors.New("accepted operational memory needs a goal, an architecture, and detectors")
	}
	goal, err := layout.LogicalText(tree.Goal.Source)
	if err != nil {
		return nil, err
	}
	architecture, err := layout.LogicalText(tree.Architecture.Source)
	if err != nil {
		return nil, err
	}
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: commit, GoalMarkdown: goal, ArchitectureMarkdown: architecture}
	for _, detector := range tree.Detectors {
		manifest := detector.Manifest
		code := ""
		for _, file := range detector.Files {
			if file.Name == layout.DetectorFileName {
				code = string(file.Content)
			}
		}
		if code == "" {
			return nil, fmt.Errorf("detector %s has no %s", manifest.ID, layout.DetectorFileName)
		}
		entry := &contracts.MemoryDetector{Id: manifest.ID, DetectorClass: manifest.Class, Owner: manifest.Owner, Firing: int32(manifest.Persistence.Firing), OriginatingIncident: manifest.OriginatingIncident, ReadsEvents: strings.Contains(code, ".Events(") || strings.Contains(code, ".RecentEventsFor(") || strings.Contains(code, "corev1.Event")}
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
		predicate, err = layout.LogicalText(predicate)
		if err != nil {
			return nil, fmt.Errorf("detector %s: %w", manifest.ID, err)
		}
		entry.PredicateExcerpt = predicate
		for _, watch := range manifest.Watches {
			entry.Watches = append(entry.Watches, &contracts.MemoryWatch{ApiVersion: watch.APIVersion, Kind: watch.Kind})
		}
		entry.PossiblePlaybookIds = append(entry.PossiblePlaybookIds, manifest.PossiblePlaybooks...)
		snapshot.Detectors = append(snapshot.Detectors, entry)
	}
	for _, source := range tree.Playbooks {
		markdown, err := layout.LogicalText(source.Document.Source)
		if err != nil {
			return nil, fmt.Errorf("playbook %s: %w", source.ID, err)
		}
		playbook := &contracts.MemoryPlaybook{Id: source.ID, Markdown: markdown}
		for _, script := range source.Scripts {
			content, err := layout.LogicalText(string(script.Content))
			if err != nil {
				return nil, fmt.Errorf("script %s/%s: %w", source.ID, script.Name, err)
			}
			playbook.Scripts = append(playbook.Scripts, &contracts.MemoryScript{Name: script.Name, Content: content})
		}
		snapshot.Playbooks = append(snapshot.Playbooks, playbook)
	}
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
	if dirty, err := git("status", "--porcelain", "--", layout.Dir); err != nil {
		return nil, err
	} else if dirty != "" {
		return nil, errors.New("operational memory changed while building worker snapshot")
	}
	return snapshot, nil
}
