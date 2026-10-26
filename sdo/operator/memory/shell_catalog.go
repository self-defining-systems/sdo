package memory

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

type shellCatalog struct{ *JSONStore }

func OpenShellCatalogReader(root string) CatalogReader { return shellCatalog{NewJSONStore(root)} }
func (s shellCatalog) Snapshot(ctx context.Context) (*contracts.OperationalMemorySnapshot, error) {
	output, err := exec.CommandContext(ctx, "git", "-C", s.root, "rev-parse", "HEAD").Output()
	if err != nil {
		return nil, err
	}
	return s.SnapshotAt(ctx, strings.TrimSpace(string(output)))
}
func (s shellCatalog) SnapshotAt(ctx context.Context, commit string) (*contracts.OperationalMemorySnapshot, error) {
	return s.JSONStore.snapshotAt(ctx, commit, CandidateShellSnapshot)
}

// CandidateShellSnapshot projects file names, notes and scripts without parsing
// front matter or Go registrations. Bytes still come from one accepted commit.
func CandidateShellSnapshot(root, commit string) (*contracts.OperationalMemorySnapshot, error) {
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: commit, CodePath: &contracts.CodePathConfig{Name: "shell"}}
	books := map[string]*contracts.MemoryPlaybook{}
	err := filepath.WalkDir(filepath.Join(root, layout.Dir), func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("shell catalog cannot follow symlinks")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("shell catalog requires regular files")
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, layout.OutcomesDir+"/") || strings.HasPrefix(rel, layout.CorpusDir+"/") || strings.HasPrefix(rel, layout.RenderedDir+"/") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > layout.MaxArtifactBytes {
			return ErrSnapshotBudget
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !utf8.Valid(data) || strings.IndexByte(string(data), 0) >= 0 {
			return fmt.Errorf("shell catalog requires UTF-8 text")
		}
		text := string(data)
		switch {
		case rel == layout.GoalFile:
			snapshot.GoalMarkdown = text
		case rel == layout.ArchitectureFile:
			snapshot.ArchitectureMarkdown = text
		case strings.HasPrefix(rel, layout.DetectorsDir+"/") && strings.HasSuffix(rel, ".sh"):
			parts := strings.Split(strings.TrimPrefix(rel, layout.DetectorsDir+"/"), "/")
			if len(parts) < 3 {
				return fmt.Errorf("shell detector needs class and ID directories")
			}
			class, owner := "health", "health_judge"
			if parts[0] == "incidents" {
				class, owner = "incident", "responder"
			} else if parts[0] != "health" {
				return fmt.Errorf("unknown shell detector class")
			}
			digest := sha256.Sum256([]byte(rel))
			excerpt := text
			if runes := []rune(text); len(runes) > 1400 {
				excerpt = string(runes[:1400])
			}
			snapshot.Detectors = append(snapshot.Detectors, &contracts.MemoryDetector{Id: fmt.Sprintf("shell.%x", digest[:8]), DetectorClass: class, Owner: owner, Firing: 2, Description: rel, PredicateExcerpt: excerpt})
		case strings.HasPrefix(rel, layout.PlaybooksDir+"/"):
			parts := strings.Split(strings.TrimPrefix(rel, layout.PlaybooksDir+"/"), "/")
			if len(parts) < 2 {
				return nil
			}
			id := parts[0]
			if !layout.ValidPlaybookID(id) {
				return fmt.Errorf("invalid shell playbook ID")
			}
			book := books[id]
			if book == nil {
				book = &contracts.MemoryPlaybook{Id: id}
				books[id] = book
			}
			switch filepath.Ext(rel) {
			case ".md":
				book.Markdown += "\n# " + rel + "\n\n" + text
			case ".sh":
				book.Scripts = append(book.Scripts, &contracts.MemoryScript{Name: strings.Join(parts[1:], "/"), Content: text})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(books))
	for id := range books {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snapshot.Playbooks = append(snapshot.Playbooks, books[id])
	}
	snapshot.PlaybookIndexMarkdown = strings.Join(ids, "\n")
	if snapshot.GoalMarkdown == "" || snapshot.ArchitectureMarkdown == "" || len(snapshot.Detectors) == 0 {
		return nil, ErrSnapshotIncomplete
	}
	if proto.Size(snapshot) > MaxSnapshotBytes {
		return nil, ErrSnapshotBudget
	}
	return snapshot, nil
}
