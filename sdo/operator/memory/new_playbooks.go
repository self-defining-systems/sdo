package memory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
)

// NewPlaybooks names the playbooks a change introduced: those with program
// source (a non-test .go file) in root and none at base. Editing an accepted
// playbook, or a README with no program, does not make one new. Without Git
// history it returns an error, so an unattributable change is never judged new.
func NewPlaybooks(ctx context.Context, root, base string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(root, layout.PlaybooksDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var fresh []string
	for _, entry := range entries {
		if !entry.IsDir() || !layout.ValidPlaybookID(entry.Name()) {
			continue
		}
		dir := layout.PlaybookDir(entry.Name())
		if !hasProgramSource(filepath.Join(root, dir)) {
			continue
		}
		listing, stderr, err := memoryio.GitWithDiagnostics(ctx, root, "ls-tree", "-r", "--name-only", base, "--", dir+"/")
		if err != nil {
			return nil, fmt.Errorf("list %s at %s: %w: %s", dir, base, err, strings.TrimSpace(stderr))
		}
		if !slices.ContainsFunc(strings.Split(string(listing), "\n"), isProgramFile) {
			fresh = append(fresh, entry.Name())
		}
	}
	return fresh, nil
}

func isProgramFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

func hasProgramSource(dir string) bool {
	files, _ := os.ReadDir(dir)
	return slices.ContainsFunc(files, func(file os.DirEntry) bool { return !file.IsDir() && isProgramFile(file.Name()) })
}

// ChangedPlaybooks names the playbooks whose program source (a non-test .go
// file) differs from base in root: the new ones and the ones edited since,
// committed or not. A README or test edit changes no program. Without Git
// history it returns an error.
func ChangedPlaybooks(ctx context.Context, root, base string) ([]string, error) {
	dir := layout.PlaybooksDir + "/"
	changed := map[string]bool{}
	for _, args := range [][]string{{"diff", "--name-only", base, "--", dir}, {"ls-files", "--others", "--exclude-standard", "--", dir}} {
		listing, stderr, err := memoryio.GitWithDiagnostics(ctx, root, args...)
		if err != nil {
			return nil, fmt.Errorf("list changed playbooks at %s: %w: %s", base, err, strings.TrimSpace(stderr))
		}
		for _, name := range strings.Split(string(listing), "\n") {
			rest, ok := strings.CutPrefix(name, dir)
			id, file, nested := strings.Cut(rest, "/")
			if ok && nested && !strings.Contains(file, "/") && isProgramFile(file) && layout.ValidPlaybookID(id) {
				changed[id] = true
			}
		}
	}
	var ids []string
	for id := range changed {
		if hasProgramSource(filepath.Join(root, layout.PlaybookDir(id))) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}
