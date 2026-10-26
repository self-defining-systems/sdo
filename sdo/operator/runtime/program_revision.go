package runtime

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var programIDPath = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
var programModule = regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`)

func fillProgramRevisions(ctx context.Context, runs []ProgramRun, repository, commit string) {
	cache := map[string]string{}
	for index := range runs {
		if strings.HasPrefix(runs[index].ProgramRevision, workingTreeRevisionPrefix) {
			continue
		}
		id := runs[index].ProgramID
		revision, ok := cache[id]
		if !ok {
			revision = commit
			if programIDPath.MatchString(id) && gitRevision.MatchString(commit) {
				if resolved, err := programDependencyRevision(ctx, repository, commit, id); err == nil {
					revision = resolved
				}
			}
			cache[id] = revision
		}
		runs[index].ProgramRevision = revision
	}
}

// Hash accepted Git objects, never mutable checkout files. Only libraries in
// this program's import closure participate, including dependencies of helpers.
func programDependencyRevision(ctx context.Context, repository, commit, id string) (string, error) {
	git := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).Output()
	}
	module := "app-diagnostics"
	if data, err := git("show", commit+":.sdo/diagnostics/go.mod"); err == nil {
		if match := programModule.FindSubmatch(data); len(match) == 2 {
			module = string(match[1])
		}
	}
	return dependencyRevision(module, id, func(directory string) ([]revisionSource, error) {
		listing, err := git("ls-tree", "-r", "-z", commit, "--", directory+"/")
		if err != nil {
			return nil, err
		}
		var files []revisionSource
		for _, entry := range strings.Split(string(listing), "\x00") {
			if entry == "" {
				continue
			}
			header, file, ok := strings.Cut(entry, "\t")
			fields := strings.Fields(header)
			if !ok || len(fields) != 3 || fields[1] != "blob" {
				return nil, fmt.Errorf("invalid dependency entry %q", entry)
			}
			item := revisionSource{name: file, digest: fields[2]}
			if strings.HasSuffix(file, ".go") && !strings.HasSuffix(file, "_test.go") {
				item.data, err = git("show", commit+":"+file)
				if err != nil {
					return nil, err
				}
			}
			files = append(files, item)
		}
		return files, nil
	})
}

type revisionSource struct {
	name, digest string
	data         []byte
}

// workingProgramRevision hashes exactly the same import closure as accepted Git
// objects. root is an immutable staged compiler workspace (without .sdo).
func workingProgramRevision(root, module, id string) (string, error) {
	return dependencyRevision(module, id, func(directory string) ([]revisionSource, error) {
		relative := strings.TrimPrefix(directory, ".sdo/")
		var files []revisionSource
		err := filepath.WalkDir(filepath.Join(root, filepath.FromSlash(relative)), func(file string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("program source cannot be a symlink")
			}
			if entry.IsDir() {
				return nil
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, file)
			if err != nil {
				return err
			}
			h := sha1.New()
			fmt.Fprintf(h, "blob %d\x00", len(data))
			_, _ = h.Write(data)
			files = append(files, revisionSource{name: ".sdo/" + filepath.ToSlash(rel), digest: fmt.Sprintf("%x", h.Sum(nil)), data: data})
			return nil
		})
		return files, err
	})
}

func dependencyRevision(module, id string, list func(string) ([]revisionSource, error)) (string, error) {
	pending := []string{".sdo/playbooks/" + id}
	visited := map[string]bool{}
	sources := map[string]string{}
	for len(pending) > 0 {
		directory := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if visited[directory] {
			continue
		}
		visited[directory] = true
		files, err := list(directory)
		if err != nil {
			return "", err
		}
		if len(files) == 0 {
			return "", fmt.Errorf("missing program dependency %s", directory)
		}
		for _, item := range files {
			file := item.name
			if ext := strings.ToLower(path.Ext(file)); ext == ".md" || ext == ".markdown" || strings.HasSuffix(file, "_test.go") {
				continue
			}
			sources[file] = item.digest
			if !strings.HasSuffix(file, ".go") {
				continue
			}
			data := item.data
			parsed, err := parser.ParseFile(token.NewFileSet(), file, data, parser.ImportsOnly)
			if err != nil {
				return "", err
			}
			for _, imported := range parsed.Imports {
				name, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return "", err
				}
				if relative, ok := strings.CutPrefix(name, module+"/lib/"); ok {
					if relative == "" || path.Clean(relative) != relative || strings.HasPrefix(relative, "../") {
						return "", fmt.Errorf("invalid library import %q", name)
					}
					pending = append(pending, ".sdo/lib/"+relative)
				}
			}
		}
	}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		fmt.Fprintf(hash, "%s\x00%s\x00", name, sources[name])
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
