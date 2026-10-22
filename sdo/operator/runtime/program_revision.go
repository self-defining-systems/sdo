package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"go/parser"
	"go/token"
	"os/exec"
	"path"
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
		listing, err := git("ls-tree", "-r", "-z", commit, "--", directory+"/")
		if err != nil {
			return "", err
		}
		if len(listing) == 0 {
			return "", fmt.Errorf("missing program dependency %s", directory)
		}
		for _, entry := range strings.Split(string(listing), "\x00") {
			if entry == "" {
				continue
			}
			header, file, ok := strings.Cut(entry, "\t")
			fields := strings.Fields(header)
			if !ok || len(fields) != 3 || fields[1] != "blob" {
				return "", fmt.Errorf("invalid dependency entry %q", entry)
			}
			if ext := strings.ToLower(path.Ext(file)); ext == ".md" || ext == ".markdown" || strings.HasSuffix(file, "_test.go") {
				continue
			}
			sources[file] = fields[2]
			if !strings.HasSuffix(file, ".go") {
				continue
			}
			data, err := git("show", commit+":"+file)
			if err != nil {
				return "", err
			}
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
