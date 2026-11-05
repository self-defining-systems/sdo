package deployment

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// copyDirDepth bounds the search for directories that hold the COPY sources.
const copyDirDepth = 5

type copySource struct {
	Path string
	Line int
}

var (
	heredocStart  = regexp.MustCompile(`<<-?\s*["']?([A-Za-z_][A-Za-z0-9_]*)["']?`)
	escapeComment = regexp.MustCompile("(?i)^#\\s*escape\\s*=\\s*(\\\\|`)\\s*$")
)

// parseCopySources returns the literal COPY/ADD sources of a Dockerfile that
// are resolved against the build context. It skips everything it cannot decide
// (variables, wildcards, URLs, heredocs, --from stages) so that the check can
// only reject a plan the build would reject too.
func parseCopySources(dockerfile string) []copySource {
	escape := `\`
	lines := strings.Split(strings.ReplaceAll(dockerfile, "\r\n", "\n"), "\n")
	var out []copySource
	headerDone := false
	for i := 0; i < len(lines); i++ {
		raw := strings.TrimSpace(lines[i])
		if !headerDone {
			if m := escapeComment.FindStringSubmatch(raw); m != nil {
				escape = m[1]
				continue
			}
			if raw == "" || strings.HasPrefix(raw, "#") {
				continue
			}
			headerDone = true
		}
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		start := i + 1
		statement := raw
		for strings.HasSuffix(statement, escape) {
			statement = strings.TrimSpace(strings.TrimSuffix(statement, escape))
			i++
			for i < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i]), "#") {
				i++
			}
			if i >= len(lines) {
				break
			}
			statement += " " + strings.TrimSpace(lines[i])
		}
		// Heredoc bodies are file contents, not instructions.
		var terminators []string
		for _, m := range heredocStart.FindAllStringSubmatch(statement, -1) {
			terminators = append(terminators, m[1])
		}
		for _, end := range terminators {
			for i++; i < len(lines) && strings.TrimSpace(lines[i]) != end; i++ {
			}
		}
		keyword, rest, _ := strings.Cut(statement, " ")
		if k := strings.ToUpper(keyword); (k == "COPY" || k == "ADD") && len(terminators) == 0 {
			out = append(out, instructionSources(strings.TrimSpace(rest), start)...)
		}
	}
	return out
}

func instructionSources(args string, line int) []copySource {
	// Leading --flags: a --from copies from another stage or image, never
	// from the context.
	for strings.HasPrefix(args, "--") {
		flag, rest, _ := strings.Cut(args, " ")
		if strings.HasPrefix(flag, "--from") {
			return nil
		}
		args = strings.TrimSpace(rest)
	}
	var fields []string
	if strings.HasPrefix(args, "[") {
		if json.Unmarshal([]byte(args), &fields) != nil {
			return nil
		}
	} else {
		fields = strings.Fields(args)
	}
	if len(fields) < 2 {
		return nil
	}
	var out []copySource
	for _, source := range fields[:len(fields)-1] {
		if source == "" || strings.ContainsAny(source, "$*?[") || strings.Contains(source, "://") || strings.HasPrefix(source, "git@") || strings.HasPrefix(source, "<<") {
			continue
		}
		clean := path.Clean(strings.TrimPrefix(source, "/"))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
			continue
		}
		out = append(out, copySource{Path: clean, Line: line})
	}
	return out
}

// copySourceProblem reports the Dockerfile COPY/ADD sources that do not exist
// under the build context, and the directories that do contain them all. It
// returns "" when everything exists or nothing can be decided. A source that
// exists but is excluded by .dockerignore is reported as excluded, not missing.
func copySourceProblem(checkout, label, contextRel, dockerfileRel string) string {
	data, err := os.ReadFile(filepath.Join(checkout, filepath.FromSlash(dockerfileRel)))
	if err != nil {
		return ""
	}
	sources := parseCopySources(string(data))
	if len(sources) == 0 {
		return ""
	}
	contextDir := filepath.Join(checkout, filepath.FromSlash(contextRel))
	var missing []copySource
	var excluded []string
	rules := loadDockerignore(contextDir, filepath.Join(checkout, filepath.FromSlash(dockerfileRel)))
	for _, source := range sources {
		if !existsUnder(contextDir, source.Path) {
			missing = append(missing, source)
		} else if pattern, ok := rules.Excluded(source.Path); ok {
			excluded = append(excluded, fmt.Sprintf("%q (Dockerfile line %d, excluded by %q in %s)", source.Path, source.Line, pattern, rules.File))
		}
	}
	if len(missing) == 0 && len(excluded) > 0 {
		return fmt.Sprintf("%s: the Dockerfile %q COPY/ADD sources %s exist under context_path %q but are excluded by %s, so BuildKit will not find them. Remove or narrow that pattern (or add a \"!path\" exception) in %s; do not change context_path.", label, dockerfileRel, listLimited2(excluded), contextRel, rules.File, rules.File)
	}
	if len(missing) == 0 {
		return ""
	}
	names := make([]string, 0, len(missing))
	seen := map[string]bool{}
	for _, source := range missing {
		if seen[source.Path] {
			continue
		}
		seen[source.Path] = true
		names = append(names, fmt.Sprintf("%q (Dockerfile line %d)", source.Path, source.Line))
	}
	problem := fmt.Sprintf("%s: the Dockerfile %q COPY/ADD sources %s do not exist under context_path %q, and BuildKit resolves COPY sources relative to the context.", label, dockerfileRel, listLimited2(names), contextRel)
	holders := directoriesContaining(checkout, contextRel, sources)
	if len(holders) > 0 {
		problem += " Directories that contain all of the Dockerfile's COPY sources: " + listLimited(holders) + ". Use one as context_path; dockerfile_path can stay where it is."
	} else {
		problem += " No directory in the checkout contains all of them: fix the COPY paths in the Dockerfile or add the missing files."
	}
	return problem
}

func listLimited2(values []string) string {
	if len(values) > nearbyLimit {
		return strings.Join(values[:nearbyLimit], ", ") + fmt.Sprintf(", ... (%d more)", len(values)-nearbyLimit)
	}
	return strings.Join(values, ", ")
}

func existsUnder(dir, rel string) bool {
	_, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(rel)))
	return err == nil
}

// directoriesContaining lists checkout-relative directories under which every
// source exists, nearest to the chosen context first.
func directoriesContaining(checkout, contextRel string, sources []copySource) []string {
	root := filepath.Clean(checkout)
	var found []string
	_ = filepath.WalkDir(root, func(p string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		if rel != "." && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "node_modules" || strings.Count(rel, string(filepath.Separator)) >= copyDirDepth) {
			return filepath.SkipDir
		}
		for _, source := range sources {
			if !existsUnder(p, source.Path) {
				return nil
			}
		}
		found = append(found, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(found)
	return nearestFirst(found, contextRel)
}
