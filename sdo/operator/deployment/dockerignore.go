package deployment

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// dockerignoreRules is a context's .dockerignore. BuildKit reads
// <Dockerfile>.dockerignore next to the Dockerfile in preference to the
// context's .dockerignore. Excluded files exist on disk but are absent from
// the build context, so a COPY of one fails exactly like a missing file.
type dockerignoreRules struct {
	File     string
	patterns []ignorePattern
}

type ignorePattern struct {
	Text    string
	negate  bool
	matcher *regexp.Regexp
}

func loadDockerignore(contextDir, dockerfileAbs string) dockerignoreRules {
	for _, candidate := range []string{dockerfileAbs + ".dockerignore", filepath.Join(contextDir, ".dockerignore")} {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		rules := dockerignoreRules{File: filepath.Base(candidate)}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			negate := strings.HasPrefix(line, "!")
			text := strings.TrimSpace(strings.TrimPrefix(line, "!"))
			clean := strings.TrimPrefix(path.Clean(filepath.ToSlash(text)), "/")
			if clean == "" || clean == "." {
				continue
			}
			if matcher, err := regexp.Compile(globRegexp(clean)); err == nil {
				rules.patterns = append(rules.patterns, ignorePattern{Text: line, negate: negate, matcher: matcher})
			}
		}
		return rules
	}
	return dockerignoreRules{}
}

func globRegexp(glob string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(glob); i++ {
		switch c := glob[i]; c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				i++
				if i+1 < len(glob) && glob[i+1] == '/' {
					i++
					b.WriteString("(.*/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '\\':
			if i+1 < len(glob) {
				i++
				b.WriteString(regexp.QuoteMeta(string(glob[i])))
			}
		case '[', ']':
			b.WriteByte(c)
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return b.String()
}

// Excluded returns the deciding pattern when rel (context-relative, slash
// separated) is excluded: the last matching pattern wins, and a pattern that
// matches a parent directory excludes everything below it.
func (r dockerignoreRules) Excluded(rel string) (string, bool) {
	rel = strings.TrimPrefix(path.Clean(rel), "/")
	excluded, deciding := false, ""
	for _, pattern := range r.patterns {
		matched := false
		for p := rel; p != "." && p != "/" && p != ""; p = path.Dir(p) {
			if pattern.matcher.MatchString(p) {
				matched = true
				break
			}
		}
		if matched {
			excluded, deciding = !pattern.negate, pattern.Text
		}
	}
	return deciding, excluded
}
