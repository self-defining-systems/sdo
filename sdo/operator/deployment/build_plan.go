package deployment

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	// nearbyLimit caps each suggestion list in a rejection so feedback stays
	// readable for large monorepos.
	nearbyLimit = 12
	// nearbyDepth bounds the Dockerfile search below the checkout.
	nearbyDepth = 4
)

// plannedBuild is an image build whose context and Dockerfile are proven to
// exist in the checkout. Relative paths are checkout-relative and slash-form.
type plannedBuild struct {
	Build         *contracts.ImageBuild
	ContextRel    string
	DockerfileRel string
}

// planImageBuilds validates every image build against the checkout before any
// build starts, so a wrong path costs no build time and the deployer learns
// every wrong path at once.
func planImageBuilds(checkout string, builds []*contracts.ImageBuild) ([]plannedBuild, error) {
	planned := make([]plannedBuild, 0, len(builds))
	seen := map[string]bool{}
	var problems []string
	for index, build := range builds {
		if build == nil || build.Image == "" || build.ContextPath == "" || build.DockerfilePath == "" {
			return nil, errors.New("image build needs image, context, and Dockerfile")
		}
		if seen[build.Image] {
			return nil, fmt.Errorf("duplicate image build %q", build.Image)
		}
		seen[build.Image] = true
		label := fmt.Sprintf("image_builds[%d] (%s)", index, build.Image)
		contextRel, contextProblem := resolveBuildPath(checkout, build.ContextPath, label+" context_path", true)
		dockerfilePath := build.DockerfilePath
		if contextProblem == "" {
			dockerfilePath = dockerfileInContext(checkout, contextRel, build.DockerfilePath)
		}
		dockerfileRel, dockerfileProblem := resolveBuildPath(checkout, dockerfilePath, label+" dockerfile_path", false)
		if contextProblem != "" {
			problems = append(problems, contextProblem)
		}
		if dockerfileProblem != "" {
			problems = append(problems, dockerfileProblem)
		}
		if contextProblem != "" || dockerfileProblem != "" {
			continue
		}
		if problem := copySourceProblem(checkout, label, contextRel, dockerfileRel); problem != "" {
			problems = append(problems, problem)
			continue
		}
		planned = append(planned, plannedBuild{Build: build, ContextRel: contextRel, DockerfileRel: dockerfileRel})
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("the plan's image_builds do not match the checkout, so no build was started. All paths are relative to the checkout root. %s", strings.Join(problems, " "))
	}
	return planned, nil
}

// dockerfileInContext applies the one convention for dockerfile_path: it is
// relative to the checkout root, like context_path. A path that does not exist
// there but does exist below the build context is the docker-CLI habit of
// naming the Dockerfile relative to the context; that reading is unambiguous
// and is accepted. When both exist the checkout-root reading wins.
func dockerfileInContext(checkout, contextRel, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || filepath.IsAbs(trimmed) || contextRel == "." {
		return raw
	}
	for _, candidate := range []string{raw, trimmed} {
		if _, err := os.Stat(filepath.Join(checkout, candidate)); err == nil {
			return raw
		}
	}
	inContext := filepath.Join(contextRel, trimmed)
	if stat, err := os.Stat(filepath.Join(checkout, inContext)); err == nil && stat.Mode().IsRegular() {
		return inContext
	}
	return raw
}

// resolveBuildPath returns the checkout-relative form of raw. Surrounding
// whitespace is dropped only when the literal path does not exist and the
// trimmed one does, which cannot change the meaning of a valid plan.
func resolveBuildPath(checkout, raw, label string, wantDirectory bool) (string, string) {
	candidate := raw
	if trimmed := strings.TrimSpace(raw); trimmed != raw {
		if _, err := os.Lstat(filepath.Join(checkout, raw)); err != nil && trimmed != "" {
			candidate = trimmed
		}
	}
	resolved, err := safeCandidatePath(checkout, candidate)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", missingPathProblem(checkout, raw, label, wantDirectory)
		}
		return "", fmt.Sprintf("%s %q is not usable: %v.", label, raw, err)
	}
	base, err := filepath.EvalSymlinks(checkout)
	if err != nil {
		return "", fmt.Sprintf("%s %q: %v.", label, raw, err)
	}
	stat, err := os.Stat(resolved)
	if err != nil {
		return "", missingPathProblem(checkout, raw, label, wantDirectory)
	}
	switch {
	case wantDirectory && !stat.IsDir():
		return "", fmt.Sprintf("%s %q is a file, not a directory. The build context is a directory; the Dockerfile goes in dockerfile_path.%s", label, raw, nearbyDirectories(checkout, raw))
	case !wantDirectory && !stat.Mode().IsRegular():
		return "", fmt.Sprintf("%s %q is a directory, not a file. Name the Dockerfile itself.%s", label, raw, nearbyDockerfiles(checkout, raw))
	}
	rel, err := filepath.Rel(base, resolved)
	if err != nil {
		return "", fmt.Sprintf("%s %q: %v.", label, raw, err)
	}
	return filepath.ToSlash(rel), ""
}

func missingPathProblem(checkout, raw, label string, wantDirectory bool) string {
	hint := ""
	if wantDirectory {
		hint = nearbyDirectories(checkout, raw)
	} else {
		hint = nearbyDockerfiles(checkout, raw)
	}
	note := ""
	if strings.TrimSpace(raw) != raw {
		note = " It has leading or trailing whitespace and the trimmed path does not exist either."
	}
	kind := "directory"
	if !wantDirectory {
		kind = "file"
	}
	if !wantDirectory {
		note += " dockerfile_path is relative to the checkout root, not to the build context (for context services/api write services/api/Dockerfile)."
	}
	return fmt.Sprintf("%s %q does not exist in the checkout (expected a %s).%s%s", label, raw, kind, note, hint)
}

// nearbyDirectories lists the subdirectories of the deepest existing ancestor
// of raw, then the directories that hold a Dockerfile.
func nearbyDirectories(checkout, raw string) string {
	parent := deepestExistingDirectory(checkout, raw)
	out := ""
	if entries, err := os.ReadDir(filepath.Join(checkout, parent)); err == nil {
		var names []string
		for _, entry := range entries {
			if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
				names = append(names, filepath.ToSlash(filepath.Join(parent, entry.Name())))
			}
		}
		if len(names) > 0 {
			out += fmt.Sprintf(" Directories under %q: %s.", parent, listLimited(names))
		}
	}
	var holders []string
	for _, dockerfile := range findDockerfiles(checkout) {
		holders = append(holders, filepath.ToSlash(filepath.Dir(dockerfile)))
	}
	if len(holders) > 0 {
		out += " Directories containing a Dockerfile: " + listLimited(uniqueSorted(holders)) + "."
	}
	return out
}

func nearbyDockerfiles(checkout, raw string) string {
	found := findDockerfiles(checkout)
	if len(found) == 0 {
		return " The checkout contains no Dockerfile."
	}
	return " Dockerfiles in the checkout: " + listLimited(nearestFirst(found, raw)) + "."
}

func deepestExistingDirectory(checkout, raw string) string {
	current := filepath.Clean(strings.TrimSpace(raw))
	for current != "." && current != string(filepath.Separator) && current != "" {
		if stat, err := os.Stat(filepath.Join(checkout, current)); err == nil && stat.IsDir() {
			return current
		}
		current = filepath.Dir(current)
	}
	return "."
}

func findDockerfiles(checkout string) []string {
	var found []string
	root := filepath.Clean(checkout)
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if entry.IsDir() {
			if rel != "." && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "node_modules" || strings.Count(rel, string(filepath.Separator)) >= nearbyDepth) {
				return filepath.SkipDir
			}
			return nil
		}
		name := entry.Name()
		if name == "Dockerfile" || strings.HasPrefix(name, "Dockerfile.") || strings.HasSuffix(name, ".Dockerfile") {
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(found)
	return found
}

// nearestFirst orders paths by how many leading directories they share with
// raw, so the suggestions closest to the wrong guess come first.
func nearestFirst(paths []string, raw string) []string {
	guess := strings.Split(filepath.ToSlash(filepath.Clean(strings.TrimSpace(raw))), "/")
	shared := func(path string) int {
		count := 0
		for i, part := range strings.Split(path, "/") {
			if i >= len(guess) || guess[i] != part {
				break
			}
			count++
		}
		return count
	}
	ordered := append([]string(nil), paths...)
	sort.SliceStable(ordered, func(i, j int) bool { return shared(ordered[i]) > shared(ordered[j]) })
	return ordered
}

func uniqueSorted(values []string) []string {
	sort.Strings(values)
	out := values[:0]
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			out = append(out, value)
		}
	}
	return out
}

func listLimited(values []string) string {
	quoted := make([]string, 0, nearbyLimit)
	for i, value := range values {
		if i == nearbyLimit {
			quoted = append(quoted, fmt.Sprintf("... (%d more)", len(values)-nearbyLimit))
			break
		}
		quoted = append(quoted, fmt.Sprintf("%q", value))
	}
	return strings.Join(quoted, ", ")
}
