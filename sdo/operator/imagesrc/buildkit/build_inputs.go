package buildkit

import (
	"encoding/json"
	"io/fs"
	"path"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
)

// BuildInputs implements imagesrc.InputLister: the tree paths a build of
// config reads. They are the Dockerfile, the ignore files BuildKit reads
// (<Dockerfile>.dockerignore and the context's .dockerignore), and the context
// paths named by the Dockerfile's COPY and ADD instructions. Whatever cannot be
// decided from the Dockerfile text (a copy of the whole context, a variable or
// wildcard source, a heredoc copy, a RUN bind mount of the context) makes the
// whole context an input, so a change that can reach the image is never
// missed.
func (Builder) BuildInputs(fsys fs.FS, config *contracts.BuildKitImageConfig) (imagesrc.BuildInputs, error) {
	contextRel := cleanTreePath(config.GetContextPath())
	dockerfileRel := cleanTreePath(config.GetDockerfilePath())
	// The docker-CLI reading of a context-relative Dockerfile (PlanBuilds'
	// dockerfileInContext) applies when the checkout-root reading is absent.
	if _, err := fs.Stat(fsys, dockerfileRel); err != nil && contextRel != "." {
		if inContext := path.Join(contextRel, dockerfileRel); fileExists(fsys, inContext) {
			dockerfileRel = inContext
		}
	}
	paths := map[string]bool{dockerfileRel: true, dockerfileRel + ".dockerignore": true, path.Join(contextRel, ".dockerignore"): true}
	data, err := fs.ReadFile(fsys, dockerfileRel)
	if err != nil {
		return imagesrc.BuildInputs{}, err
	}
	sources, whole := contextReads(string(data))
	if whole {
		paths[contextRel] = true
	}
	for _, source := range sources {
		paths[path.Join(contextRel, source)] = true
	}
	out := make([]string, 0, len(paths))
	for p := range paths {
		out = append(out, p)
	}
	sort.Strings(out)
	return imagesrc.BuildInputs{Paths: out}, nil
}

// contextReads returns the context-relative paths a Dockerfile's instructions
// read from the build context, and whole when some read cannot be decided.
func contextReads(dockerfile string) (sources []string, whole bool) {
	forEachInstruction(dockerfile, func(keyword, rest string, _ int, heredoc bool) {
		switch keyword {
		case "COPY", "ADD":
			fromStage, fields := copyFields(rest)
			if fromStage {
				return
			}
			if heredoc || len(fields) < 2 {
				whole = true
				return
			}
			for _, source := range fields[:len(fields)-1] {
				if strings.Contains(source, "://") || strings.HasPrefix(source, "git@") {
					continue
				}
				clean := path.Clean(strings.TrimPrefix(source, "/"))
				if strings.ContainsAny(source, "$*?[") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
					whole = true
					continue
				}
				sources = append(sources, clean)
			}
		case "RUN":
			if contextBindMount(rest) {
				whole = true
			}
		}
	})
	return sources, whole
}

// copyFields strips the leading flags of a COPY/ADD and returns its sources
// and destination; fromStage is a copy from another stage or image.
func copyFields(args string) (fromStage bool, fields []string) {
	for strings.HasPrefix(args, "--") {
		flag, rest, _ := strings.Cut(args, " ")
		if strings.HasPrefix(flag, "--from") {
			return true, nil
		}
		args = strings.TrimSpace(rest)
	}
	if strings.HasPrefix(args, "[") {
		if json.Unmarshal([]byte(args), &fields) != nil {
			return false, nil
		}
		return false, fields
	}
	return false, strings.Fields(args)
}

// contextBindMount reports a RUN --mount that binds the build context: type
// bind (the default type) without from=.
func contextBindMount(args string) bool {
	for _, field := range strings.Fields(args) {
		if !strings.HasPrefix(field, "--mount=") {
			if !strings.HasPrefix(field, "--") {
				return false
			}
			continue
		}
		kind, from := "bind", false
		for _, option := range strings.Split(strings.TrimPrefix(field, "--mount="), ",") {
			key, value, _ := strings.Cut(option, "=")
			switch key {
			case "type":
				kind = value
			case "from":
				from = true
			}
		}
		if kind == "bind" && !from {
			return true
		}
	}
	return false
}

func cleanTreePath(p string) string {
	clean := path.Clean(strings.TrimPrefix(strings.TrimSpace(p), "/"))
	if clean == "" {
		return "."
	}
	return clean
}

func fileExists(fsys fs.FS, p string) bool {
	stat, err := fs.Stat(fsys, p)
	return err == nil && stat.Mode().IsRegular()
}
