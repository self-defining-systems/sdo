package rendered

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strings"

	"github.com/moby/patternmatcher"
	"github.com/moby/patternmatcher/ignorefile"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Validate checks repository-relative, literal build paths. External images
// explicitly carry their source reference and no local build paths.
func (i ImageInputs) Validate() error {
	if i.SourceImage == "" {
		if i.ContextPath != "" || i.DockerfilePath != "" {
			return errors.New("build inputs require a source image")
		}
		return nil
	}
	if i.ContextPath == "" && i.DockerfilePath == "" {
		return nil
	}
	for _, p := range []string{i.ContextPath, i.DockerfilePath} {
		if p == "" || path.IsAbs(p) || path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") || strings.ContainsAny(p, "\\\x00") || p == ".sdo" || strings.HasPrefix(p, ".sdo/") {
			return fmt.Errorf("invalid image build input path %q", p)
		}
	}
	if i.DockerfilePath == "." {
		return errors.New("Dockerfile must be a file")
	}
	return nil
}

// SameImageInputs compares only a trusted image declaration's local build
// context and Dockerfile. Unknown declarations preserve the conservative rule.
// Ignore files and Dockerfiles always affect identity, even if excluded.
func (s Source) SameImageInputs(ctx context.Context, left, right string, inputs ImageInputs) (bool, error) {
	if err := inputs.Validate(); err != nil {
		return false, err
	}
	if inputs.SourceImage == "" {
		return s.SameInputs(ctx, left, right)
	}
	if !gitRevision.MatchString(left) || !gitRevision.MatchString(right) {
		return false, errors.New("full source revisions required")
	}
	if inputs.ContextPath == "" {
		return true, nil
	}
	// Reject symlinked or missing context/Dockerfile declarations. Read only Git
	// objects: checkout edits cannot change the identity being authorized.
	for _, revision := range []string{left, right} {
		for _, item := range []struct{ path, kind string }{{inputs.ContextPath, "tree"}, {inputs.DockerfilePath, "blob"}} {
			spec := revision + ":" + item.path
			if item.path == "." {
				spec = revision + "^{tree}"
			}
			data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "cat-file", "-t", spec).Output()
			if err != nil || strings.TrimSpace(string(data)) != item.kind {
				return false, fmt.Errorf("image input unavailable at %s:%s", revision, item.path)
			}
			if item.kind == "blob" {
				data, err = exec.CommandContext(ctx, "git", "-C", s.Repository, "ls-tree", revision, "--", item.path).Output()
				if err != nil || strings.HasPrefix(string(data), "120000") {
					return false, errors.New("Dockerfile must be a regular Git file")
				}
			}
		}
	}
	ignorePath := path.Join(inputs.ContextPath, ".dockerignore")
	specific := inputs.DockerfilePath + ".dockerignore"
	read := func(revision, p string) ([]byte, bool, error) {
		listing, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "ls-tree", revision, "--", p).Output()
		if err != nil {
			return nil, false, err
		}
		if len(listing) == 0 {
			return nil, false, nil
		}
		if !strings.HasPrefix(string(listing), "100644 ") && !strings.HasPrefix(string(listing), "100755 ") {
			return nil, false, errors.New("ignore file must be a regular Git file")
		}
		data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "show", revision+":"+p).Output()
		return data, true, err
	}
	data, specificFound, err := read(left, specific)
	if err != nil {
		return false, err
	}
	if specificFound {
		ignorePath = specific
	} else {
		data, _, err = read(left, ignorePath)
		if err != nil {
			return false, err
		}
	}
	patterns, err := ignorefile.ReadAll(strings.NewReader(string(data)))
	if err != nil {
		return false, err
	}
	matcher, err := patternmatcher.New(patterns)
	if err != nil {
		return false, err
	}
	changed, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "diff", "--no-renames", "--name-only", "-z", left, right, "--").Output()
	if err != nil {
		return false, err
	}
	for _, p := range strings.Split(string(changed), "\x00") {
		if p == "" {
			continue
		}
		if p == inputs.DockerfilePath || p == specific || p == path.Join(inputs.ContextPath, ".dockerignore") {
			return false, nil
		}
		if p == ".sdo" || strings.HasPrefix(p, ".sdo/") {
			continue
		}
		relative := p
		if inputs.ContextPath != "." {
			prefix := inputs.ContextPath + "/"
			if !strings.HasPrefix(p, prefix) {
				continue
			}
			relative = strings.TrimPrefix(p, prefix)
		}
		excluded, err := matcher.MatchesOrParentMatches(relative)
		if err != nil {
			return false, err
		}
		if !excluded {
			return false, nil
		}
	}
	return true, nil
}

// SameWorkloadInputs requires unchanged inputs for every named regular/init
// image. The all-source rule remains in force without trusted scoped evidence.
func (s Source) SameWorkloadInputs(ctx context.Context, left, right string, expected *unstructured.Unstructured, evidence []DeploymentEvidence) (bool, error) {
	if len(evidence) == 0 {
		return s.SameInputs(ctx, left, right)
	}
	images, err := containerImages(expected)
	if err != nil {
		return false, err
	}
	for _, image := range images {
		found := false
		for _, proof := range evidence {
			deployed, ok := proof.Image(objectRef(expected), image.Name, image.Init)
			if !ok {
				continue
			}
			if deployed.Inputs.SourceImage != "" && deployed.Inputs.SourceImage != image.Digest {
				continue
			}
			same, err := s.SameImageInputs(ctx, left, right, deployed.Inputs)
			if err != nil {
				return false, err
			}
			if same {
				found = true
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}
