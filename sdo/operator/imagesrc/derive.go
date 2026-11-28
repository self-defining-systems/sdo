package imagesrc

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/known/anypb"
)

// SetBuildDerivation names the source that builds an image the tree has a
// Dockerfile for, and how its config is made from the build context and the
// Dockerfile path. Without it Derive declares nothing.
func (c *Catalog) SetBuildDerivation(sourceID string, config func(contextPath, dockerfile string) (*anypb.Any, error)) {
	c.buildID, c.buildConfig = sourceID, config
}

// Derive returns declared plus one build declaration (in repository form) for
// every image under the trusted push host that declared does not cover: the
// tree's files say how to build it (BuildFor). An image with no Dockerfile is
// left undeclared, for ResolveAll to refuse with its reason.
func (c *Catalog) Derive(images []string, declared []Declared, fsys fs.FS) ([]Declared, error) {
	out := slices.Clone(declared)
	if c == nil || c.buildID == "" || c.pushHost == "" {
		return out, nil
	}
	for _, image := range images {
		if registryHost(image) != c.pushHost {
			continue
		}
		if _, ok := Match(out, image); ok {
			continue
		}
		contextPath, dockerfile, err := BuildFor(fsys, image)
		if err != nil {
			return nil, err
		}
		config, err := c.buildConfig(contextPath, dockerfile)
		if err != nil {
			return nil, fmt.Errorf("imagesrc: image %q: %w", image, err)
		}
		out = append(out, Declared{Image: Repository(image), SourceID: c.buildID, Config: config})
	}
	return out, nil
}

// BuildFor finds how the tree builds image, by the image's name N (the last
// path segment of its repository): a file Dockerfile.N or N.Dockerfile builds
// from its own directory, a file N/Dockerfile from N. When nothing is named
// for N and the tree has exactly one Dockerfile, that one builds it. .sdo/ and
// hidden directories are not searched. The returned context is "." for the
// tree root.
func BuildFor(fsys fs.FS, image string) (contextPath, dockerfile string, err error) {
	name := path.Base(Repository(image))
	var named, all []string
	err = fs.WalkDir(fsys, ".", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		base := path.Base(p)
		if entry.IsDir() {
			if p != "." && strings.HasPrefix(base, ".") {
				return fs.SkipDir
			}
			return nil
		}
		if base != "Dockerfile" && !strings.HasPrefix(base, "Dockerfile.") && !strings.HasSuffix(base, ".Dockerfile") {
			return nil
		}
		all = append(all, p)
		if base == "Dockerfile."+name || base == name+".Dockerfile" || (base == "Dockerfile" && path.Base(path.Dir(p)) == name) {
			named = append(named, p)
		}
		return nil
	})
	if err != nil {
		return "", "", fmt.Errorf("imagesrc: list Dockerfiles for %q: %w", image, err)
	}
	switch {
	case len(named) == 1:
		dockerfile = named[0]
	case len(named) > 1:
		slices.Sort(named)
		return "", "", fmt.Errorf("imagesrc: image %q has several Dockerfiles: %s; name the one to build in .sdo/deployment/plan.yaml", image, strings.Join(named, ", "))
	case len(all) == 1:
		dockerfile = all[0]
	default:
		return "", "", fmt.Errorf("imagesrc: image %q is pushed to the operator's registry but the tree has no Dockerfile for it: add Dockerfile.%s (built from its directory) or %s/Dockerfile, or declare the build in .sdo/deployment/plan.yaml", image, name, name)
	}
	return path.Dir(dockerfile), dockerfile, nil
}
