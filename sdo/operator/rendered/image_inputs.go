package rendered

import (
	"errors"
	"fmt"
	"path"
	"strings"
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
