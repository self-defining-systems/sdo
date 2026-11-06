package imagesrc

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Repository strips a digest and a tag from an image reference (both, for
// name:tag@digest). It is the one
// parser for "the repository an image reference names"; a registry port
// (host:5000/app) is not mistaken for a tag.
func Repository(image string) string {
	if at := strings.IndexByte(image, '@'); at >= 0 {
		image = image[:at]
	}
	lastSlash := strings.LastIndexByte(image, '/')
	if colon := strings.LastIndexByte(image, ':'); colon > lastSlash {
		return image[:colon]
	}
	return image
}

// SafeCheckoutPath resolves raw, a path a proposal names, to a real path
// inside root. Absolute paths, paths that climb out of root and symlinks that
// resolve outside it are rejected; a missing path wraps fs.ErrNotExist.
func SafeCheckoutPath(root, raw string) (string, error) {
	if raw == "" || filepath.IsAbs(raw) || raw == ".." || strings.HasPrefix(filepath.Clean(raw), ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("candidate path escapes proposal: %q", raw)
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(base, raw))
	if err != nil {
		return "", err
	}
	if rel, err := filepath.Rel(base, path); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("candidate path escapes proposal: %q", raw)
	}
	return path, nil
}
