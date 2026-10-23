package rendered

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

var ErrManifestSelectionUnavailable = errors.New("deployed manifest paths are unavailable")

// SelectManifestPaths resolves the paths named by the accepted deployment
// strategy against files in the accepted source tree. A path may name one
// manifest or a directory deployed recursively.
func SelectManifestPaths(available, selected []string) ([]string, error) {
	if len(selected) == 0 {
		return nil, ErrManifestSelectionUnavailable
	}
	seen := map[string]bool{}
	for _, selection := range selected {
		clean := path.Clean(selection)
		if selection == "" || clean != selection || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			return nil, fmt.Errorf("%w: invalid path %q", ErrManifestSelectionUnavailable, selection)
		}
		matched := false
		for _, candidate := range available {
			if candidate != selection && !strings.HasPrefix(candidate, selection+"/") {
				continue
			}
			if !strings.HasSuffix(candidate, ".yaml") && !strings.HasSuffix(candidate, ".yml") && !strings.HasSuffix(candidate, ".json") {
				continue
			}
			matched = true
			if seen[candidate] {
				return nil, fmt.Errorf("%w: manifest %s is selected twice", ErrManifestSelectionUnavailable, candidate)
			}
			seen[candidate] = true
		}
		if !matched {
			return nil, fmt.Errorf("%w: path %s has no manifests", ErrManifestSelectionUnavailable, selection)
		}
	}
	paths := make([]string, 0, len(seen))
	for candidate := range seen {
		paths = append(paths, candidate)
	}
	sort.Strings(paths)
	return paths, nil
}
