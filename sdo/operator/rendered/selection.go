//archcheck:ratchet R5 count=1 until=S3-10d
package rendered

import (
	"errors"
	"path"
	"sort"
	"strings"
)

// SelectManifestPaths resolves the paths named by the accepted deployment
// strategy against files in the accepted source tree. A path may name one
// manifest or a directory deployed recursively.
func SelectManifestPaths(available, selected []string) ([]string, error) {
	if len(selected) == 0 {
		return nil, &SourceError{Kind: SourceUnconfigured, Err: errors.New("no deployed manifest paths are configured")}
	}
	seen := map[string]bool{}
	for _, selection := range selected {
		clean := path.Clean(selection)
		if selection == "" || clean != selection || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			return nil, sourceErrorf(SourceInvalid, "invalid manifest path %q", selection)
		}
		matched := false
		for _, candidate := range available {
			if candidate != selection && !strings.HasPrefix(candidate, selection+"/") {
				continue
			}
			if path.Base(candidate) != "Kustomization" && !strings.HasSuffix(candidate, ".yaml") && !strings.HasSuffix(candidate, ".yml") && !strings.HasSuffix(candidate, ".json") {
				continue
			}
			matched = true
			if seen[candidate] {
				return nil, sourceErrorf(SourceInvalid, "manifest %s is selected twice", candidate)
			}
			seen[candidate] = true
		}
		if !matched {
			return nil, sourceErrorf(SourceInvalid, "path %s has no manifests", selection)
		}
	}
	paths := make([]string, 0, len(seen))
	for candidate := range seen {
		paths = append(paths, candidate)
	}
	sort.Strings(paths)
	return paths, nil
}
