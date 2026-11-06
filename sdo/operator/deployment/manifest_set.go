//archcheck:ratchet R5 count=3 until=S3-13b
package deployment

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/imagesrc"
)

// selectManifestPaths resolves the paths named by the accepted deployment
// strategy against files in the candidate checkout. A path may name one
// manifest or a directory deployed recursively. Transitional: the deploy path
// renders through rendered.Desired once the Deployer replaces RawStrategy.
func selectManifestPaths(available, selected []string) ([]string, error) {
	if len(selected) == 0 {
		return nil, errors.New("no deployed manifest paths are configured")
	}
	seen := map[string]bool{}
	for _, selection := range selected {
		clean := path.Clean(selection)
		if selection == "" || clean != selection || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			return nil, fmt.Errorf("invalid manifest path %q", selection)
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
				return nil, fmt.Errorf("manifest %s is selected twice", candidate)
			}
			seen[candidate] = true
		}
		if !matched {
			return nil, fmt.Errorf("path %s has no manifests", selection)
		}
	}
	paths := make([]string, 0, len(seen))
	for candidate := range seen {
		paths = append(paths, candidate)
	}
	sort.Strings(paths)
	return paths, nil
}

func candidateManifestPaths(root string, selected []string) ([]string, error) {
	available := make([]string, 0)
	for _, selection := range selected {
		resolved, err := imagesrc.SafeCheckoutPath(root, selection)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			available = append(available, selection)
			continue
		}
		err = filepath.WalkDir(resolved, func(candidate string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(root, candidate)
			if err != nil {
				return err
			}
			available = append(available, filepath.ToSlash(relative))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return selectManifestPaths(available, selected)
}
