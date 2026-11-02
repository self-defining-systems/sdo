package deployment

import (
	"io/fs"
	"os"
	"path/filepath"
	"sdo.dev/operator/rendered"
)

var ErrManifestSelectionUnavailable = rendered.ErrManifestSelectionUnavailable

func SelectManifestPaths(available, selected []string) ([]string, error) {
	return rendered.SelectManifestPaths(available, selected)
}

func candidateManifestPaths(root string, selected []string) ([]string, error) {
	available := make([]string, 0)
	for _, selection := range selected {
		resolved, err := safeCandidatePath(root, selection)
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
	return SelectManifestPaths(available, selected)
}
