package validate

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// NormalizeCapturedCorpus stores each distinct snapshot of a changed corpus
// case once. An open.json or healthy.json that is a byte copy of the case's
// fixed.json is removed; the corpus loader then references fixed.json. Like
// whitespace, a copy is fixed, never rejected. It returns the removed paths.
func NormalizeCapturedCorpus(root string, paths []string) ([]string, error) {
	var cases []string
	for _, path := range paths {
		if rel, ok := strings.CutPrefix(path, layout.CorpusDir+"/"); ok {
			if id, _, nested := strings.Cut(rel, "/"); nested && !slices.Contains(cases, id) {
				cases = append(cases, id)
			}
		}
	}
	slices.Sort(cases)
	var removed []string
	for _, id := range cases {
		dir := filepath.Join(root, filepath.FromSlash(layout.CorpusDir), id)
		fixed, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, name := range []string{"open.json", "healthy.json"} {
			file := filepath.Join(dir, name)
			info, err := os.Lstat(file)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(data, fixed) {
				continue
			}
			if err := os.Remove(file); err != nil {
				return nil, err
			}
			removed = append(removed, layout.CorpusDir+"/"+id+"/"+name)
		}
	}
	return removed, nil
}
