package kustomize

import (
	"errors"
	"io/fs"
	"path"
	"slices"

	"gopkg.in/yaml.v3"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// Detect finds the kustomization roots of a tree: directories with a
// kustomization file that no other kustomization includes as a resource, base
// or component, and that are not themselves components (a component only
// patches a kustomization that includes it).
func Detect(fsys render.FS) ([]render.Root, error) {
	files, err := render.TreeFiles(fsys)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, name := range files {
		if isKustomizationFile(path.Base(name)) {
			dirs = append(dirs, path.Dir(name))
		}
	}
	dirs = slices.Compact(dirs)
	included := map[string]bool{}
	for _, dir := range dirs {
		for _, name := range kustomizationFiles {
			data, err := fsys.ReadFile(path.Join(dir, name))
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			var k struct {
				Kind       string   `yaml:"kind"`
				Resources  []string `yaml:"resources"`
				Bases      []string `yaml:"bases"`
				Components []string `yaml:"components"`
			}
			// An unreadable kustomization is a root; rendering it reports why.
			_ = yaml.Unmarshal(data, &k)
			if k.Kind == "Component" {
				included[dir] = true
			}
			for _, ref := range slices.Concat(k.Resources, k.Bases, k.Components) {
				included[path.Clean(path.Join(dir, ref))] = true
			}
		}
	}
	var roots []render.Root
	for _, dir := range dirs {
		if !included[dir] {
			roots = append(roots, render.Root{Dir: dir, Config: &contracts.KustomizeRenderConfig{Path: dir}})
		}
	}
	return roots, nil
}
