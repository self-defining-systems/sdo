package helm

import (
	"path"
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// Detect finds the chart roots of a tree: directories holding a Chart.yaml
// with no Chart.yaml above them (vendored subcharts belong to their chart).
// Each renders with the chart's own values.yaml.
func Detect(fsys render.FS) ([]render.Root, error) {
	files, err := render.TreeFiles(fsys)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, name := range files {
		if path.Base(name) == "Chart.yaml" {
			dirs = append(dirs, path.Dir(name))
		}
	}
	var roots []render.Root
	for _, dir := range dirs {
		if !slices.ContainsFunc(dirs, func(other string) bool { return render.Within(dir, other) }) {
			roots = append(roots, render.Root{Dir: dir, Config: &contracts.HelmRenderConfig{ChartPath: dir}})
		}
	}
	return roots, nil
}
