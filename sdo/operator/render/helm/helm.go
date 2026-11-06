// Package helm is the render plugin for Helm charts. It renders a chart the
// way `helm template` does, over the in-memory files read through render.FS:
// no repository, cluster, registry or process is touched. Only Helm's pure
// packages (chart loading, values coalescing, the template engine and the
// manifest sorter) are linked; pkg/action, pkg/kube and the registry client
// stay out. Charts are checked by the static policy first (no lookup, no
// clock or random functions, vendored subcharts only, no crds/, only
// pre-install and post-install hooks).
package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	"helm.sh/helm/v3/pkg/ignore"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm/internal/policy"
)

// ID is the renderer ID of this plugin.
const ID = "helm"

// DefaultKubeVersion is the .Capabilities.KubeVersion used when Env carries
// none: the Kubernetes line SDO links.
const DefaultKubeVersion = "v1.30.0"

// Doc tells an agent what the helm renderer does and when to choose it.
const Doc = `Renders a Helm chart from the source tree the way helm template does, with no cluster or registry access. Choose it when the application ships a chart (a directory with Chart.yaml) in the tree.
Config: chart_path, the tree-relative chart directory; value_files, tree-relative values files merged in order after the chart's values.yaml; values, an inline object merged last (like --set-json), for settings such as image tags or replica counts.
Charts must be self-contained: subcharts are vendored in charts/, and lookup, clock and random template functions, crds/ directories and hooks other than pre-install and post-install are rejected.`

// Register adds the helm renderer to c under ID.
func Register(c *render.Catalog) error {
	return render.Register[*contracts.HelmRenderConfig](c, ID, render.RendererFunc[*contracts.HelmRenderConfig](renderChart), render.WithDoc(Doc))
}

func renderChart(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.HelmRenderConfig) (render.Result, error) {
	root := path.Clean(cfg.GetChartPath())
	if cfg.GetChartPath() == "" || !fs.ValidPath(root) {
		return render.Result{}, fmt.Errorf("helm chart_path %q must be a relative path inside the source tree", cfg.GetChartPath())
	}
	if env.Release == "" {
		return render.Result{}, errors.New("helm needs Env.Release (the release name)")
	}
	chrt, err := loadChart(ctx, fsys, root)
	if err != nil {
		return render.Result{}, err
	}
	if err := policy.Check(chrt); err != nil {
		return render.Result{}, err
	}
	overrides, err := userValues(fsys, cfg)
	if err != nil {
		return render.Result{}, err
	}
	caps, err := capabilities(env)
	if err != nil {
		return render.Result{}, err
	}
	values, err := chartutil.ToRenderValues(chrt, overrides, chartutil.ReleaseOptions{
		Name: env.Release, Namespace: env.Namespace, Revision: 1, IsInstall: true,
	}, caps)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm values for %s: %w", root, err)
	}
	if err := ctx.Err(); err != nil {
		return render.Result{}, err
	}
	files, err := engine.Render(chrt, values)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm render of %s: %w", root, err)
	}
	for name := range files {
		// NOTES.txt is user documentation, not a manifest (as in helm install).
		if strings.HasSuffix(name, "NOTES.txt") {
			delete(files, name)
		}
	}
	hooks, manifests, err := releaseutil.SortManifests(files, caps.APIVersions, releaseutil.InstallOrder)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm render of %s: %w", root, err)
	}
	return phases(chrt.Name(), env, hooks, manifests)
}

// loadChart reads every file under root through fsys (honoring .helmignore)
// and loads the chart from memory. The schema probe records an absent
// values.schema.json as an input.
func loadChart(ctx context.Context, fsys render.FS, root string) (*chart.Chart, error) {
	info, err := fsys.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("helm chart directory %s: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("helm chart_path %s is a file; name the directory holding Chart.yaml", root)
	}
	if _, err := fsys.Stat(path.Join(root, "Chart.yaml")); err != nil {
		return nil, fmt.Errorf("helm chart %s: %w", root, err)
	}
	rules := ignore.Empty()
	if data, err := fsys.ReadFile(path.Join(root, ".helmignore")); err == nil {
		if rules, err = ignore.Parse(strings.NewReader(string(data))); err != nil {
			return nil, fmt.Errorf("helm chart %s: .helmignore: %w", root, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	var buffered []*loader.BufferedFile
	var walk func(dir string) error
	walk = func(dir string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := fsys.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			full := path.Join(dir, e.Name())
			rel := strings.TrimPrefix(full, root+"/")
			fi, err := e.Info()
			if err != nil {
				return err
			}
			if rules.Ignore(rel, fi) {
				continue
			}
			switch {
			case e.IsDir():
				if err := walk(full); err != nil {
					return err
				}
			case e.Type().IsRegular():
				data, err := fsys.ReadFile(full)
				if err != nil {
					return err
				}
				buffered = append(buffered, &loader.BufferedFile{Name: rel, Data: data})
			default:
				return fmt.Errorf("helm chart %s: %s is not a regular file", root, full)
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	// A chart without a values schema is valid; the probe makes its absence an
	// input so adding one invalidates the render.
	if _, err := fsys.Stat(path.Join(root, "values.schema.json")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	chrt, err := loader.LoadFiles(buffered)
	if err != nil {
		return nil, fmt.Errorf("helm chart %s: %w", root, err)
	}
	return chrt, nil
}

// userValues merges value_files in order, then the inline values (last wins,
// maps merge recursively, null deletes a key).
func userValues(fsys render.FS, cfg *contracts.HelmRenderConfig) (map[string]any, error) {
	merged := map[string]any{}
	layer := func(next map[string]any) { merged = chartutil.CoalesceTables(next, merged) }
	for _, name := range cfg.GetValueFiles() {
		clean := path.Clean(name)
		if !fs.ValidPath(clean) {
			return nil, fmt.Errorf("helm value file %q must be a relative path inside the source tree", name)
		}
		data, err := fsys.ReadFile(clean)
		if err != nil {
			return nil, fmt.Errorf("helm value file %s: %w", clean, err)
		}
		vals := map[string]any{}
		if err := yaml.Unmarshal(data, &vals); err != nil {
			return nil, fmt.Errorf("helm value file %s: %w", clean, err)
		}
		layer(vals)
	}
	if inline := cfg.GetValues(); inline != nil {
		layer(structpb.NewStructValue(inline).GetStructValue().AsMap())
	}
	return merged, nil
}

func capabilities(env render.Env) (*chartutil.Capabilities, error) {
	caps := chartutil.DefaultCapabilities.Copy()
	version := env.KubeVersion
	if version == "" {
		version = DefaultKubeVersion
	}
	kv, err := chartutil.ParseKubeVersion(version)
	if err != nil {
		return nil, fmt.Errorf("helm kube version %q: %w", version, err)
	}
	caps.KubeVersion = *kv
	if env.APIVersions != nil {
		caps.APIVersions = chartutil.VersionSet(append([]string(nil), env.APIVersions...))
	}
	return caps, nil
}

// phases builds pre-install, main and post-install phases. Hooks are ordered
// by weight then name (the sorter's order); main objects follow Helm's install
// order.
func phases(chartName string, env render.Env, hooks []*release.Hook, manifests []releaseutil.Manifest) (render.Result, error) {
	var pre, post, main []*unstructured.Unstructured
	for _, h := range hooks {
		events := make([]string, len(h.Events))
		for i, e := range h.Events {
			events[i] = string(e)
		}
		joined, err := policy.HookPhases(chartName, h.Path, events)
		if err != nil {
			return render.Result{}, err
		}
		obj, err := decode(h.Path, h.Manifest, env)
		if err != nil {
			return render.Result{}, err
		}
		for _, phase := range joined {
			if phase == "pre-install" {
				pre = append(pre, obj.DeepCopy())
			} else {
				post = append(post, obj.DeepCopy())
			}
		}
	}
	for _, m := range manifests {
		obj, err := decode(m.Name, m.Content, env)
		if err != nil {
			return render.Result{}, err
		}
		main = append(main, obj)
	}
	var res render.Result
	if len(pre) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "pre-install", Objects: pre, Gate: render.GateReady, Ephemeral: true})
	}
	if len(main) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "main", Objects: main, Gate: render.GateReady})
	}
	if len(post) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "post-install", Objects: post, Gate: render.GateReady, Ephemeral: true})
	}
	return res, nil
}

func decode(file, content string, env render.Env) (*unstructured.Unstructured, error) {
	data, err := yaml.YAMLToJSON([]byte(content))
	if err != nil {
		return nil, fmt.Errorf("helm %s: %w", file, err)
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(data); err != nil {
		return nil, fmt.Errorf("helm %s: %w", file, err)
	}
	if env.Secrets == render.SecretsOmit && obj.GetKind() == "Secret" && obj.GetAPIVersion() == "v1" {
		delete(obj.Object, "data")
		delete(obj.Object, "stringData")
	}
	return obj, nil
}
