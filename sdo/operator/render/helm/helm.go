// Package helm is the render plugin for Helm charts. It renders a chart the
// way `helm template` does, over the in-memory files read through render.FS:
// no repository, cluster, registry or process is touched. Only Helm's pure
// packages (chart loading, values coalescing, the manifest sorter) and a
// trimmed copy of its template engine are linked; pkg/action, pkg/kube and the registry client
// stay out. Like helm install, the renderer first processes the chart's
// dependencies (conditions, tags, import-values), so everything after that
// (the static policy, rendering, hooks) sees only the release set: enabled
// subcharts, vendored, and without crds/. The engine is a trimmed copy of Helm's
// with its function map restricted by the policy (internal/policy.Restrict):
// every function that reads the clock, the cluster, the environment or a random
// source fails the render when executed, however it was reached (tpl, include,
// helpers), while tpl, include and duration run freely over values from Git. Hooks are chosen by the events of the
// rendered operation (install): pre-install and post-install become phases,
// hooks of other operations (upgrade, delete, rollback, test) are left out.
package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/ignore"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm/internal/engine"
	"sdo.dev/operator/render/helm/internal/policy"
)

// ID is the renderer ID of this plugin.
const ID = "helm"

// DefaultKubeVersion is the .Capabilities.KubeVersion used when Env carries
// none: the Kubernetes line SDO links. An Env without APIVersions gets the
// APIs that version still serves.
const DefaultKubeVersion = "v1.30.0"

// Doc tells an agent what the helm renderer does and when to choose it.
const Doc = `Renders a Helm chart from the source tree the way helm template does, with no cluster or registry access. Choose it when the application ships a chart (a directory with Chart.yaml) in the tree.
Config: chart_path, the tree-relative chart directory; value_files, tree-relative values files merged in order after the chart's values.yaml; values, an inline object merged last (like --set-json), for settings such as image tags or replica counts.
Charts must be self-contained: enabled subcharts are vendored in charts/ (disabled ones, by condition or tag, are ignored), and crds/ directories of enabled charts are rejected. tpl, include and the pure template functions work; the render fails when a template executes lookup, env, a clock or date function, or a random or key-generating function (for example a random suffix in a test hook), so such parts must be disabled by values. Hooks with a pre-install or post-install event become phases; hooks of other events (upgrade, delete, rollback, test) are not part of a deploy.`

// Register adds the helm renderer to c under ID.
func Register(c *render.Catalog) error {
	return render.Register[*contracts.HelmRenderConfig](c, ID, render.RendererFunc[*contracts.HelmRenderConfig](renderChart), render.WithDoc(Doc))
}

// renderTimeLimit bounds one chart render's wall-clock time. The largest real
// charts render in well under a second.
const renderTimeLimit = 30 * time.Second

func renderChart(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.HelmRenderConfig) (render.Result, error) {
	root := path.Clean(cfg.GetChartPath())
	if cfg.GetChartPath() == "" || !fs.ValidPath(root) {
		return render.Result{}, fmt.Errorf("helm chart_path %q must be a relative path inside the source tree", cfg.GetChartPath())
	}
	releaseName := env.Release
	if releaseName == "" {
		// No install configuration names a release: the release is the
		// application namespace, the same for deploy and every read.
		releaseName = env.Namespace
	}
	chrt, err := loadChart(ctx, fsys, root)
	if err != nil {
		return render.Result{}, err
	}
	overrides, err := userValues(fsys, cfg)
	if err != nil {
		return render.Result{}, err
	}
	// As helm install does, before anything else: disabled subcharts leave the
	// release set, import-values merge into the parent.
	if err := chartutil.ProcessDependenciesWithMerge(chrt, overrides); err != nil {
		return render.Result{}, fmt.Errorf("helm dependencies of %s: %w", root, err)
	}
	if err := policy.Check(chrt); err != nil {
		return render.Result{}, err
	}
	caps, err := capabilities(env)
	if err != nil {
		return render.Result{}, err
	}
	values, err := chartutil.ToRenderValues(chrt, overrides, chartutil.ReleaseOptions{
		Name: releaseName, Namespace: env.Namespace, Revision: 1, IsInstall: true,
	}, caps)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm values for %s: %w", root, err)
	}
	if err := ctx.Err(); err != nil {
		return render.Result{}, err
	}
	// The engine checks this deadline on every template function call; a
	// template that expands without end fails with a typed budget error instead
	// of holding the controller.
	renderCtx, stop := context.WithTimeoutCause(ctx, renderTimeLimit,
		&policy.Error{Rule: policy.RuleRenderBudget, Chart: path.Base(root), Detail: fmt.Sprintf("rendering took longer than %s", renderTimeLimit)})
	defer stop()
	files, err := engine.Engine{Funcs: policy.Restrict, Context: renderCtx}.Render(chrt, values)
	if err != nil {
		return render.Result{}, renderError(root, err)
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
	return phases(policy.OperationInstall, env, hooks, manifests)
}

// renderError names a policy violation raised by a stubbed function with the
// template that called it and the output file whose render reached it (the
// same file unless the call sits in a helper or a tpl string); any other
// render failure is wrapped as is.
func renderError(root string, err error) error {
	var perr *policy.Error
	if !errors.As(err, &perr) {
		return fmt.Errorf("helm render of %s: %w", root, err)
	}
	var outer, inner string
	for e := err; e != nil; e = errors.Unwrap(e) {
		if ee, ok := e.(template.ExecError); ok {
			if outer == "" {
				outer = ee.Name
			}
			inner = ee.Name
		}
	}
	out := *perr
	out.Chart = path.Base(root)
	out.File = inner
	if outer != "" && outer != inner {
		out.Detail += fmt.Sprintf(" (reached from %s)", outer)
	}
	return &out
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
	} else {
		// Unknown: Helm's default set lists every API its client library has
		// ever known, including ones the cluster no longer serves.
		minor, _ := strconv.Atoi(strings.TrimRight(kv.Minor, "+"))
		caps.APIVersions = servedAPIVersions(caps.APIVersions, kv.Major, minor)
	}
	return caps, nil
}

// removedAPIs lists, by the Kubernetes minor version (1.x) that stopped
// serving them, the group versions removed from the API (Kubernetes
// deprecation guide). Only whole group versions are listed.
var removedAPIs = []struct {
	minor         int
	groupVersions []string
}{
	{22, []string{"extensions/v1beta1", "networking.k8s.io/v1beta1", "admissionregistration.k8s.io/v1beta1", "apiextensions.k8s.io/v1beta1",
		"rbac.authorization.k8s.io/v1beta1", "rbac.authorization.k8s.io/v1alpha1", "scheduling.k8s.io/v1beta1", "scheduling.k8s.io/v1alpha1",
		"storage.k8s.io/v1beta1", "storage.k8s.io/v1alpha1", "certificates.k8s.io/v1beta1", "coordination.k8s.io/v1beta1",
		"authentication.k8s.io/v1beta1", "authorization.k8s.io/v1beta1"}},
	{25, []string{"batch/v1beta1", "discovery.k8s.io/v1beta1", "events.k8s.io/v1beta1", "autoscaling/v2beta1", "policy/v1beta1", "node.k8s.io/v1beta1", "node.k8s.io/v1alpha1"}},
	{26, []string{"autoscaling/v2beta2", "flowcontrol.apiserver.k8s.io/v1beta1"}},
	{29, []string{"flowcontrol.apiserver.k8s.io/v1beta2"}},
	{32, []string{"flowcontrol.apiserver.k8s.io/v1beta3"}},
}

// servedAPIVersions drops from known the group versions (and their kinds)
// that a Kubernetes major.minor no longer serves.
func servedAPIVersions(known chartutil.VersionSet, major string, minor int) chartutil.VersionSet {
	removed := map[string]bool{}
	if major == "1" {
		for _, r := range removedAPIs {
			if minor >= r.minor {
				for _, gv := range r.groupVersions {
					removed[gv] = true
				}
			}
		}
	}
	var out chartutil.VersionSet
	for _, v := range known {
		gv := v
		if parts := strings.Split(v, "/"); len(parts) == 3 {
			gv = parts[0] + "/" + parts[1]
		}
		if !removed[gv] {
			out = append(out, v)
		}
	}
	return out
}

// phases builds the pre, main and post phases of operation op. Hooks join the
// phases their events name for op and are ordered by weight then name (the
// sorter's order); hooks without such an event are left out. Main objects
// follow Helm's install order.
func phases(op policy.Operation, env render.Env, hooks []*release.Hook, manifests []releaseutil.Manifest) (render.Result, error) {
	var pre, post, main []*unstructured.Unstructured
	for _, h := range hooks {
		events := make([]string, len(h.Events))
		for i, e := range h.Events {
			events[i] = string(e)
		}
		joined := policy.HookPhases(op, events)
		if len(joined) == 0 {
			continue
		}
		obj, err := decode(h.Path, h.Manifest, env)
		if err != nil {
			return render.Result{}, err
		}
		for _, phase := range joined {
			if phase == "pre-"+string(op) {
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
		res.Phases = append(res.Phases, render.Phase{Name: "pre-" + string(op), Objects: pre, Gate: render.GateReady, Ephemeral: true})
	}
	if len(main) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "main", Objects: main, Gate: render.GateReady})
	}
	if len(post) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "post-" + string(op), Objects: post, Gate: render.GateReady, Ephemeral: true})
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
		obj.Object = render.SecretIdentity(obj.Object)
	}
	return obj, nil
}
