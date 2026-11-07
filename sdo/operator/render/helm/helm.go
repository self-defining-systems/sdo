// Package helm is the render plugin for Helm charts. It renders a chart the
// way `helm template` does, over the in-memory files read through render.FS:
// no repository, cluster or registry is touched. The files are read in the
// caller; the templates execute through an injected Runner, which production
// wires to a bounded child process (see Runner). Only Helm's pure
// packages (chart loading, values coalescing, the manifest sorter) and a
// trimmed copy of its template engine are linked; pkg/action, pkg/kube and the registry client
// stay out. Like helm install, the renderer first processes the chart's
// dependencies (conditions, tags, import-values), so everything after that
// (the static policy, rendering, hooks) sees only the release set: enabled
// subcharts, vendored, and without crds/. The engine is a trimmed copy of Helm's
// with its function map restricted by the policy (internal/policy.Restrict):
// every function that reads the clock, the cluster, the environment or a random
// source fails the render when executed, however it was reached (tpl, include,
// helpers), while tpl, include and duration run freely over values from Git. NOTES.txt, where charts
// run their configuration guards, executes as in helm install: its output is
// discarded and a template fail in it fails the render (policy.RuleChartGuard).
// Hooks are chosen by their events: pre-install, post-install, pre-upgrade and
// post-upgrade become phases that carry their operation (render.Phase.Runs), so
// the deployer applies an install's or an upgrade's; hooks of other events
// (delete, rollback, test) are left out.
package helm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/ignore"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
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
Charts must be self-contained: enabled subcharts are vendored in charts/ (disabled ones, by condition or tag, are ignored), and crds/ directories of enabled charts are rejected. tpl, include and the pure template functions work; the render fails when a template executes lookup, env, a clock or date function, or a random or key-generating function (for example a random suffix in a test hook), so such parts must be disabled by values. NOTES.txt is executed and its output discarded: a fail in it (the chart's configuration guard) fails the render with the chart's message, so fix the values it names. Hooks with a pre-install, post-install, pre-upgrade or post-upgrade event become phases (an install applies the install ones, an upgrade of a running deployment the upgrade ones); hooks of other events (delete, rollback, test) are not part of a deploy.`

// Register adds the helm renderer to c under ID. Every render reads the chart
// through the catalog's FS in the calling process and executes it through run,
// which production wires to a memory- and time-bounded child process (see
// Runner). Tests that need no isolation pass InProcess.
func Register(c *render.Catalog, run Runner) error {
	if run == nil {
		return errors.New("helm: Register needs a Runner (InProcess, or an isolated one)")
	}
	renderer := func(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.HelmRenderConfig) (render.Result, error) {
		return renderChart(ctx, fsys, env, cfg, run)
	}
	return render.Register[*contracts.HelmRenderConfig](c, ID, render.RendererFunc[*contracts.HelmRenderConfig](renderer), render.WithDoc(Doc))
}

// RenderTimeLimit bounds one chart render's wall-clock time, counted from when
// its execution starts: a Runner that queues work (production bounds the number
// of concurrent children) starts this clock only once the render runs, so
// waiting for a slot is never a verdict on the chart. The largest real charts
// render in well under a second.
const RenderTimeLimit = 30 * time.Second

// renderChart reads everything the render may use through fsys (so the
// catalog records the inputs here, in the caller), then executes the chart
// through run. A chart is Git content an agent can edit: what its templates do
// is bounded by the engine's hooks first, and by run's process limits for what
// no hook sees (memory held in variables, loops that call no function).
func renderChart(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.HelmRenderConfig, run Runner) (render.Result, error) {
	root := path.Clean(cfg.GetChartPath())
	if cfg.GetChartPath() == "" || !fs.ValidPath(root) {
		return render.Result{}, fmt.Errorf("helm chart_path %q must be a relative path inside the source tree", cfg.GetChartPath())
	}
	req, err := gather(ctx, fsys, env, cfg, root)
	if err != nil {
		return render.Result{}, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm render of %s: encode request: %w", root, err)
	}
	if err := ctx.Err(); err != nil {
		return render.Result{}, err
	}
	out, err := run(ctx, payload)
	if ctx.Err() != nil {
		return render.Result{}, fmt.Errorf("render stopped: %w", context.Cause(ctx))
	}
	if err != nil {
		if errors.Is(err, ErrTimeLimit) {
			return render.Result{}, timeLimitError(root)
		}
		if errors.Is(err, ErrRenderProcess) {
			return render.Result{}, &policy.Error{Rule: policy.RuleRenderBudget, Chart: path.Base(root), Detail: err.Error()}
		}
		return render.Result{}, fmt.Errorf("helm render of %s: %w", root, err)
	}
	return decodeResponse(root, out)
}

func timeLimitError(root string) error {
	return &policy.Error{Rule: policy.RuleRenderBudget, Chart: path.Base(root), Detail: fmt.Sprintf("rendering took longer than %s", RenderTimeLimit)}
}

// compute renders a gathered request. It touches no FS: it is what run
// executes, in a child process in production.
func compute(ctx context.Context, req request) (render.Result, error) {
	root := req.Root
	chrt, err := loader.LoadFiles(req.files())
	if err != nil {
		return render.Result{}, fmt.Errorf("helm chart %s: %w", root, err)
	}
	overrides, err := req.userValues()
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
	env := req.Env.env()
	caps, err := capabilities(env)
	if err != nil {
		return render.Result{}, err
	}
	values, err := chartutil.ToRenderValues(chrt, overrides, chartutil.ReleaseOptions{
		Name: req.Env.Release, Namespace: env.Namespace, Revision: 1, IsInstall: true,
	}, caps)
	if err != nil {
		return render.Result{}, fmt.Errorf("helm values for %s: %w", root, err)
	}
	// The engine checks this deadline on every template function call, so a
	// template that expands through functions fails with a typed budget error
	// even in process; the Runner bounds what calls no function.
	renderCtx, stop := context.WithTimeoutCause(ctx, RenderTimeLimit, timeLimitError(root))
	defer stop()
	files, err := engine.Engine{Funcs: policy.Restrict, Context: renderCtx}.Render(chrt, values)
	var notes *engine.NotesError
	if errors.As(err, &notes) {
		// Helm executes NOTES.txt on every install, and charts run their
		// configuration guards there. Its output is documentation and is
		// discarded; a chart's own fail is the chart refusing this
		// configuration. A function the policy forbids is not: the output
		// would be discarded, so the guard cannot be evaluated
		// deterministically and the chart's other guards still stand.
		if !errors.As(notes.Err, new(*policy.Error)) {
			return render.Result{}, &policy.Error{Rule: policy.RuleChartGuard, Chart: path.Base(root), File: "NOTES.txt", Detail: notes.Err.Error()}
		}
		err = nil
	}
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
	return phases(env, hooks, manifests)
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

// readChart reads every file under root through fsys (honoring .helmignore).
// The schema probe records an absent values.schema.json as an input. Parsing
// the files is left to compute.
func readChart(ctx context.Context, fsys render.FS, root string) ([]*loader.BufferedFile, error) {
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
	return buffered, nil
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
		caps.APIVersions = servedAPIVersions(withKinds(caps.APIVersions), kv.Major, minor)
	}
	return caps, nil
}

// withKinds adds the kind-qualified entries ("policy/v1/PodDisruptionBudget")
// a real cluster's discovery reports and charts probe with
// .Capabilities.APIVersions.Has. Helm's default set holds group versions only.
// Kinds come from the client-go scheme; a group version the scheme no longer
// knows (removed upstream, still served by an older cluster) is given the kinds
// that the other versions of its group have.
func withKinds(known chartutil.VersionSet) chartutil.VersionSet {
	kindsOf := map[string][]string{}
	groupKinds := map[string]map[string]bool{}
	for gvk := range kubescheme.Scheme.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal || strings.HasSuffix(gvk.Kind, "List") || strings.HasSuffix(gvk.Kind, "Options") {
			continue
		}
		gv := gvk.GroupVersion().String()
		kindsOf[gv] = append(kindsOf[gv], gvk.Kind)
		if groupKinds[gvk.Group] == nil {
			groupKinds[gvk.Group] = map[string]bool{}
		}
		groupKinds[gvk.Group][gvk.Kind] = true
	}
	out := slices.Clone(known)
	for _, gv := range known {
		if strings.Count(gv, "/") > 1 {
			continue
		}
		kinds := kindsOf[gv]
		if len(kinds) == 0 {
			group := ""
			if before, _, ok := strings.Cut(gv, "/"); ok {
				group = before
			}
			for kind := range groupKinds[group] {
				kinds = append(kinds, kind)
			}
		}
		for _, kind := range kinds {
			out = append(out, gv+"/"+kind)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
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

// phases builds the hook phases of every deploy operation and the main phase.
// A hook joins the phase of each deploy event it names (pre-install,
// pre-upgrade, post-install, post-upgrade), ordered by weight then name (the
// sorter's order), so a hook annotated for both operations is applied by an
// install and by an upgrade; hooks without a deploy event (delete, rollback,
// test) are left out. Main objects follow Helm's install order. Which phases a
// deploy applies is the deployer's decision, from the operation it performs.
func phases(env render.Env, hooks []*release.Hook, manifests []releaseutil.Manifest) (render.Result, error) {
	byEvent := map[string][]*unstructured.Unstructured{}
	for _, h := range hooks {
		var joined []string
		for _, op := range []policy.Operation{policy.OperationInstall, policy.OperationUpgrade} {
			events := make([]string, len(h.Events))
			for i, e := range h.Events {
				events[i] = string(e)
			}
			joined = append(joined, policy.HookPhases(op, events)...)
		}
		if len(joined) == 0 {
			continue
		}
		obj, err := decode(h.Path, h.Manifest, env)
		if err != nil {
			return render.Result{}, err
		}
		for _, phase := range joined {
			byEvent[phase] = append(byEvent[phase], obj.DeepCopy())
		}
	}
	var main []*unstructured.Unstructured
	for _, m := range manifests {
		obj, err := decode(m.Name, m.Content, env)
		if err != nil {
			return render.Result{}, err
		}
		main = append(main, obj)
	}
	var res render.Result
	hookPhase := func(event string, op render.Operation) {
		if objects := byEvent[event]; len(objects) > 0 {
			res.Phases = append(res.Phases, render.Phase{Name: event, Objects: objects, Gate: render.GateReady, Ephemeral: true, Runs: op})
		}
	}
	hookPhase("pre-install", render.OperationInstall)
	hookPhase("pre-upgrade", render.OperationUpgrade)
	if len(main) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "main", Objects: main, Gate: render.GateReady})
	}
	hookPhase("post-install", render.OperationInstall)
	hookPhase("post-upgrade", render.OperationUpgrade)
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
