package helm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"

	"google.golang.org/protobuf/types/known/structpb"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm/internal/policy"
)

// Runner executes one encoded render request and returns the encoded
// response: InProcess calls Serve directly; production runs Serve in a child
// process bounded in memory, time and output, so that a chart that exhausts
// memory (a Go fatal) or loops without calling a template function cannot
// crash or hang the caller. A Runner stops when ctx ends (including while it
// waits for capacity to start). It also stops a render that runs longer than
// RenderTimeLimit, counted from when the render starts, and reports that as an
// error wrapping ErrTimeLimit. An error that wraps
// ErrRenderProcess means the render itself broke a process limit or crashed;
// helm reports it as a render-budget violation (a verdict on the chart). Any
// other error is returned as is; a process that could not be started should wrap
// render.ErrTransient so that it is not mistaken for a verdict on the chart.
type Runner func(ctx context.Context, request []byte) ([]byte, error)

// ErrRenderProcess marks a Runner failure caused by the render: the process
// ran out of memory, passed its output cap or crashed.
var ErrRenderProcess = errors.New("helm render process failed")

// ErrTimeLimit marks a Runner failure because the render itself passed
// RenderTimeLimit once it was running: a verdict on the chart.
var ErrTimeLimit = errors.New("helm render passed its time limit")

// InProcess runs the render in the calling goroutine, bounded only by the
// engine's in-template hooks. For tests and tools that render trusted charts.
func InProcess(ctx context.Context, request []byte) ([]byte, error) {
	return serve(ctx, request), nil
}

// Serve is the child side of an isolated Runner: it decodes a request made
// by the helm renderer, renders it and encodes the result or the error.
func Serve(request []byte) []byte { return serve(context.Background(), request) }

func serve(ctx context.Context, payload []byte) []byte {
	var req request
	var resp response
	if err := json.Unmarshal(payload, &req); err != nil {
		resp.Error = encodeError(fmt.Errorf("helm render: decode request: %w", err))
	} else if res, err := compute(ctx, req); err != nil {
		resp.Error = encodeError(err)
	} else if resp.Phases, err = encodePhases(res); err != nil {
		resp.Error = encodeError(err)
	}
	out, err := json.Marshal(resp)
	if err != nil {
		out, _ = json.Marshal(response{Error: &wireError{Message: "helm render: encode response: " + err.Error()}})
	}
	return out
}

// request is everything a render uses, read in the caller through the
// catalog's recording FS.
type request struct {
	Root       string    `json:"root"`
	Chart      []file    `json:"chart"`       // chart files, chart-relative, .helmignore applied
	ValueFiles []file    `json:"value_files"` // in merge order
	Inline     *string   `json:"inline,omitempty"`
	Env        renderEnv `json:"env"`
}

type file struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type renderEnv struct {
	Namespace   string   `json:"namespace"`
	Release     string   `json:"release"` // resolved: Env.Release or the namespace
	KubeVersion string   `json:"kube_version"`
	APIVersions []string `json:"api_versions"` // null: unknown
	OmitSecrets bool     `json:"omit_secrets"`
}

func (e renderEnv) env() render.Env {
	env := render.Env{Namespace: e.Namespace, Release: e.Release, KubeVersion: e.KubeVersion, APIVersions: e.APIVersions, Secrets: render.SecretsInclude}
	if e.OmitSecrets {
		env.Secrets = render.SecretsOmit
	}
	return env
}

func (r request) files() []*loader.BufferedFile {
	out := make([]*loader.BufferedFile, len(r.Chart))
	for i, f := range r.Chart {
		out[i] = &loader.BufferedFile{Name: f.Name, Data: f.Data}
	}
	return out
}

// userValues merges value_files in order, then the inline values (last wins,
// maps merge recursively, null deletes a key).
func (r request) userValues() (map[string]any, error) {
	merged := map[string]any{}
	layer := func(next map[string]any) { merged = chartutil.CoalesceTables(next, merged) }
	for _, f := range r.ValueFiles {
		vals := map[string]any{}
		if err := yaml.Unmarshal(f.Data, &vals); err != nil {
			return nil, fmt.Errorf("helm value file %s: %w", f.Name, err)
		}
		layer(vals)
	}
	if r.Inline != nil {
		inline := &structpb.Struct{}
		if err := inline.UnmarshalJSON([]byte(*r.Inline)); err != nil {
			return nil, fmt.Errorf("helm inline values: %w", err)
		}
		layer(inline.AsMap())
	}
	return merged, nil
}

// gather reads the chart and the value files through fsys, in the order the
// renderer always probed them, so the recorded inputs do not depend on where
// the render executes.
func gather(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.HelmRenderConfig, root string) (request, error) {
	release := env.Release
	if release == "" {
		// No install configuration names a release: the release is the
		// application namespace, the same for deploy and every read.
		release = env.Namespace
	}
	req := request{Root: root, Env: renderEnv{
		Namespace: env.Namespace, Release: release, KubeVersion: env.KubeVersion,
		APIVersions: env.APIVersions, OmitSecrets: env.Secrets == render.SecretsOmit,
	}}
	buffered, err := readChart(ctx, fsys, root)
	if err != nil {
		return request{}, err
	}
	for _, b := range buffered {
		req.Chart = append(req.Chart, file{Name: b.Name, Data: b.Data})
	}
	for _, name := range cfg.GetValueFiles() {
		clean := path.Clean(name)
		if !fs.ValidPath(clean) {
			return request{}, fmt.Errorf("helm value file %q must be a relative path inside the source tree", name)
		}
		data, err := fsys.ReadFile(clean)
		if err != nil {
			return request{}, fmt.Errorf("helm value file %s: %w", clean, err)
		}
		req.ValueFiles = append(req.ValueFiles, file{Name: clean, Data: data})
	}
	if inline := cfg.GetValues(); inline != nil {
		data, err := inline.MarshalJSON()
		if err != nil {
			return request{}, fmt.Errorf("helm inline values: %w", err)
		}
		s := string(data)
		req.Inline = &s
	}
	return req, nil
}

type response struct {
	Phases []phase    `json:"phases,omitempty"`
	Error  *wireError `json:"error,omitempty"`
}

type phase struct {
	Name      string            `json:"name"`
	Gate      render.Gate       `json:"gate"`
	Ephemeral bool              `json:"ephemeral"`
	Runs      render.Operation  `json:"runs,omitempty"`
	Objects   []json.RawMessage `json:"objects"`
}

// wireError carries a render error across the process boundary: its message
// and every typed policy violation in it, so errors.Is and errors.As behave
// as they would in process.
type wireError struct {
	Message    string         `json:"message"`
	Violations []policy.Error `json:"violations,omitempty"`
}

func encodePhases(res render.Result) ([]phase, error) {
	out := make([]phase, len(res.Phases))
	for i, p := range res.Phases {
		out[i] = phase{Name: p.Name, Gate: p.Gate, Ephemeral: p.Ephemeral, Runs: p.Runs, Objects: make([]json.RawMessage, len(p.Objects))}
		for j, obj := range p.Objects {
			data, err := obj.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("helm render: encode %s %s: %w", obj.GetKind(), obj.GetName(), err)
			}
			out[i].Objects[j] = data
		}
	}
	return out, nil
}

func encodeError(err error) *wireError {
	w := &wireError{Message: err.Error()}
	var walk func(error)
	walk = func(e error) {
		if perr, ok := e.(*policy.Error); ok {
			w.Violations = append(w.Violations, *perr)
			return
		}
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range u.Unwrap() {
				walk(inner)
			}
		case interface{ Unwrap() error }:
			if inner := u.Unwrap(); inner != nil {
				walk(inner)
			}
		}
	}
	walk(err)
	return w
}

// remoteError is a decoded wireError.
type remoteError struct {
	message    string
	violations []error
}

func (e *remoteError) Error() string   { return e.message }
func (e *remoteError) Unwrap() []error { return e.violations }

func decodeResponse(root string, payload []byte) (render.Result, error) {
	var resp response
	if err := json.Unmarshal(payload, &resp); err != nil {
		return render.Result{}, fmt.Errorf("helm render of %s: decode response: %w", root, err)
	}
	if w := resp.Error; w != nil {
		if len(w.Violations) == 1 && w.Violations[0].Error() == w.Message {
			v := w.Violations[0]
			return render.Result{}, &v
		}
		out := &remoteError{message: w.Message}
		for i := range w.Violations {
			out.violations = append(out.violations, &w.Violations[i])
		}
		return render.Result{}, out
	}
	var res render.Result
	for _, p := range resp.Phases {
		objects := make([]*unstructured.Unstructured, len(p.Objects))
		for i, raw := range p.Objects {
			obj := &unstructured.Unstructured{}
			if err := obj.UnmarshalJSON(raw); err != nil {
				return render.Result{}, fmt.Errorf("helm render of %s: decode object: %w", root, err)
			}
			objects[i] = obj
		}
		res.Phases = append(res.Phases, render.Phase{Name: p.Name, Objects: objects, Gate: p.Gate, Ephemeral: p.Ephemeral, Runs: p.Runs})
	}
	return res, nil
}
