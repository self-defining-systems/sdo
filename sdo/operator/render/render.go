package render

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ErrTransient marks a renderer failure that says nothing about the source: the
// renderer could not run (for example its isolated child could not be started
// under host resource pressure). Callers retry instead of treating the source
// as invalid.
var ErrTransient = errors.New("render unavailable")

// SecretMode states whether Secret values may leave the renderer.
type SecretMode int

const (
	// SecretsOmit: returned Secret objects carry identity only; inputs that
	// exist solely to feed Secret values are not read. Desired-state reads
	// always use this mode.
	SecretsOmit SecretMode = iota
	// SecretsInclude is the trusted deployer's mode.
	SecretsInclude
)

// Env is everything outside the tree that may influence a render. It must be
// deterministic and recorded: it comes from install configuration, never from
// a live read during render.
type Env struct {
	Namespace   string   // application namespace (default namespace, Helm .Release.Namespace)
	Release     string   // stable release name (Helm .Release.Name)
	// KubeVersion and APIVersions describe the target cluster. Both are part
	// of EnvDigest. Empty/nil means unknown: a renderer that needs them
	// applies its own default (helm: v1.30.0 and the APIs that version
	// serves). Nothing populates them from cluster discovery yet.
	KubeVersion string
	APIVersions []string
	Secrets     SecretMode
}

// Gate says what the deployer waits for after applying a phase.
type Gate int

const (
	// GateReady waits until every workload in the phase is ready or complete.
	GateReady Gate = iota
	// GateNone applies and continues.
	GateNone
)

// Operation is what a deploy does to the application: install it, or upgrade
// what a previous deploy installed.
type Operation string

const (
	OperationInstall Operation = "install"
	OperationUpgrade Operation = "upgrade"
)

// Phase is a named group of objects applied together.
type Phase struct {
	// Name identifies the phase; it is non-empty and unique within a Result.
	Name    string
	Objects []*unstructured.Unstructured // apply order within the phase
	Gate    Gate
	// Ephemeral objects are applied but are not desired state: they are
	// excluded from inventory, drift, onboarding identities and restore.
	Ephemeral bool
	// Runs names the one operation the deployer applies this phase for; empty
	// means every operation. A phase of another operation is still part of the
	// rendered set (reads, policy and drift see its objects) but is not applied.
	Runs Operation
}

// Result is what a renderer returns. Each object belongs to exactly one phase
// by construction.
type Result struct {
	Phases []Phase // apply order
	Inputs []Input // set by Catalog.Render from the Recorder; renderers leave it nil
}

// SinglePhase is the common case: one GateReady phase named "default" over
// objs (no phase when objs is empty).
func SinglePhase(objs ...*unstructured.Unstructured) Result {
	if len(objs) == 0 {
		return Result{}
	}
	return Result{Phases: []Phase{{Name: "default", Objects: objs, Gate: GateReady}}}
}

// Objects returns every object in apply order.
func (r Result) Objects() []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, p := range r.Phases {
		out = append(out, p.Objects...)
	}
	return out
}

// Desired returns the objects outside Ephemeral phases: the desired state
// readers see.
func (r Result) Desired() []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, p := range r.Phases {
		if !p.Ephemeral {
			out = append(out, p.Objects...)
		}
	}
	return out
}

// Renderer is implemented by each tool. It must be a pure function of
// (fsys contents, env, config): no clock, network, cluster, environment
// variables or process execution. Errors name the input path that failed.
type Renderer[C proto.Message] interface {
	Render(ctx context.Context, fsys FS, env Env, config C) (Result, error)
}

// RendererFunc adapts a function to Renderer.
type RendererFunc[C proto.Message] func(context.Context, FS, Env, C) (Result, error)

// Render calls f.
func (f RendererFunc[C]) Render(ctx context.Context, fsys FS, env Env, config C) (Result, error) {
	return f(ctx, fsys, env, config)
}
