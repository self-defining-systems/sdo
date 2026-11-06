package render

import (
	"context"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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
	KubeVersion string   // empty until slice 8
	APIVersions []string // nil until slice 8
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

// Phase is a contiguous range of Result.Objects applied together.
type Phase struct {
	Name string
	// End is an exclusive index into Result.Objects. Phases partition Objects
	// in order.
	End  int
	Gate Gate
	// Ephemeral objects are applied but are not desired state: they are
	// excluded from inventory, drift, onboarding identities and restore.
	Ephemeral bool
}

// Result is what a renderer returns.
type Result struct {
	Objects []*unstructured.Unstructured // apply order
	Phases  []Phase                      // nil = one GateReady phase over all Objects
	Inputs  []string                     // set by Catalog.Render from the Recorder; renderers leave it nil
}

// EffectivePhases returns Phases, or the single GateReady phase over every
// object when Phases is nil.
func (r Result) EffectivePhases() []Phase {
	if r.Phases != nil {
		return r.Phases
	}
	if len(r.Objects) == 0 {
		return nil
	}
	return []Phase{{Name: "default", End: len(r.Objects), Gate: GateReady}}
}

// Desired returns the objects outside Ephemeral phases: the desired state
// readers see.
func (r Result) Desired() []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	start := 0
	for _, p := range r.EffectivePhases() {
		if !p.Ephemeral {
			out = append(out, r.Objects[start:p.End]...)
		}
		start = p.End
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
