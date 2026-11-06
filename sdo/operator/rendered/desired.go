package rendered

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/sdk"
)

// DesiredObject is one normalized object of a desired set.
type DesiredObject struct {
	Ref sdk.ObjectRef
	// Object is normalized; Secret data is present only under SecretsInclude.
	Object   *unstructured.Unstructured
	Workload bool
	// Foreign marks a namespaced object outside env.Namespace. Deploy applies
	// it; unqualified reads project it out (a view, not a second computation).
	Foreign bool
}

// DesiredPhase is a renderer phase with normalized objects.
type DesiredPhase struct {
	Name      string
	Gate      render.Gate
	Ephemeral bool
	Objects   []DesiredObject
}

// DesiredSet is the one desired-state computation deploy and reads share.
type DesiredSet struct {
	Phases []DesiredPhase
	Inputs []render.Input
	// RenderDigest is sha256 over the canonical JSON of the non-ephemeral
	// objects sorted by ref, with each Secret reduced to its identity (the
	// SecretsOmit form), so SecretsOmit and SecretsInclude renders of one
	// source agree.
	RenderDigest string
	// ShapeDigest is sha256 over the desired topology only (see shapeDigest
	// for exactly what is and is not in it). Health refresh keys on it.
	ShapeDigest string
	// EnvDigest is sha256 over the canonical Env with its SecretMode cleared.
	EnvDigest string
}

// Objects returns the non-ephemeral objects in apply order.
func (d DesiredSet) Objects() []DesiredObject {
	var out []DesiredObject
	for _, phase := range d.Phases {
		if !phase.Ephemeral {
			out = append(out, phase.Objects...)
		}
	}
	return out
}

// Desired renders src from fsys through cat, normalizes every object to
// env.Namespace, rejects any duplicate ref (all kinds, every phase), marks
// foreign-namespace objects and computes the digests. Every error is a
// *SourceError: a nil src is SourceUnconfigured; renderer, config, limit and
// duplicate failures are SourceInvalid unless the cause is unavailable Git
// I/O or a context error, which stay SourceUnavailable.
func Desired(ctx context.Context, fsys render.FS, env render.Env, cat *render.Catalog, src *contracts.DeploymentSource) (DesiredSet, error) {
	if src == nil {
		return DesiredSet{}, &SourceError{Kind: SourceUnconfigured, Err: errors.New("no deployment source is configured")}
	}
	if cat == nil {
		return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: fmt.Errorf("deployment source uses renderer %q but no renderer catalog is wired", src.GetRendererId())}
	}
	if env.Namespace == "" {
		return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: errors.New("desired state needs the application namespace")}
	}
	result, err := cat.Render(ctx, fsys, env, src.GetRendererId(), src.GetConfig())
	if err != nil {
		return DesiredSet{}, desiredError(err)
	}
	set := DesiredSet{Inputs: result.Inputs}
	seen := map[sdk.ObjectRef]bool{}
	for _, phase := range result.Phases {
		desired := DesiredPhase{Name: phase.Name, Gate: phase.Gate, Ephemeral: phase.Ephemeral}
		for _, object := range phase.Objects {
			document := object.DeepCopy().Object
			ref, workload, err := Normalize(document, env.Namespace)
			if err != nil {
				return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: fmt.Errorf("render accepted manifest %s/%s: %w", object.GetKind(), object.GetName(), err)}
			}
			if seen[ref] {
				return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: fmt.Errorf("duplicate accepted manifest for %s/%s", ref.Kind, ref.Name)}
			}
			seen[ref] = true
			desired.Objects = append(desired.Objects, DesiredObject{
				Ref: ref, Object: &unstructured.Unstructured{Object: document}, Workload: workload,
				Foreign: ref.Namespace != "" && ref.Namespace != env.Namespace,
			})
		}
		set.Phases = append(set.Phases, desired)
	}
	if set.RenderDigest, err = renderDigest(set.Objects()); err != nil {
		return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: err}
	}
	if set.ShapeDigest, err = shapeDigest(set.Objects()); err != nil {
		return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: err}
	}
	if set.EnvDigest, err = envDigest(env); err != nil {
		return DesiredSet{}, &SourceError{Kind: SourceInvalid, Err: err}
	}
	return set, nil
}

// desiredError keeps an unavailable cause transient (a renderer reading a
// GitTreeFS that fails mid-read) and makes everything else invalid.
func desiredError(err error) error {
	var existing *SourceError
	if errors.As(err, &existing) && existing.Kind == SourceUnavailable {
		return &SourceError{Kind: SourceUnavailable, Err: err}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &SourceError{Kind: SourceUnavailable, Err: err}
	}
	return &SourceError{Kind: SourceInvalid, Err: err}
}

func renderDigest(objects []DesiredObject) (string, error) {
	sorted := append([]DesiredObject(nil), objects...)
	sort.Slice(sorted, func(i, j int) bool { return refKey(sorted[i].Ref) < refKey(sorted[j].Ref) })
	canonical := make([]map[string]any, 0, len(sorted))
	for _, object := range sorted {
		document := object.Object.DeepCopy().Object
		if strings.EqualFold(object.Ref.Kind, "Secret") {
			// Identity only: renderers drop other Secret fields (type,
			// labels) in SecretsOmit, so only the identity agrees across modes.
			document = secretReference(document)
		}
		canonical = append(canonical, document)
	}
	// encoding/json sorts map keys, so equal objects encode identically.
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("digest desired objects: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func envDigest(env render.Env) (string, error) {
	env.Secrets = 0
	encoded, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("digest render environment: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func refKey(ref sdk.ObjectRef) string {
	return strings.Join([]string{ref.APIVersion, ref.Kind, ref.Namespace, ref.Name}, "\x00")
}
