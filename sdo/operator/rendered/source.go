package rendered

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/sdk"
)

// ErrAcceptedManifestAbsent distinguishes confirmed absence from unavailable source.
var ErrAcceptedManifestAbsent = errors.New("object is absent from accepted manifests")

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// Source reads accepted desired state from Git: the accepted commit's tree,
// rendered by the configured DeploymentSource through the renderer catalog
// with Secret values omitted. Images keep the exact string the renderer
// emits, tag or digest. Deploy and reads share the one Desired function.
type Source struct {
	Repository string
	// Deployment names the renderer and its config. Nil reads fail with
	// SourceUnconfigured.
	Deployment *contracts.DeploymentSource
	// Catalog holds the renderers Deployment may name.
	Catalog *render.Catalog
	// Env is the recorded render environment. Each read sets its namespace and
	// forces SecretsOmit.
	Env render.Env
	// Git, when set, replaces the git process edge (`git -C root args...`
	// standard output). A simulation supplies the accepted tree without
	// spawning a process.
	Git   GitFunc
	cache *manifestCache
}

// WithCache shares immutable accepted manifests across object reads. The cache
// belongs to this source instance; it never caches live API reads or failures.
// Configure the source and its Git reader before creating the cached instance.
func (s Source) WithCache() Source {
	if s.Deployment != nil {
		s.Deployment = proto.Clone(s.Deployment).(*contracts.DeploymentSource)
	}
	s.cache = &manifestCache{}
	return s
}

// AcceptedManifests returns rendered, namespace-scoped source objects that
// the drift change feed can own.
func (s Source) AcceptedManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	raw, err := s.rawManifests(ctx, revision, namespace, false)
	if err != nil {
		return nil, err
	}
	return copyManifests(raw), nil
}

func (s Source) RawManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	objects, err := s.rawManifests(ctx, revision, namespace, false)
	return copyManifests(objects), err
}

// rawManifests is immutable inside this package. Public reads copy the objects
// they return so no caller can change another reader's accepted source.
func (s Source) rawManifests(ctx context.Context, revision, namespace string, qualified bool) ([]*unstructured.Unstructured, error) {
	var objects []DesiredObject
	var err error
	if s.cache != nil {
		objects, err = s.cache.read(ctx, s, revision, namespace)
	} else {
		objects, err = s.loadManifests(ctx, revision, namespace)
	}
	if err != nil {
		return nil, classifySource(err)
	}
	return readView(objects, qualified), nil
}

// readView projects the desired set for reads: unqualified reads see only
// the application namespace and cluster-scoped objects; qualified reads see
// every object (D5).
func readView(objects []DesiredObject, qualified bool) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(objects))
	for _, object := range objects {
		if object.Foreign && !qualified {
			continue
		}
		out = append(out, object.Object)
	}
	return out
}

func (s Source) env(namespace string) render.Env {
	env := s.Env
	env.Namespace = namespace
	env.Secrets = render.SecretsOmit
	return env
}

// loadManifests renders revision^{tree} and keeps the non-ephemeral objects,
// with Secrets reduced to identity (defense in depth beyond SecretsOmit).
func (s Source) loadManifests(ctx context.Context, revision, namespace string) ([]DesiredObject, error) {
	set, err := s.desired(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	objects := set.Objects()
	for i, object := range objects {
		if strings.EqualFold(object.Ref.Kind, "Secret") {
			objects[i].Object = &unstructured.Unstructured{Object: secretReference(object.Object.Object)}
		}
	}
	return objects, nil
}

// ShapeDigest returns the desired-state shape digest of revision (see
// DesiredSet.ShapeDigest). Health refresh compares it between the last
// judged revision and the accepted one. It reads Git, never the cache.
func (s Source) ShapeDigest(ctx context.Context, revision, namespace string) (string, error) {
	set, err := s.desired(ctx, revision, namespace)
	if err != nil {
		return "", classifySource(err)
	}
	return set.ShapeDigest, nil
}

// ShapeOf adapts the source to the injected function memory.AcceptedHealthRevision
// takes: the shape of repository at revision, in namespace. A nil source
// returns nil so callers fall back to source bytes (transitional, S3-11c).
func (s Source) ShapeOf(namespace string) func(ctx context.Context, repository, revision string) (string, error) {
	if s.Deployment == nil {
		return nil
	}
	return func(ctx context.Context, repository, revision string) (string, error) {
		s.Repository, s.cache = repository, nil
		return s.ShapeDigest(ctx, revision, namespace)
	}
}

func (s Source) desired(ctx context.Context, revision, namespace string) (DesiredSet, error) {
	if s.Deployment == nil {
		return DesiredSet{}, &SourceError{Kind: SourceUnconfigured, Err: errors.New("no deployment source is configured")}
	}
	if s.Repository == "" || !gitRevision.MatchString(revision) || namespace == "" {
		return DesiredSet{}, sourceErrorf(SourceInvalid, "accepted repository, full commit revision, and namespace are required")
	}
	store := ObjectStore{Dir: s.Repository, Git: s.Git}
	tree, err := store.git(ctx, "rev-parse", "--verify", "--quiet", revision+"^{tree}")
	if err != nil {
		return DesiredSet{}, sourceErrorf(SourceUnavailable, "resolve the tree of %s: %w", revision, err)
	}
	effective, err := Effective(ctx, store, BaseTree(strings.TrimSpace(string(tree))))
	if err != nil {
		return DesiredSet{}, err
	}
	fsys, err := GitTreeFS(ctx, store, effective)
	if err != nil {
		return DesiredSet{}, err
	}
	deployment, err := legacyReadSource(fsys, s.Deployment)
	if err != nil {
		return DesiredSet{}, err
	}
	return Desired(ctx, fsys, s.env(namespace), s.Catalog, deployment)
}

func (s Source) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.acceptedObject(ctx, revision, ref.Namespace, ref)
}

func (s Source) acceptedObject(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	raw, err := s.rawManifests(ctx, revision, namespace, false)
	if err != nil {
		return nil, err
	}
	for _, object := range raw {
		if objectRef(object) == ref {
			return object.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("%w: %s/%s", ErrAcceptedManifestAbsent, ref.Kind, ref.Name)
}

func (s Source) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	objects, err := s.rawManifests(ctx, revision, ref.Namespace, false)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.GetAPIVersion() == ref.APIVersion && object.GetKind() == ref.Kind && object.GetName() == ref.Name && object.GetNamespace() == ref.Namespace {
			return object.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("%w: %s/%s/%s at %s", ErrAcceptedManifestAbsent, ref.Kind, ref.Namespace, ref.Name, revision)
}

func objectRef(o *unstructured.Unstructured) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
}

// AcceptedScopedManifest is a read-only source comparison; it does not
// authorize foreign writes.
func (s Source) AcceptedScopedManifest(ctx context.Context, revision, applicationNamespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	target := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
	if target.Cluster() {
		target.Name = ref.Name
	}
	if err := (sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}).Validate(); err != nil {
		return nil, err
	}
	objects, err := s.rawManifests(ctx, revision, applicationNamespace, true)
	if err != nil {
		return nil, err
	}
	for _, o := range objects {
		if objectRef(o) == ref {
			return sdk.ProjectObservationObject(o)
		}
	}
	return nil, ErrAcceptedManifestAbsent
}

// AcceptedApplicationManifest reads a source-declared object, including named
// cluster-scoped objects, for source restoration; arbitrary foreign
// observations still use the separately restricted AcceptedScopedManifest.
func (s Source) AcceptedApplicationManifest(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.acceptedObject(ctx, revision, namespace, ref)
}

// secretReference keeps a Secret's identity only: last-applied annotations and
// arbitrary extension fields can contain credential bytes too.
func secretReference(object map[string]any) map[string]any {
	metadata, _ := object["metadata"].(map[string]any)
	reference := map[string]any{"name": metadata["name"]}
	if namespace, ok := metadata["namespace"]; ok {
		reference["namespace"] = namespace
	}
	return map[string]any{"apiVersion": object["apiVersion"], "kind": "Secret", "metadata": reference}
}
