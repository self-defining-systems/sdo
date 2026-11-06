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
	"sdo.dev/operator/renderenv"
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
	// Environment is the recorded install-time render environment (nil is
	// unknown). Each read sets its namespace and forces SecretsOmit.
	Environment *contracts.RenderEnvironment
	// Git, when set, replaces the git process edge (`git -C root args...`
	// standard output). A simulation supplies the accepted tree without
	// spawning a process.
	Git GitFunc
	// Pins supplies the image pins of the accepted deployment. Reads of a
	// revision whose effective tree is the pins' EffectiveTree return each
	// pinned container with the digest the deployment applied instead of the
	// tag the source declares: the deployed image is part of desired state, so
	// drift, restore and closure never compare a digest-pinned workload with a
	// bare tag. Another effective tree keeps the source image until a new
	// accepted deployment pins it. Digests of the render are unchanged: the
	// pins apply after rendering. Nil means no pins.
	Pins  PinProvider
	cache *manifestCache
}

// PinProvider supplies the image pins of the newest accepted deployment.
// ReceiptPins is the fixed form (onboarding staging holds the receipt it just
// verified); the commit broker supplies them from its durable evidence ledger.
type PinProvider interface {
	AcceptedImagePins(ctx context.Context) (ReceiptPins, error)
}

// ReceiptPins are the container images a verified deployment applied, bound to
// the effective tree they were applied for. The effective tree, not the commit
// tree, is the key: it does not move for memory-only commits, and it changes
// exactly when the rendered inputs change.
type ReceiptPins struct {
	EffectiveTree string
	// BaseTree is the commit tree the deployment applied. With it a later
	// effective tree can verify which pins its image inputs still match.
	// Empty keeps exact-tree binding only.
	BaseTree string
	Images   []DeployedImage

	// inherited marks pins carried to a later effective tree: each applies
	// only to a container still declaring the image it was built from.
	inherited bool
}

// AcceptedImagePins makes ReceiptPins a PinProvider.
func (p ReceiptPins) AcceptedImagePins(context.Context) (ReceiptPins, error) { return p, nil }

func (p ReceiptPins) active() bool { return p.EffectiveTree != "" && len(p.Images) > 0 }

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
	return renderenv.For(s.Environment, namespace, render.SecretsOmit)
}

// loadManifests renders revision^{tree} and keeps the non-ephemeral objects,
// with Secrets reduced to identity (defense in depth beyond SecretsOmit).
func (s Source) loadManifests(ctx context.Context, revision, namespace string) ([]DesiredObject, error) {
	if err := s.requireDesired(namespace); err != nil {
		return nil, err
	}
	effective, err := s.effectiveAt(ctx, revision)
	if err != nil {
		return nil, err
	}
	defer effective.Close()
	set, err := s.desiredFrom(ctx, effective, namespace)
	if err != nil {
		return nil, err
	}
	objects := set.Objects()
	pins, err := s.pinsFor(ctx, effective)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.Workload && pins.active() {
			pins.apply(object.Object.Object, object.Ref)
		}
	}
	for i, object := range objects {
		if strings.EqualFold(object.Ref.Kind, "Secret") {
			objects[i].Object = &unstructured.Unstructured{Object: render.SecretIdentity(object.Object.Object)}
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
// takes: the shape of repository at revision, in namespace. A source with no
// DeploymentSource has no renderer to read a shape from, so its "shape" is the
// effective tree ID: it changes exactly when a source file, a deployment view
// or the patch series changes, never for memory-only commits.
func (s Source) ShapeOf(namespace string) func(ctx context.Context, repository, revision string) (string, error) {
	return func(ctx context.Context, repository, revision string) (string, error) {
		s.Repository, s.cache = repository, nil
		if s.Deployment == nil {
			effective, err := s.effectiveAt(ctx, revision)
			if err != nil {
				return "", classifySource(err)
			}
			defer effective.Close()
			return effective.ID, nil
		}
		return s.ShapeDigest(ctx, revision, namespace)
	}
}

// effectiveAt resolves revision's tree and its effective tree. The caller
// closes the result.
func (s Source) effectiveAt(ctx context.Context, revision string) (EffectiveTree, error) {
	if s.Repository == "" || !gitRevision.MatchString(revision) {
		return EffectiveTree{}, sourceErrorf(SourceInvalid, "accepted repository and full commit revision are required")
	}
	store := ObjectStore{Dir: s.Repository, Git: s.Git}
	tree, err := store.git(ctx, "rev-parse", "--verify", "--quiet", revision+"^{tree}")
	if err != nil {
		return EffectiveTree{}, sourceErrorf(SourceUnavailable, "resolve the tree of %s: %w", revision, err)
	}
	return Effective(ctx, store, BaseTree(strings.TrimSpace(string(tree))))
}

func (s Source) desired(ctx context.Context, revision, namespace string) (DesiredSet, error) {
	if err := s.requireDesired(namespace); err != nil {
		return DesiredSet{}, err
	}
	effective, err := s.effectiveAt(ctx, revision)
	if err != nil {
		return DesiredSet{}, err
	}
	defer effective.Close()
	return s.desiredFrom(ctx, effective, namespace)
}

func (s Source) requireDesired(namespace string) error {
	if s.Deployment == nil {
		return &SourceError{Kind: SourceUnconfigured, Err: errors.New("no deployment source is configured")}
	}
	if namespace == "" {
		return sourceErrorf(SourceInvalid, "accepted repository, full commit revision, and namespace are required")
	}
	return nil
}

// desiredFrom renders an already resolved effective tree.
func (s Source) desiredFrom(ctx context.Context, effective EffectiveTree, namespace string) (DesiredSet, error) {
	fsys, err := GitTreeFS(ctx, effective)
	if err != nil {
		return DesiredSet{}, err
	}
	deployment, err := legacyReadSource(fsys, s.Deployment)
	if err != nil {
		return DesiredSet{}, err
	}
	return Desired(ctx, fsys, s.env(namespace), s.Catalog, deployment)
}

// pinsFor returns the pins that apply to effective. A provider failure fails
// the read; falling back to tags would report the deployed digests as drift.
//
// Pins are bound to the effective tree the deployment applied. A later
// effective tree (an accepted responder patch, a values or overlay change, any
// source commit) has no deployment evidence of its own, so each pinned
// container inherits its pin when nothing its image was built from changed
// (see inheritedPins); a container whose image inputs changed has no pin and
// reads the source image until a deployment builds it and records new pins.
func (s Source) pinsFor(ctx context.Context, effective EffectiveTree) (ReceiptPins, error) {
	if s.Pins == nil {
		return ReceiptPins{}, nil
	}
	pins, err := s.Pins.AcceptedImagePins(ctx)
	if err != nil {
		return ReceiptPins{}, sourceErrorf(SourceUnavailable, "read accepted deployment image pins: %w", err)
	}
	if !pins.active() {
		return ReceiptPins{}, nil
	}
	if pins.EffectiveTree == effective.ID {
		return pins, nil
	}
	return s.inheritedPins(ctx, pins, effective)
}

// inheritedPins keeps the pins of the deployed tree whose image inputs are
// unchanged in effective: the declared source image (checked against the
// rendered container at apply) and, for a built image, the Git objects at its
// recorded context and Dockerfile paths. Unknown inputs (the zero ImageInputs)
// and an unverifiable deployed tree inherit nothing: a pin is never carried on
// a guess. A context of "." covers the whole effective tree, so any source or
// deployment-view change moves it; that errs toward requiring a deployment.
func (s Source) inheritedPins(ctx context.Context, pins ReceiptPins, effective EffectiveTree) (ReceiptPins, error) {
	if pins.BaseTree == "" {
		return ReceiptPins{}, nil
	}
	store := ObjectStore{Dir: s.Repository, Git: s.Git}
	deployed, err := Effective(ctx, store, BaseTree(pins.BaseTree))
	if err != nil {
		var sourceErr *SourceError
		if errors.As(err, &sourceErr) && sourceErr.Kind == SourceUnavailable {
			return ReceiptPins{}, err
		}
		return ReceiptPins{}, nil
	}
	defer deployed.Close()
	if deployed.ID != pins.EffectiveTree {
		return ReceiptPins{}, nil
	}
	kept := ReceiptPins{EffectiveTree: effective.ID, inherited: true}
	for _, pin := range pins.Images {
		same, err := sameImageInputs(ctx, pin.Inputs, deployed, effective)
		if err != nil {
			return ReceiptPins{}, err
		}
		if same {
			kept.Images = append(kept.Images, pin)
		}
	}
	return kept, nil
}

func sameImageInputs(ctx context.Context, inputs ImageInputs, deployed, current EffectiveTree) (bool, error) {
	if inputs.SourceImage == "" || inputs.Validate() != nil {
		return false, nil
	}
	for _, p := range []string{inputs.ContextPath, inputs.DockerfilePath} {
		if p == "" {
			continue
		}
		before, err := pathObject(ctx, deployed, p)
		if err != nil {
			return false, err
		}
		after, err := pathObject(ctx, current, p)
		if err != nil {
			return false, err
		}
		if before != after {
			return false, nil
		}
	}
	return true, nil
}

// pathObject names what a tree holds at p: the tree ID itself for ".", the
// ls-tree entry (mode, type, object ID) otherwise, empty when absent.
func pathObject(ctx context.Context, tree EffectiveTree, p string) (string, error) {
	if p == "." {
		return tree.ID, nil
	}
	out, err := tree.Store.git(ctx, "ls-tree", "-z", tree.ID, "--", p)
	if err != nil {
		return "", sourceErrorf(SourceUnavailable, "read %s of effective tree %s: %w", p, tree.ID, err)
	}
	entry, _, _ := strings.Cut(string(out), "\x00")
	meta, _, _ := strings.Cut(entry, "\t")
	return meta, nil
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

// apply replaces the image of each container of the workload that a pin names.
func (p ReceiptPins) apply(document map[string]any, ref sdk.ObjectRef) {
	for _, field := range []string{"initContainers", "containers"} {
		containers, found, _ := unstructured.NestedSlice(document, "spec", "template", "spec", field)
		if !found {
			continue
		}
		changed := false
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			name, _ := container["name"].(string)
			for _, pin := range p.Images {
				if pin.Ref.Kind == ref.Kind && pin.Ref.Namespace == ref.Namespace && pin.Ref.Name == ref.Name &&
					pin.Name == name && pin.Init == (field == "initContainers") && pin.Applied != "" &&
					(!p.inherited || container["image"] == pin.Inputs.SourceImage) {
					container["image"] = pin.Applied
					changed = true
				}
			}
		}
		if changed {
			_ = unstructured.SetNestedSlice(document, containers, "spec", "template", "spec", field)
		}
	}
}
