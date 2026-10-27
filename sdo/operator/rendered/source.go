package rendered

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

// ErrAcceptedManifestAbsent distinguishes confirmed absence from unavailable source.
var ErrAcceptedManifestAbsent = errors.New("object is absent from accepted manifests")

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// Source reads accepted manifests from Git and renders them as source writes
// them: images keep the exact string the manifest declares, tag or digest.
type Source struct {
	Repository    string
	ManifestPaths []string
}

// AcceptedManifests returns rendered, namespace-scoped source objects that
// the drift change feed can own. Render failures return the available objects
// together with an explicit error; callers must not treat a partial set as complete.
func (s Source) AcceptedManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	objects := make([]*unstructured.Unstructured, 0, len(raw))
	var failures []error
	for _, object := range raw {
		rendered, err := s.render(object)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		objects = append(objects, rendered)
	}
	return objects, errors.Join(failures...)
}

func (s Source) RawManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	return s.rawManifests(ctx, revision, namespace, false)
}
func (s Source) rawManifests(ctx context.Context, revision, namespace string, qualified bool) ([]*unstructured.Unstructured, error) {
	if s.Repository == "" || !gitRevision.MatchString(revision) || namespace == "" {
		return nil, fmt.Errorf("accepted repository, full commit revision, and namespace are required")
	}
	files, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "ls-tree", "-r", "--name-only", revision).Output()
	if err != nil {
		return nil, fmt.Errorf("list manifests at %s: %w", revision, err)
	}
	paths, err := SelectManifestPaths(strings.Fields(string(files)), s.ManifestPaths)
	if err != nil {
		return nil, err
	}
	objects := make([]*unstructured.Unstructured, 0)
	seen := map[sdk.ObjectRef]bool{}
	for _, path := range paths {
		data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "show", revision+":"+path).Output()
		if err != nil {
			return nil, fmt.Errorf("read accepted manifest %s: %w", path, err)
		}
		if len(data) > 2<<20 {
			return nil, fmt.Errorf("accepted manifest %s exceeds 2 MiB", path)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var document map[string]any
			if err := decoder.Decode(&document); err == io.EOF {
				break
			} else if err != nil {
				return nil, fmt.Errorf("decode accepted manifest %s: %w", path, err)
			}
			// Unstructured values must use JSON numeric types before DeepCopy or
			// Kubernetes conversion; YAML's native int values are not supported.
			encoded, err := json.Marshal(document)
			if err != nil {
				return nil, err
			}
			object := &unstructured.Unstructured{}
			if err := json.Unmarshal(encoded, &object.Object); err != nil {
				return nil, err
			}
			if object.GetAPIVersion() == "" || object.GetKind() == "" || object.GetName() == "" {
				continue
			}
			objectNamespace := namespace
			if qualified {
				objectNamespace = object.GetNamespace()
				if (sdk.ObservationTarget{Kind: object.GetKind()}).Cluster() {
					objectNamespace = ""
				} else if objectNamespace == "" {
					objectNamespace = namespace
				}
			} else if object.GetNamespace() != "" && object.GetNamespace() != namespace {
				continue
			}
			if ClusterScopedKind(object.GetKind()) {
				objectNamespace = ""
			}
			ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: objectNamespace, Name: object.GetName()}

			if seen[ref] {
				return nil, fmt.Errorf("%w: duplicate accepted manifest for %s/%s", ErrManifestSelectionUnavailable, ref.Kind, ref.Name)
			}
			seen[ref] = true
			object.SetNamespace(objectNamespace)
			objects = append(objects, object)
		}
	}
	return objects, nil
}

func (s Source) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.acceptedObject(ctx, revision, ref.Namespace, ref)
}

func (s Source) acceptedObject(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	for _, object := range raw {
		if objectRef(object) == ref {
			return s.render(object)
		}
	}
	return nil, fmt.Errorf("%w: %s/%s", ErrAcceptedManifestAbsent, ref.Kind, ref.Name)
}

func (s Source) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	objects, err := s.RawManifests(ctx, revision, ref.Namespace)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.GetAPIVersion() == ref.APIVersion && object.GetKind() == ref.Kind && object.GetName() == ref.Name && object.GetNamespace() == ref.Namespace {
			return object, nil
		}
	}
	return nil, fmt.Errorf("%w: %s/%s/%s at %s", ErrAcceptedManifestAbsent, ref.Kind, ref.Namespace, ref.Name, revision)
}

// render applies the namespace default and returns a copy; images are kept
// exactly as source declares them.
func (s Source) render(object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	document := object.DeepCopy().Object
	if _, _, err := RenderManifest(document, object.GetNamespace(), "", nil); err != nil {
		return nil, fmt.Errorf("render accepted manifest %s/%s: %w", object.GetKind(), object.GetName(), err)
	}
	return &unstructured.Unstructured{Object: document}, nil
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
