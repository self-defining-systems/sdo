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

// SourceImages provides FX1 image evidence keyed by workload, name and role.
type SourceImages interface {
	BaselineImage(sdk.ObjectRef, string, bool) (string, bool)
	BaselineTree(sdk.ObjectRef) string
}

// Source reads durable accepted locks and prepares controller-owned renders.
type Source struct {
	Evidence   []DeploymentEvidence
	Gaps       map[sdk.ObjectRef]string
	Repository string
	Images     SourceImages
	// Digests records pull identities separately when applied references are mutable.
	Digests       SourceImages
	ManifestPaths []string
	// DeployedCommit binds explicit image evidence for legacy deployments that
	// predate source-tree annotations. It is the trusted deployed revision,
	// never the advancing operational-memory head.
	DeployedCommit string
}

// AcceptedManifests returns rendered, namespace-scoped source objects that
// the drift change feed can own. Render failures return the available objects
// together with an explicit error; callers must not treat a partial set as complete.
func (s Source) AcceptedManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	return s.lockManifests(ctx, revision, namespace)
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
	return s.lockManifest(ctx, revision, ref)
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

func (s Source) renderDocument(ctx context.Context, document map[string]any, tree string, ref sdk.ObjectRef, path string) (*unstructured.Unstructured, error) {
	renderTree := ""
	useImages := s.Images != nil
	if useImages {
		observedTree := s.Images.BaselineTree(ref)
		deployedTree := observedTree
		if deployedTree == "" && s.DeployedCommit != "" {
			if !gitRevision.MatchString(s.DeployedCommit) {
				return nil, fmt.Errorf("full deployed commit revision is required")
			}
			var err error
			deployedTree, err = s.sourceTree(ctx, s.DeployedCommit)
			if err != nil {
				return nil, err
			}
		}
		useImages = deployedTree != ""
		if useImages {
			same, err := s.SameWorkloadInputs(ctx, deployedTree, tree, &unstructured.Unstructured{Object: document}, s.Evidence)
			if err != nil {
				return nil, fmt.Errorf("image source identity for %s/%s: %w", ref.Kind, ref.Name, err)
			}
			useImages = same
			if same {
				renderTree = observedTree
			}
		}
	}
	missingImage := false
	_, _, err := RenderManifest(document, ref.Namespace, renderTree, func(kind, name, container string, init bool, sourceImage string) string {
		if immutableImage.MatchString(sourceImage) {
			return sourceImage
		}
		if useImages {
			if image, ok := s.Images.BaselineImage(sdk.ObjectRef{APIVersion: ref.APIVersion, Kind: kind, Namespace: ref.Namespace, Name: name}, container, init); ok {
				return image
			}
		}
		if !strings.Contains(sourceImage, "@sha256:") {
			missingImage = true
		}
		return sourceImage
	})
	if err != nil {
		return nil, fmt.Errorf("render accepted manifest %s: %w", path, err)
	}
	if missingImage {
		return nil, fmt.Errorf("deployed image for %s/%s is unavailable from trusted deployment evidence", ref.Kind, ref.Name)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	object := &unstructured.Unstructured{}
	if err := json.Unmarshal(encoded, &object.Object); err != nil {
		return nil, err
	}
	return object, nil
}

func (s Source) sourceTree(ctx context.Context, revision string) (string, error) {
	data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "rev-parse", revision+"^{tree}").Output()
	if err != nil {
		return "", fmt.Errorf("source tree at %s: %w", revision, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// Memory publication changes the repository tree without deploying application
// bytes. All other paths remain relevant: a build may consume more than YAML.
func (s Source) sameImageSource(ctx context.Context, deployedTree, acceptedTree string) (bool, error) {
	if !gitRevision.MatchString(deployedTree) {
		return false, fmt.Errorf("invalid deployed source tree %q", deployedTree)
	}
	if deployedTree == acceptedTree {
		return true, nil
	}
	err := exec.CommandContext(ctx, "git", "-C", s.Repository, "diff", "--quiet", deployedTree, acceptedTree, "--", ".", ":(exclude).sdo").Run()
	if err == nil {
		return true, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("compare deployed source tree %s: %w", deployedTree, err)
}

// SameInputs compares all application inputs, excluding only operational memory.
func (s Source) SameInputs(ctx context.Context, left, right string) (bool, error) {
	if !gitRevision.MatchString(left) || !gitRevision.MatchString(right) {
		return false, errors.New("full source revisions required")
	}
	leftTree, err := s.sourceTree(ctx, left)
	if err != nil {
		return false, err
	}
	rightTree, err := s.sourceTree(ctx, right)
	if err != nil {
		return false, err
	}
	return s.sameImageSource(ctx, leftTree, rightTree)
}

// AcceptedScopedManifest is a read-only source comparison. The application's
// trusted lock proves source selection/freshness; it does not authorize foreign
// writes, image fallback or a foreign rendered lock.
func (s Source) AcceptedScopedManifest(ctx context.Context, revision, applicationNamespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if _, err := s.readLock(ctx, revision, applicationNamespace); err != nil {
		return nil, err
	}
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

// AcceptedApplicationManifest reads a source-declared object through the owning
// application's accepted render lock, including named cluster-scoped objects.
// Its full locked fields are usable for source restoration; arbitrary foreign
// observations still use the separately restricted AcceptedScopedManifest.
func (s Source) AcceptedApplicationManifest(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	snapshot, err := s.readLock(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	for _, entry := range snapshot.Lock.Objects {
		if entry.Ref == ref {
			return s.lockedObject(ctx, revision, snapshot, entry)
		}
	}
	return nil, fmt.Errorf("%w: %s/%s", ErrAcceptedManifestAbsent, ref.Kind, ref.Name)
}
