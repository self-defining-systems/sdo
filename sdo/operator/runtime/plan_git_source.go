package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
)

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// SourceImages returns images captured at the last healthy deployment.
type SourceImages interface {
	BaselineImage(sdk.ObjectRef, string, bool) (string, bool)
	BaselineTree(sdk.ObjectRef) string
}

// GitPlanSource reads accepted manifests and applies the deployment renderer.
type GitPlanSource struct {
	Repository    string
	Images        SourceImages
	ManifestPaths []string
}

// AcceptedManifests returns rendered, namespace-scoped source objects that
// the drift change feed can own.
func (s GitPlanSource) AcceptedManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	return s.manifests(ctx, revision, namespace, true)
}

func (s GitPlanSource) manifests(ctx context.Context, revision, namespace string, render bool) ([]*unstructured.Unstructured, error) {
	if s.Repository == "" || !gitRevision.MatchString(revision) || namespace == "" {
		return nil, fmt.Errorf("accepted repository, full commit revision, and namespace are required")
	}
	files, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "ls-tree", "-r", "--name-only", revision).Output()
	if err != nil {
		return nil, fmt.Errorf("list manifests at %s: %w", revision, err)
	}
	paths, err := deployment.SelectManifestPaths(strings.Fields(string(files)), s.ManifestPaths)
	if err != nil {
		return nil, err
	}
	treeBytes, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "rev-parse", revision+"^{tree}").Output()
	if err != nil {
		return nil, fmt.Errorf("source tree at %s: %w", revision, err)
	}
	tree := strings.TrimSpace(string(treeBytes))
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
			object := &unstructured.Unstructured{Object: document}
			if object.GetAPIVersion() == "" || object.GetKind() == "" || object.GetName() == "" {
				continue
			}
			if object.GetNamespace() != "" && object.GetNamespace() != namespace {
				continue
			}
			ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: namespace, Name: object.GetName()}
			if !supportsAcceptedSource(ref) {
				continue
			}
			if seen[ref] {
				return nil, fmt.Errorf("%w: duplicate accepted manifest for %s/%s", deployment.ErrManifestSelectionUnavailable, ref.Kind, ref.Name)
			}
			seen[ref] = true
			if !render {
				object.SetNamespace(namespace)
				objects = append(objects, object)
				continue
			}
			rendered, err := s.renderDocument(document, tree, ref, path)
			if err != nil {
				return nil, err
			}
			objects = append(objects, rendered)
		}
	}
	return objects, nil
}

func (s GitPlanSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.manifest(ctx, revision, ref, true)
}

func (s GitPlanSource) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	return s.manifest(ctx, revision, ref, false)
}

func (s GitPlanSource) manifest(ctx context.Context, revision string, ref sdk.ObjectRef, render bool) (*unstructured.Unstructured, error) {
	objects, err := s.manifests(ctx, revision, ref.Namespace, render)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.GetAPIVersion() == ref.APIVersion && object.GetKind() == ref.Kind && object.GetName() == ref.Name && object.GetNamespace() == ref.Namespace {
			return object, nil
		}
	}
	return nil, fmt.Errorf("accepted manifest for %s/%s/%s is absent at %s", ref.Kind, ref.Namespace, ref.Name, revision)
}

func (s GitPlanSource) renderDocument(document map[string]any, tree string, ref sdk.ObjectRef, path string) (*unstructured.Unstructured, error) {
	renderTree := ""
	if s.Images != nil && s.Images.BaselineTree(ref) == tree {
		renderTree = tree
	}
	missingImage := false
	_, _, err := deployment.RenderManifest(document, ref.Namespace, renderTree, func(kind, name, container string, init bool, sourceImage string) string {
		if s.Images != nil {
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
		return nil, fmt.Errorf("deployed image for %s/%s is unavailable from the healthy baseline", ref.Kind, ref.Name)
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
