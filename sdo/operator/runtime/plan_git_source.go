package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// GitPlanSource reads raw manifests from the accepted repository revision.
type GitPlanSource struct{ Repository string }

func (s GitPlanSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.manifest(ctx, revision, ref)
}

func (s GitPlanSource) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	return s.manifest(ctx, revision, ref)
}

func (s GitPlanSource) manifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if s.Repository == "" || !gitRevision.MatchString(revision) {
		return nil, fmt.Errorf("accepted repository and full commit revision are required")
	}
	files, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "ls-tree", "-r", "--name-only", revision).Output()
	if err != nil {
		return nil, fmt.Errorf("list manifests at %s: %w", revision, err)
	}
	for _, path := range strings.Split(string(files), "\n") {
		if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") && !strings.HasSuffix(path, ".json") {
			continue
		}
		if strings.HasPrefix(path, ".sdo/") {
			continue
		}
		data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "show", revision+":"+path).Output()
		if err != nil {
			return nil, fmt.Errorf("read accepted manifest %s: %w", path, err)
		}
		if len(data) > 2<<20 {
			continue
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var document map[string]any
			if err := decoder.Decode(&document); err == io.EOF {
				break
			} else if err != nil {
				break
			}
			if document["apiVersion"] != ref.APIVersion || document["kind"] != ref.Kind {
				continue
			}
			object := &unstructured.Unstructured{Object: document}
			if object.GetNamespace() == "" {
				object.SetNamespace(ref.Namespace)
			}
			if object.GetNamespace() == ref.Namespace && object.GetName() == ref.Name {
				return object, nil
			}
		}
	}
	return nil, fmt.Errorf("accepted manifest for %s/%s/%s is absent at %s", ref.Kind, ref.Namespace, ref.Name, revision)
}
