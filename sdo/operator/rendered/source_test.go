package rendered

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/sdk"
)

const (
	sourceDeployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec:\n      containers:\n      - name: web\n        image: %s\n"
	sourceService    = "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec:\n  selector: {app: web}\n  ports:\n  - port: 80\n"
)

func commitManifests(t *testing.T, root, image string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"deployment.yaml": strings.Replace(sourceDeployment, "%s", image, 1), "service.yaml": sourceService} {
		if err := os.WriteFile(filepath.Join(root, "k8s", name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "manifests "+image)
	return strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
}

func containerImage(t *testing.T, object *unstructured.Unstructured) string {
	t.Helper()
	containers, _, err := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	if err != nil || len(containers) != 1 {
		t.Fatalf("containers: %v %v", containers, err)
	}
	return containers[0].(map[string]any)["image"].(string)
}

// Accepted source renders straight from Git at the asked revision, with no
// lock, receipt or image evidence: the image is the string the manifest wrote.
func TestSourceRendersAcceptedManifestsFromGitWithImagesAsDeclared(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	first := commitManifests(t, root, "example/web:1")
	second := commitManifests(t, root, "example/web@sha256:"+strings.Repeat("b", 64))
	source := Source{Repository: root, ManifestPaths: []string{"k8s"}}
	ctx := context.Background()

	objects, err := source.AcceptedManifests(ctx, first, "app")
	if err != nil || len(objects) != 2 {
		t.Fatalf("accepted manifests: %d %v", len(objects), err)
	}
	byKind := map[string]*unstructured.Unstructured{}
	for _, object := range objects {
		byKind[object.GetKind()] = object
		if object.GetNamespace() != "app" {
			t.Fatalf("%s rendered into %q, want the application namespace", object.GetKind(), object.GetNamespace())
		}
	}
	if got := containerImage(t, byKind["Deployment"]); got != "example/web:1" {
		t.Fatalf("a tag-only image was rewritten: %q", got)
	}

	raw, err := source.RawManifests(ctx, second, "app")
	if err != nil || len(raw) != 2 {
		t.Fatalf("raw manifests: %d %v", len(raw), err)
	}
	for _, object := range raw {
		if object.GetKind() == "Deployment" && containerImage(t, object) != "example/web@sha256:"+strings.Repeat("b", 64) {
			t.Fatalf("raw render did not follow the asked revision: %q", containerImage(t, object))
		}
	}

	one, err := source.AcceptedManifest(ctx, second, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "web"})
	if err != nil || one.GetKind() != "Service" {
		t.Fatalf("accepted manifest: %v %v", one, err)
	}
	if _, err := source.AcceptedManifest(ctx, second, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "absent"}); err == nil {
		t.Fatal("an object absent from source rendered")
	}
}
