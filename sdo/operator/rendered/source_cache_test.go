//archcheck:ratchet R5 count=4 until=S3-10d
package rendered

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sync"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/sdk"
)

func TestCachedSourceReadsOneImmutableRevisionWithoutRereadingGit(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	first := cacheTestCommitManifests(t, root, "example/web:1")
	source := (Source{Repository: root, ManifestPaths: []string{"k8s"}}).WithCache()
	ctx := context.Background()
	objects, err := source.RawManifests(ctx, first, "app")
	if err != nil || len(objects) != 2 {
		t.Fatalf("initial source read: %v %v", objects, err)
	}
	// An immutable revision already loaded must need no further Git process or
	// YAML parse, even when different read methods and objects are requested.
	if err := os.Rename(root, root+"-away"); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	var readers sync.WaitGroup
	for range 64 {
		readers.Go(func() {
			object, err := source.AcceptedManifest(ctx, first, ref)
			if err != nil || object == nil {
				t.Errorf("repeat accepted read needs Git: %v", err)
				return
			}
			object.SetName("caller-mutation")
			raw, err := source.Revision(ctx, ref, first)
			if err != nil || raw.GetName() != "web" {
				t.Errorf("revision read leaked caller mutation: %v %v", raw, err)
			}
		})
	}
	readers.Wait()
	all, err := source.AcceptedManifests(ctx, first, "app")
	if err != nil || len(all) != 2 {
		t.Fatalf("inventory read needs Git: %v %v", all, err)
	}
	objects[0].SetName("raw-mutation")
	if got, err := source.RawManifests(ctx, first, "app"); err != nil || got[0].GetName() != "web" {
		t.Fatalf("raw caller mutation changed cached source: %v %v", got, err)
	}
}

func TestCachedSourceFollowsRevisionAndRetriesUnavailableSource(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	first := cacheTestCommitManifests(t, root, "example/web:1")
	source := (Source{Repository: root, ManifestPaths: []string{"k8s"}}).WithCache()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	if _, err := source.AcceptedManifest(ctx, first, ref); err != nil {
		t.Fatal(err)
	}
	second := cacheTestCommitManifests(t, root, "example/web:2")
	if err := os.Rename(root, root+"-away"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.AcceptedManifest(ctx, second, ref); err == nil {
		t.Fatal("new revision reused stale manifests")
	}
	if err := os.Rename(root+"-away", root); err != nil {
		t.Fatal(err)
	}
	object, err := source.AcceptedManifest(ctx, second, ref)
	if err != nil || cacheTestContainerImage(t, object) != "example/web:2" {
		t.Fatalf("unavailable revision was cached or stale source returned: %v %v", object, err)
	}
	foreign, err := source.RawManifests(ctx, second, "other")
	if err != nil || foreign[0].GetNamespace() != "other" {
		t.Fatalf("namespace was not part of cache identity: %v %v", foreign, err)
	}
}

func TestCachedSourceSeparatesSelectionsAndQualifiedScopes(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	if err := os.Mkdir(filepath.Join(root, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	foreign := "apiVersion: v1\nkind: Service\nmetadata: {name: peer, namespace: other}\nspec: {ports: [{port: 80}]}\n"
	if err := os.WriteFile(filepath.Join(root, "k8s", "foreign.yaml"), []byte(foreign), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := cacheTestCommitManifests(t, root, "example/web:1")
	source := (Source{Repository: root, ManifestPaths: []string{"k8s"}}).WithCache()
	ctx := context.Background()
	objects, err := source.RawManifests(ctx, revision, "app")
	if err != nil || len(objects) != 2 {
		t.Fatalf("application projection: %v %v", objects, err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "other", Name: "peer"}
	peer, err := source.AcceptedScopedManifest(ctx, revision, "app", ref)
	if err != nil || peer.GetNamespace() != "other" {
		t.Fatalf("qualified projection used application-only cache: %v %v", peer, err)
	}
	changed := source
	changed.ManifestPaths = []string{"not-selected"}
	if _, err := changed.RawManifests(ctx, revision, "app"); err == nil {
		t.Fatal("changed manifest selection reused previous source")
	}
}

func cacheTestCommitManifests(t *testing.T, root, image string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	deployment := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec:\n      containers:\n      - name: web\n        image: " + image + "\n"
	service := "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec: {ports: [{port: 80}]}\n"
	for name, body := range map[string]string{"deployment.yaml": deployment, "service.yaml": service} {
		if err := os.WriteFile(filepath.Join(root, "k8s", name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic manifests")
	return strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
}

func cacheTestContainerImage(t *testing.T, object *unstructured.Unstructured) string {
	t.Helper()
	containers, _, err := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	if err != nil || len(containers) != 1 {
		t.Fatalf("containers: %v %v", containers, err)
	}
	return containers[0].(map[string]any)["image"].(string)
}
