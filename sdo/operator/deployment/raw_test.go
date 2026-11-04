package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

const synthDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeRawBackend struct {
	applied []byte
	images  map[string][]string
}

func (b *fakeRawBackend) BuildImage(context.Context, string, *contracts.ImageBuild) (string, error) {
	return "", errors.New("no builds expected")
}

func (b *fakeRawBackend) Apply(_ context.Context, documents []byte) error {
	b.applied = documents
	return nil
}

func (b *fakeRawBackend) Observe(_ context.Context, id string) (WorkloadObservation, error) {
	named := []rendered.DeployedImage{}
	for _, image := range b.images[id] {
		named = append(named, rendered.DeployedImage{Name: "app", Applied: image, Digest: image})
	}
	return WorkloadObservation{ResourceID: id, Revision: "tree", Images: b.images[id], Ready: true}, nil
}

type fakeResolver struct {
	digests map[string]string
	calls   []string
}

func (r *fakeResolver) ResolveImage(_ context.Context, ref string) (string, error) {
	r.calls = append(r.calls, ref)
	digest, ok := r.digests[ref]
	if !ok {
		return "", errors.New("synthetic registry has no such tag")
	}
	return digest, nil
}

func writeTagManifest(t *testing.T, image string) string {
	t.Helper()
	dir := t.TempDir()
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: cache\n  namespace: demo\nspec:\n  template:\n    spec:\n      containers:\n      - name: app\n        image: " + image + "\n"
	if err := os.WriteFile(filepath.Join(dir, "cache.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func rawPlan() *contracts.RawManifestStrategy {
	return &contracts.RawManifestStrategy{ManifestPaths: []string{"cache.yaml"}, ExpectedWorkloadIds: []string{"Deployment/demo/cache"}}
}

func TestRawPinsTagImagesAtAdmission(t *testing.T) {
	checkout := writeTagManifest(t, "redis:6.2.4")
	pinned := "redis@" + synthDigest
	backend := &fakeRawBackend{images: map[string][]string{"Deployment/demo/cache": {pinned}}}
	resolver := &fakeResolver{digests: map[string]string{"redis:6.2.4": pinned}}
	observation, err := (RawStrategy{Namespace: "demo", Backend: backend, Resolver: resolver}).ApplyAndVerify(context.Background(), checkout, "tree", rawPlan())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(backend.applied), "image: "+pinned) || strings.Contains(string(backend.applied), "redis:6.2.4") {
		t.Fatalf("applied manifest must carry only the pinned ref:\n%s", backend.applied)
	}
	if len(observation.Images) != 1 || observation.Images[0] != pinned {
		t.Fatalf("observed images = %v", observation.Images)
	}
	if len(resolver.calls) != 1 {
		t.Fatalf("resolver calls = %v, want one per distinct tag", resolver.calls)
	}
}

func TestRawLeavesDigestImagesAlone(t *testing.T) {
	pinned := "redis@" + synthDigest
	checkout := writeTagManifest(t, pinned)
	backend := &fakeRawBackend{images: map[string][]string{"Deployment/demo/cache": {pinned}}}
	resolver := &fakeResolver{}
	if _, err := (RawStrategy{Namespace: "demo", Backend: backend, Resolver: resolver}).ApplyAndVerify(context.Background(), checkout, "tree", rawPlan()); err != nil {
		t.Fatal(err)
	}
	if len(resolver.calls) != 0 {
		t.Fatalf("digest ref must not be resolved: %v", resolver.calls)
	}
}

func TestRawTagRejectionIsActionable(t *testing.T) {
	for name, strategy := range map[string]RawStrategy{
		"no resolver":    {Namespace: "demo", Backend: &fakeRawBackend{}},
		"resolver fails": {Namespace: "demo", Backend: &fakeRawBackend{}, Resolver: &fakeResolver{}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := strategy.ApplyAndVerify(context.Background(), writeTagManifest(t, "redis:6.2.4"), "tree", rawPlan())
			if err == nil {
				t.Fatal("unresolvable tag must be rejected")
			}
			for _, want := range []string{"redis:6.2.4", "@sha256:", "image_builds"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("rejection %q must mention %q", err, want)
				}
			}
		})
	}
}
