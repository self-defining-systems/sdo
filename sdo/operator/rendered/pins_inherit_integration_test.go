//go:build integration

package rendered

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

const inheritedDigest = "example/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const sidecarDigest = "example/side@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

const twoContainerManifest = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web, namespace: app}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec:\n      containers:\n      - {name: web, image: \"example/web:1\"}\n      - {name: side, image: \"example/side:1\"}\n"

// pinFixture is a deployed source: a built web image (context app/web), a built
// sidecar (context app/side) and a docs file no image reads. pins are what the
// deployment recorded for the first commit.
func pinFixture(t *testing.T) (dir string, pins ReceiptPins) {
	t.Helper()
	dir = t.TempDir()
	runGit(t, dir, "init", "-q")
	write(t, dir, "k8s/web.yaml", twoContainerManifest)
	write(t, dir, "app/web/Dockerfile", "FROM scratch\n")
	write(t, dir, "app/web/main.go", "package main\n")
	write(t, dir, "app/side/Dockerfile", "FROM scratch\n")
	write(t, dir, "docs/notes.md", "one\n")
	base := commitTree(t, dir)
	eff := effective(t, ObjectStore{Dir: dir}, base)
	web := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	return dir, ReceiptPins{EffectiveTree: eff.ID, BaseTree: string(base), Images: []DeployedImage{
		{Ref: web, Name: "web", Applied: inheritedDigest, Digest: inheritedDigest, Inputs: ImageInputs{SourceImage: "example/web:1", ContextPath: "app/web", DockerfilePath: "app/web/Dockerfile"}},
		{Ref: web, Name: "side", Applied: sidecarDigest, Digest: sidecarDigest, Inputs: ImageInputs{SourceImage: "example/side:1", ContextPath: "app/side", DockerfilePath: "app/side/Dockerfile"}},
	}}
}

func containerImages(t *testing.T, dir string, pins PinProvider) map[string]string {
	t.Helper()
	source := Source{Repository: dir, Deployment: rawSource(t, "k8s"), Catalog: testCatalog(t), Pins: pins}
	objects, err := source.RawManifests(context.Background(), runGit(t, dir, "rev-parse", "HEAD"), "app")
	if err != nil || len(objects) != 1 {
		t.Fatalf("RawManifests = %v, %v", objects, err)
	}
	containers, _, _ := unstructured.NestedSlice(objects[0].Object, "spec", "template", "spec", "containers")
	got := map[string]string{}
	for _, raw := range containers {
		c := raw.(map[string]any)
		got[c["name"].(string)], _ = c["image"].(string)
	}
	return got
}

// An accepted change with no deployment of its own (a responder patch, values
// or overlay edit, any source commit) must not move the deployed images out of
// desired state when nothing an image was built from changed, and must stop
// pinning exactly the containers whose build inputs or declared image changed.
func TestPinsFollowTheImageInputsAcrossEffectiveTreeChanges(t *testing.T) {
	for name, tc := range map[string]struct {
		edit     func(t *testing.T, dir string)
		pins     func(ReceiptPins) ReceiptPins
		web, sid string
	}{
		"same tree": {edit: func(*testing.T, string) {}, web: inheritedDigest, sid: sidecarDigest},
		"a file no image reads": {
			edit: func(t *testing.T, dir string) { write(t, dir, "docs/notes.md", "two\n") },
			web:  inheritedDigest, sid: sidecarDigest,
		},
		"a values file": {
			edit: func(t *testing.T, dir string) { write(t, dir, ".sdo/deployment/values/v.yaml", "v: 2\n") },
			web:  inheritedDigest, sid: sidecarDigest,
		},
		"a manifest change that keeps the images": {
			edit: func(t *testing.T, dir string) {
				write(t, dir, "k8s/web.yaml", strings.Replace(twoContainerManifest, "namespace: app}", "namespace: app, labels: {x: y}}", 1))
			},
			web: inheritedDigest, sid: sidecarDigest,
		},
		"the web build context": {
			edit: func(t *testing.T, dir string) { write(t, dir, "app/web/main.go", "package main // patched\n") },
			web:  "example/web:1", sid: sidecarDigest,
		},
		"the web Dockerfile": {
			edit: func(t *testing.T, dir string) { write(t, dir, "app/web/Dockerfile", "FROM alpine\n") },
			web:  "example/web:1", sid: sidecarDigest,
		},
		"the declared web image": {
			edit: func(t *testing.T, dir string) {
				write(t, dir, "k8s/web.yaml", strings.Replace(twoContainerManifest, "example/web:1", "example/web:2", 1))
			},
			web: "example/web:2", sid: sidecarDigest,
		},
		"a deleted build context": {
			edit: func(t *testing.T, dir string) { runGit(t, dir, "rm", "-rq", "app/side") },
			web:  inheritedDigest, sid: "example/side:1",
		},
		"unknown inputs inherit nothing": {
			edit: func(t *testing.T, dir string) { write(t, dir, "docs/notes.md", "two\n") },
			pins: func(p ReceiptPins) ReceiptPins {
				for i := range p.Images {
					p.Images[i].Inputs = ImageInputs{}
				}
				return p
			},
			web: "example/web:1", sid: "example/side:1",
		},
		"no deployed base tree inherits nothing": {
			edit: func(t *testing.T, dir string) { write(t, dir, "docs/notes.md", "two\n") },
			pins: func(p ReceiptPins) ReceiptPins { p.BaseTree = ""; return p },
			web:  "example/web:1", sid: "example/side:1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir, pins := pinFixture(t)
			tc.edit(t, dir)
			if runGit(t, dir, "status", "--porcelain") != "" {
				commitTree(t, dir)
			}
			if tc.pins != nil {
				pins = tc.pins(pins)
			}
			got := containerImages(t, dir, pins)
			if got["web"] != tc.web || got["side"] != tc.sid {
				t.Fatalf("images = %v, want web=%s side=%s", got, tc.web, tc.sid)
			}
		})
	}
}

// A responder patch (a real patch file in the series) that edits a non-image
// manifest field keeps the pins; one that edits a built image's context does not.
func TestPinsAcrossAnAcceptedPatchSeries(t *testing.T) {
	for name, tc := range map[string]struct {
		path string
		body string
		want string
	}{
		"patch of a manifest field":  {path: "k8s/web.yaml", body: strings.Replace(twoContainerManifest, "namespace: app}", "namespace: app, labels: {tier: x}}", 1), want: inheritedDigest},
		"patch of the build context": {path: "app/web/main.go", body: "package main // fixed\n", want: "example/web:1"},
	} {
		t.Run(name, func(t *testing.T) {
			dir, pins := pinFixture(t)
			text := patchText(t, dir, func() { write(t, dir, tc.path, tc.body) })
			write(t, dir, ".sdo/deployment/patches/0001-fix.patch", text)
			commitTree(t, dir)
			if got := containerImages(t, dir, pins)["web"]; got != tc.want {
				t.Fatalf("web image = %s, want %s", got, tc.want)
			}
		})
	}
}
