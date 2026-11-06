//go:build integration

package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

const pinnedDigest = "example/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// The controller's drift admission (declaredSourceDiff) compares the live Deployment with desired state, and desired state is
// the accepted source plus the accepted deployment's image pins: a workload
// running the digest the deployer applied is not drift, a different digest or
// a manual tag edit still is, a source image change that was never deployed
// has no pins (desired is the source image), and a pin ledger failure is an
// error, never a silent fall back to the tag.
func TestDriftTreatsTheDeployedImagePinAsDesiredState(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web, namespace: app}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec:\n      containers:\n      - {name: web, image: \"example/web:1\"}\n"
	if err := os.WriteFile(filepath.Join(root, "k8s", "web.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"app/Dockerfile": "FROM scratch\n", "app/main.go": "package main\n", "docs/notes.md": "one\n"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "seed"}} {
		if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD", "HEAD^{tree}").Output()
	if err != nil {
		t.Fatal(err)
	}
	ids := strings.Fields(string(out))
	commit, tree := ids[0], ids[1]
	git := func(args ...string) string {
		out, err := exec.Command("git", append([]string{"-C", root, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	catalog := render.NewCatalog()
	if err := raw.Register(catalog); err != nil {
		t.Fatal(err)
	}
	deployment, err := rendered.LegacyRawSource([]string{"k8s"})
	if err != nil {
		t.Fatal(err)
	}
	web := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	pins := rendered.ReceiptPins{EffectiveTree: tree, BaseTree: tree, Images: []rendered.DeployedImage{{Ref: web, Name: "web", Applied: pinnedDigest, Digest: pinnedDigest,
		Inputs: rendered.ImageInputs{SourceImage: "example/web:1", ContextPath: "app", DockerfilePath: "app/Dockerfile"}}}}

	at := commit
	changed := func(provider rendered.PinProvider, liveImage string) (bool, error) {
		live := appsv1.Deployment{
			TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: liveImage}}}},
			},
		}
		source := rendered.Source{Repository: root, Deployment: deployment, Catalog: catalog, Pins: provider}.WithCache()
		view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{live}}), Context: context.Background(), Source: source, SourceCommit: at}
		diff, err := declaredSourceDiff(view, web)
		return diff.Changed, err
	}
	for name, tc := range map[string]struct {
		pins    rendered.PinProvider
		live    string
		drifted bool
	}{
		"deployed digest equals the pin":                {pins, pinnedDigest, false},
		"another digest than the pin":                   {pins, "example/web@sha256:" + strings.Repeat("f", 64), true},
		"manual tag edit of a pinned workload":          {pins, "example/web:2", true},
		"the source tag while the pin says digest":      {pins, "example/web:1", true},
		"pins of another effective tree keep the tag":   {rendered.ReceiptPins{EffectiveTree: strings.Repeat("a", 40), Images: pins.Images}, "example/web:1", false},
		"without pins a digest-running workload drifts": {nil, pinnedDigest, true},
	} {
		got, err := changed(tc.pins, tc.live)
		if err != nil || got != tc.drifted {
			t.Errorf("%s: changed = %v (%v), want %v", name, got, err, tc.drifted)
		}
	}
	// The accepted tree moves without a deployment (a responder patch, a values
	// or manifest edit): the deployed digest stays desired while the image's
	// build inputs are unchanged, and a manual tag edit still drifts.
	if err := os.WriteFile(filepath.Join(root, "docs", "notes.md"), []byte("two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	git("commit", "-q", "-m", "docs only")
	at = git("rev-parse", "HEAD")
	for name, tc := range map[string]struct {
		live    string
		drifted bool
	}{
		"after a change no image reads, the deployed digest": {pinnedDigest, false},
		"after a change no image reads, a manual tag edit":   {"example/web:2", true},
		"after a change no image reads, the source tag":      {"example/web:1", true},
		"after a change no image reads, another digest":      {"example/web@sha256:" + strings.Repeat("f", 64), true},
	} {
		got, err := changed(pins, tc.live)
		if err != nil || got != tc.drifted {
			t.Errorf("%s: changed = %v (%v), want %v", name, got, err, tc.drifted)
		}
	}
	// A change to the build context needs a deployment that records new pins:
	// until then the pin is not desired, so the old digest reads as drift.
	if err := os.WriteFile(filepath.Join(root, "app", "main.go"), []byte("package main // patched\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	git("commit", "-q", "-m", "patch the app")
	at = git("rev-parse", "HEAD")
	if got, err := changed(pins, pinnedDigest); err != nil || !got {
		t.Errorf("after a build-input change the stale digest must not be desired: changed = %v (%v)", got, err)
	}
	if got, err := changed(pins, "example/web:1"); err != nil || got {
		t.Errorf("after a build-input change desired is the source image: changed = %v (%v)", got, err)
	}
	if _, err := changed(failingPinProvider{}, pinnedDigest); err == nil {
		t.Error("an unreadable pin ledger must fail the comparison, not fall back to the tag")
	}
}

type failingPinProvider struct{}

func (failingPinProvider) AcceptedImagePins(context.Context) (rendered.ReceiptPins, error) {
	return rendered.ReceiptPins{}, os.ErrPermission
}
