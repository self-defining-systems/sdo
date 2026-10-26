package install

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

type namedImages map[string]string

func (n namedImages) BaselineImage(ref sdk.ObjectRef, name string, init bool) (string, bool) {
	value, ok := n[ref.Name+"/"+name]
	return value, ok
}
func (namedImages) BaselineTree(sdk.ObjectRef) string { return "" }

func adoptedDeployment(name, image string) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: image}}}}}}
}

func adoptedPod(name, owner, image, imageID string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"app": owner}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: owner, Image: image}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: owner, Image: image, ImageID: imageID}}}}
}

// An application SDO adopts (install-existing) uses tag images and was never
// deployed by SDO. Its first lock must still render every workload whose live
// identity is unambiguous, so DiffAgainstSource works for it.
func TestAdoptionSeederGivesTagImageWorkloadsTrustedLockEvidence(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	manifest := ""
	for _, name := range []string{"geo", "mongodb-geo", "rolling", "drifted", "mirrored", "pending"} {
		manifest += "---\napiVersion: apps/v1\nkind: Deployment\nmetadata: {name: " + name + "}\nspec:\n  selector: {matchLabels: {app: " + name + "}}\n  template:\n    metadata: {labels: {app: " + name + "}}\n    spec:\n      containers:\n      - {name: " + name + ", image: example/" + name + ":latest}\n"
	}
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "adopted application")
	revision := git("rev-parse", "HEAD")

	digest := func(name, fill string) string {
		return "docker.io/example/" + name + "@sha256:" + strings.Repeat(fill, 64)
	}
	client := fake.NewSimpleClientset(
		adoptedDeployment("geo", "example/geo:latest"),
		adoptedPod("geo-1", "geo", "example/geo:latest", "docker-pullable://"+digest("geo", "a")),
		adoptedPod("geo-2", "geo", "example/geo:latest", digest("geo", "a")),
		adoptedDeployment("mongodb-geo", "example/mongodb-geo:latest"),
		adoptedPod("mongodb-geo-1", "mongodb-geo", "example/mongodb-geo:latest", digest("mongodb-geo", "b")),
		// Two Pods disagree on what the tag pulled: no single trusted identity.
		adoptedDeployment("rolling", "example/rolling:latest"),
		adoptedPod("rolling-1", "rolling", "example/rolling:latest", digest("rolling", "c")),
		adoptedPod("rolling-2", "rolling", "example/rolling:latest", digest("rolling", "d")),
		// The live template no longer applies the accepted source image.
		adoptedDeployment("drifted", "example/drifted:v2"),
		adoptedPod("drifted-1", "drifted", "example/drifted:v2", digest("drifted", "e")),
		// The runtime names the content under another repository (a mirror);
		// pinning that digest would point nodes at a registry the source never named.
		adoptedDeployment("mirrored", "example/mirrored:latest"),
		adoptedPod("mirrored-1", "mirrored", "example/mirrored:latest", "registry.local/mirror/mirrored@sha256:"+strings.Repeat("f", 64)),
		// One Pod on the tag has not pulled yet; it may resolve the tag differently.
		adoptedDeployment("pending", "example/pending:latest"),
		adoptedPod("pending-1", "pending", "example/pending:latest", digest("pending", "1")),
		adoptedPod("pending-2", "pending", "example/pending:latest", ""),
	)

	bin, output := t.TempDir(), filepath.Join(t.TempDir(), "ledger.tar")
	script := "#!/bin/sh\ncase \"$*\" in\n*'tar -C /workspace/worktrees -xf -'*) cat > \"$SEED_TEST_ARCHIVE\";;\n*'tar -C /workspace/application -xf -'*) cat > /dev/null;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SEED_TEST_ARCHIVE", output)
	seed := KubectlTarAdoptionSeeder(repo, revision, "", "", &AdoptionCapture{Client: client, ManifestPaths: []string{"app.yaml"}})
	if err := seed(ctx, Config{Namespace: "app", ControlNamespace: "app-sdo", AcceptedCommit: revision}); err != nil {
		t.Fatal(err)
	}

	// Restore the copied ledger exactly where the controller broker reads it.
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("adopted install copied no deployment ledger: %v", err)
	}
	root := t.TempDir()
	archive := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, _ := io.ReadAll(archive)
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(header.Name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, header.Name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	authority, err := broker.New(repo, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := authority.RenderedDeploymentEvidence(ctx, revision)
	if err != nil {
		t.Fatal(err)
	}

	// Mirror the controller's lock reconciler: named evidence supplies the
	// applied image and the pulled digest separately.
	applied, digests, gaps := namedImages{}, namedImages{}, map[sdk.ObjectRef]string{}
	for _, name := range []string{"geo", "mongodb-geo", "rolling", "drifted", "mirrored", "pending"} {
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
		found := false
		for _, proof := range proofs {
			if image, ok := proof.Image(ref, name, false); ok {
				applied[name+"/"+name], digests[name+"/"+name], found = image.Applied, image.Digest, true
			}
		}
		if !found {
			gaps[ref] = "trusted deployment image evidence missing for container " + name + " (init=false)"
		}
	}
	snapshot, err := rendered.Source{Repository: repo, ManifestPaths: []string{"app.yaml"}, DeployedCommit: revision, Evidence: proofs, Images: applied, Digests: digests, Gaps: gaps}.BuildLock(ctx, revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.ValidateWithEvidence(ctx, repo, revision, proofs); err != nil {
		t.Fatalf("broker would reject the adopted lock: %v", err)
	}
	if err := snapshot.Write(repo); err != nil {
		t.Fatal(err)
	}
	git("add", ".sdo")
	git("commit", "-qm", "rendered lock")
	locked := git("rev-parse", "HEAD")
	errors := map[string]string{}
	for _, object := range snapshot.Lock.Objects {
		errors[object.Ref.Name] = object.Error
	}
	for _, name := range []string{"geo", "mongodb-geo"} {
		if errors[name] != "" {
			t.Fatalf("adopted %s has no trusted lock entry: %s", name, errors[name])
		}
		// DiffAgainstSource reads the accepted object through this lock reader.
		object, err := rendered.Source{Repository: repo, ManifestPaths: []string{"app.yaml"}}.AcceptedManifest(ctx, locked, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name})
		if err != nil {
			t.Fatalf("accepted-source comparison unavailable for adopted %s: %v", name, err)
		}
		containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
		if image := containers[0].(map[string]any)["image"]; image != "example/"+name+":latest" {
			t.Fatalf("lock must keep the applied tag so live comparison stays exact, got %v", image)
		}
	}
	for _, name := range []string{"rolling", "drifted", "mirrored", "pending"} {
		if errors[name] == "" {
			t.Fatalf("ambiguous or drifted %s must stay an explicit gap", name)
		}
	}
}
