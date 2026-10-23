package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
)

// Reduced S3D: a manifest-only SOURCE acceptance keeps both image identities;
// changing one service's build inputs gaps only that workload, including init images.
func TestRenderedLockImageInputScope(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		gaps             int
		unknown, root    bool
	}{
		{name: "manifest-only", path: "kubernetes/verification-policy.yaml", body: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: verification-policy}\ndata: {verified: 'true'}\n"},
		{name: "one-service", path: "services/consul/main.go", body: "changed", gaps: 1},
		{name: "external-dockerfile", path: "docker/consul.Dockerfile", body: "FROM scratch\n# changed", gaps: 1},
		{name: "sibling-directory", path: "services/consul-other/main.go", body: "changed"},
		{name: "unknown-context", path: "README.md", body: "changed", unknown: true, gaps: 2},
		{name: "root-context-includes-manifest", path: "kubernetes/verification-policy.yaml", body: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: verification-policy}\n", root: true, gaps: 1},
		{name: "root-context-excludes-manifest", path: "kubernetes/verification-policy.yaml", body: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: verification-policy}\n", root: true},
		{name: "init-context", path: "init/setup.sh", body: "changed", gaps: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			source, _, _, live, service := acceptedSourceDeployment(t, true)
			write := func(path, body string) {
				t.Helper()
				p := filepath.Join(source.Repository, path)
				if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, p := range []string{"services/consul/main.go", "services/other/main.go", "docker/consul.Dockerfile", "docker/other.Dockerfile", "init/setup.sh", "init/Dockerfile"} {
				write(p, "original\n")
			}
			write("Dockerfile", "FROM scratch\nCOPY . /app\n")
			if tc.name == "root-context-excludes-manifest" {
				write(".dockerignore", "kubernetes\n.sdo\n")
			}
			raw, err := os.ReadFile(filepath.Join(source.Repository, "consul.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			write("other.yaml", strings.ReplaceAll(string(raw), "consul", "other"))
			source.ManifestPaths = append(source.ManifestPaths, "other.yaml", "kubernetes")
			write("kubernetes/base.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n")
			nativeGit(t, source.Repository, "add", ".")
			nativeGit(t, source.Repository, "commit", "-qm", "synthetic build declarations")
			authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := authority.CreateProposalForPhase(ctx, "scoped-deploy", broker.InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(proposal.Path, "deployment.txt"), []byte("fresh trusted deployment"), 0600); err != nil {
				t.Fatal(err)
			}
			tree, err := broker.ProposalTreeDigest(ctx, proposal.Path)
			if err != nil {
				t.Fatal(err)
			}
			other := live.DeepCopy()
			other.Name = "other"
			other.Spec.Template.Spec.Containers[0].Name = "other"
			other.Spec.Template.Spec.Containers[0].Image = "other@sha256:" + strings.Repeat("c", 64)
			proof := broker.SourceEvidence{ProposalTreeDigest: tree, ObservedRevision: tree, StrategyID: "raw-manifests", ManifestPaths: source.ManifestPaths, Ready: true}
			for _, d := range []*appsv1.Deployment{live, other} {
				d.Annotations["sdo.dev/source-tree"] = tree
				d.Spec.Template.Annotations["sdo.dev/source-tree"] = tree
				replicas := int32(1)
				d.Spec.Replicas = &replicas
				d.Generation = 1
				d.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
				proof.ResourceIDs = append(proof.ResourceIDs, "Deployment/app/"+d.Name)
				for _, init := range []bool{false, true} {
					cs := d.Spec.Template.Spec.Containers
					if init {
						cs = d.Spec.Template.Spec.InitContainers
					}
					for _, c := range cs {
						contextPath, dockerfile, sourceImage := "services/"+d.Name, "docker/"+d.Name+".Dockerfile", d.Name+":1.15"
						if init {
							contextPath, dockerfile, sourceImage = "init", "init/Dockerfile", "example/setup:latest"
						}
						if tc.root && d.Name == "consul" && !init {
							contextPath, dockerfile = ".", "Dockerfile"
						}
						image := map[string]any{"ref": map[string]string{"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": d.Name}, "name": c.Name, "init": init, "applied": c.Image, "digest": c.Image}
						// Decode the proposed evidence extension so the failing-first test also
						// builds against the previous implementation (which ignores the inputs).
						if !tc.unknown {
							image["inputs"] = map[string]string{"source_image": sourceImage, "context_path": contextPath, "dockerfile_path": dockerfile}
						}
						encoded, _ := json.Marshal(image)
						var named rendered.DeployedImage
						if err := json.Unmarshal(encoded, &named); err != nil {
							t.Fatal(err)
						}
						// ObjectRef's wire spelling is owned by the SDK.
						named.Ref.APIVersion = "apps/v1"
						proof.ContainerImages = append(proof.ContainerImages, named)
						proof.Images = append(proof.Images, c.Image)
					}
				}
			}
			receipt, err := authority.PublishVerifiedSource(ctx, "scoped-deploy", proof)
			if err != nil {
				t.Fatal(err)
			}
			source.DeployedCommit = receipt.Commit
			source.Images = nil
			client := fake.NewSimpleClientset(live, other, service)
			reconciler := RenderedLockReconciler{Source: source, Client: client, Broker: authority}
			if _, err := reconciler.Reconcile(ctx, receipt.Commit, "app"); err != nil {
				t.Fatal(err)
			}
			write(tc.path, tc.body)
			nativeGit(t, source.Repository, "add", ".")
			nativeGit(t, source.Repository, "commit", "-qm", "accepted source repair")
			changed := nativeGit(t, source.Repository, "rev-parse", "HEAD")
			check := func(revision string, want int) {
				t.Helper()
				locked, err := reconciler.Reconcile(ctx, revision, "app")
				if err != nil {
					t.Fatal(err)
				}
				data := nativeGit(t, source.Repository, "show", locked+":"+rendered.LockPath)
				var lock rendered.Lock
				if err := json.Unmarshal([]byte(data), &lock); err != nil {
					t.Fatal(err)
				}
				gaps := 0
				for _, o := range lock.Objects {
					if o.Error != "" {
						gaps++
						if o.File != "" || len(o.Images) > 0 {
							t.Fatal("gap retained image authority")
						}
					}
				}
				if gaps != want {
					t.Fatalf("gaps=%d want %d: %s", gaps, want, data)
				}
			}
			check(changed, tc.gaps)
			if tc.name == "one-service" {
				// A forged annotation and a subsequent source revert cannot revive old evidence.
				live.Spec.Template.Annotations["sdo.dev/source-tree"] = nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
				if _, err := client.AppsV1().Deployments("app").Update(ctx, live, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				write(tc.path, "original\n")
				nativeGit(t, source.Repository, "add", ".")
				nativeGit(t, source.Repository, "commit", "-qm", "revert source without deployment")
				check(nativeGit(t, source.Repository, "rev-parse", "HEAD"), 1)

				// Only a new verified deployment may restore coverage. Its
				// fresh digest also supersedes still-valid older peer evidence.
				fresh, err := authority.CreateProposalForPhase(ctx, "fresh-scoped-deploy", broker.InitialPhase)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(fresh.Path, "deployment.txt"), []byte("new trusted deployment"), 0600); err != nil {
					t.Fatal(err)
				}
				freshTree, err := broker.ProposalTreeDigest(ctx, fresh.Path)
				if err != nil {
					t.Fatal(err)
				}
				proof.ProposalTreeDigest = freshTree
				proof.ObservedRevision = freshTree
				proof.Images = nil
				for i := range proof.ContainerImages {
					image := &proof.ContainerImages[i]
					if image.Ref.Name == "other" && !image.Init {
						image.Applied = "other@sha256:" + strings.Repeat("d", 64)
						image.Digest = image.Applied
						other.Spec.Template.Spec.Containers[0].Image = image.Applied
					}
					proof.Images = append(proof.Images, image.Digest)
				}
				accepted, err := authority.PublishVerifiedSource(ctx, "fresh-scoped-deploy", proof)
				if err != nil {
					t.Fatal(err)
				}
				for _, d := range []*appsv1.Deployment{live, other} {
					d.Annotations["sdo.dev/source-tree"] = freshTree
					d.Spec.Template.Annotations["sdo.dev/source-tree"] = freshTree
					if _, err := client.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				check(accepted.Commit, 0)
			}
		})
	}
}
