package install

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

func seedEvidenceFixture(t *testing.T) (string, string, string, string, []byte) {
	t.Helper()
	repo, root := t.TempDir(), t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: api}\nspec:\n  template:\n    spec:\n      containers:\n      - {name: api, image: example/api:latest}\n"
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "synthetic trusted deployment")
	revision, tree := git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
	digest := "example/api@sha256:" + strings.Repeat("a", 64)
	proof := broker.SourceEvidence{AcceptedCommit: revision, ProposalTreeDigest: tree, ObservedRevision: tree, StrategyID: "raw-manifests", Ready: true, Images: []string{digest}, ResourceIDs: []string{"Deployment/app/api"}, ManifestPaths: []string{"app.yaml"}, ContainerImages: []rendered.DeployedImage{{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, Name: "api", Applied: digest, Digest: digest, Inputs: rendered.ImageInputs{SourceImage: "example/api:latest"}}}}
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("b", 64) + ".json"
	if err := os.Mkdir(filepath.Join(root, "source-evidence"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source-evidence", name), data, 0600); err != nil {
		t.Fatal(err)
	}
	return repo, root, revision, name, data
}

func TestSeederPreservesOnlyTrustedDeploymentEvidence(t *testing.T) {
	repo, root, revision, name, expected := seedEvidenceFixture(t)
	if err := os.WriteFile(filepath.Join(root, "unrelated-private-state"), []byte("must not copy"), 0600); err != nil {
		t.Fatal(err)
	}
	bin, output := t.TempDir(), filepath.Join(t.TempDir(), "copied.tar")
	script := "#!/bin/sh\ncase \"$*\" in\n*'tar -C /workspace/worktrees -xf -'*) cat > \"$SEED_TEST_ARCHIVE\";;\n*'tar -C /workspace/application -xf -'*) cat > /dev/null;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SEED_TEST_ARCHIVE", output)
	seed := KubectlTarSeederWithBrokerEvidence(repo, revision, "", root)
	if err := seed(context.Background(), Config{Namespace: "app", ControlNamespace: "app-sdo", AcceptedCommit: revision}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("accepted checkout copied without its deployment ledger: %v", err)
	}
	archive := tar.NewReader(bytes.NewReader(data))
	header, err := archive.Next()
	if err != nil || header.Name != "source-evidence/"+name || header.Mode != 0600 {
		t.Fatalf("wrong ledger archive entry: %+v %v", header, err)
	}
	actual, err := io.ReadAll(archive)
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatalf("trusted deployment evidence changed: %v", err)
	}
	if _, err := archive.Next(); err != io.EOF {
		t.Fatalf("archive contains unrelated private state: %v", err)
	}
}

func TestSeedEvidenceRejectsUnavailableOrUnboundAuthority(t *testing.T) {
	for _, mutation := range []string{"unaccepted", "other-tree", "unknown-image", "symlink", "repository-input"} {
		t.Run(mutation, func(t *testing.T) {
			repo, root, revision, name, data := seedEvidenceFixture(t)
			path := filepath.Join(root, "source-evidence", name)
			var proof broker.SourceEvidence
			if err := json.Unmarshal(data, &proof); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "unaccepted":
				proof.AcceptedCommit = ""
			case "other-tree":
				proof.ProposalTreeDigest = strings.Repeat("c", 40)
				proof.ObservedRevision = proof.ProposalTreeDigest
			case "unknown-image":
				proof.ContainerImages[0].Inputs.SourceImage = "example/other:latest"
			case "repository-input":
				root = repo
			}
			encoded, err := json.Marshal(proof)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			if mutation == "symlink" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(repo, "app.yaml"), path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := seedBrokerArchive(context.Background(), repo, revision, root); err == nil {
				t.Fatal("seed accepted missing or unbound deployment authority")
			}
		})
	}
}
