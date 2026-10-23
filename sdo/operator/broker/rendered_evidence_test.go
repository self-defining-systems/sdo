package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

type deployedImageStore struct {
	proof   rendered.DeploymentEvidence
	digests bool
}

func (s deployedImageStore) BaselineTree(sdk.ObjectRef) string { return s.proof.SourceTree }
func (s deployedImageStore) BaselineImage(ref sdk.ObjectRef, name string, init bool) (string, bool) {
	image, ok := s.proof.Image(ref, name, init)
	if s.digests {
		return image.Digest, ok
	}
	return image.Applied, ok
}

// Uses real source acceptance to establish the authority independently of the
// later submitted lock. A forged snapshot cannot manufacture its own evidence.
func TestRenderedLockRequiresDurableNamedDeploymentEvidence(t *testing.T) {
	for _, mode := range []string{"honest-restart", "missing-ledger", "legacy-flat", "wrong-workload", "wrong-name", "wrong-role", "changed-digest", "changed-applied"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			b, source, _, _ := rv4PinnedSnapshot(t)
			proposal, err := b.CreateProposalForPhase(ctx, "trusted-deploy", InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(proposal.Path, "app.yaml")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			body = []byte(strings.ReplaceAll(strings.ReplaceAll(string(body), "example/api@sha256:"+strings.Repeat("b", 64), "example/api:latest"), "example/init@sha256:"+strings.Repeat("a", 64), "example/init:latest"))
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			tree, err := ProposalTreeDigest(ctx, proposal.Path)
			if err != nil {
				t.Fatal(err)
			}
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			proof := rendered.DeploymentEvidence{SourceTree: tree, Images: []rendered.DeployedImage{
				{Ref: ref, Name: "init", Init: true, Applied: "example/init:latest", Digest: "example/init@sha256:" + strings.Repeat("a", 64)},
				{Ref: ref, Name: "api", Applied: "example/api:latest", Digest: "example/api@sha256:" + strings.Repeat("b", 64)},
			}}
			receipt, err := b.PublishVerifiedSource(ctx, "trusted-deploy", SourceEvidence{ProposalTreeDigest: tree, ObservedRevision: tree, StrategyID: "raw-manifests", Images: []string{proof.Images[0].Digest, proof.Images[1].Digest}, ContainerImages: proof.Images, ResourceIDs: []string{"Deployment/app/api"}, ManifestPaths: source.ManifestPaths, Ready: true})
			if err != nil {
				t.Fatal(err)
			}
			// Reconstruct the broker; no volatile reconciler state authorizes images.
			if mode == "legacy-flat" {
				value, ok, err := b.SourceEvidence(ctx, "trusted-deploy")
				if err != nil || !ok {
					t.Fatal(err)
				}
				value.ContainerImages = nil
				if err := b.writeSourceEvidence("trusted-deploy", value); err != nil {
					t.Fatal(err)
				}
			}
			workspace := b.workspaceRoot
			if mode == "missing-ledger" {
				workspace = t.TempDir()
			}
			b, err = New(source.Repository, workspace, func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "wrong-workload":
				proof.Images[1].Ref.Name = "other"
			case "wrong-name":
				proof.Images[1].Name = "other"
			case "wrong-role":
				proof.Images[1].Init = true
			case "changed-digest":
				proof.Images[1].Digest = "other/image@sha256:" + strings.Repeat("c", 64)
			case "changed-applied":
				proof.Images[1].Applied = "other/image:incident"
			}
			source.Images = deployedImageStore{proof: proof}
			source.Digests = deployedImageStore{proof: proof, digests: true}
			snapshot, err := source.BuildLock(ctx, receipt.Commit, "app")
			if err != nil {
				t.Fatal(err)
			}
			// For cross-key mismatches, force a forged non-gap snapshot by starting
			// with the original valid image mapping; the unauthorized key itself must
			// never become usable evidence in the private ledger.
			if mode == "wrong-workload" || mode == "wrong-name" || mode == "wrong-role" {
				value, ok, err := b.SourceEvidence(ctx, "trusted-deploy")
				if err != nil || !ok {
					t.Fatal(err)
				}
				value.ContainerImages = proof.Images
				if mode == "wrong-workload" {
					value.ResourceIDs = append(value.ResourceIDs, "Deployment/app/other")
				}
				if err := b.writeSourceEvidence("trusted-deploy", value); err != nil {
					t.Fatal(err)
				}
				proof.Images[1] = rendered.DeployedImage{Ref: ref, Name: "api", Applied: "example/api:latest", Digest: "example/api@sha256:" + strings.Repeat("b", 64)}
				source.Images = deployedImageStore{proof: proof}
				source.Digests = deployedImageStore{proof: proof, digests: true}
				snapshot, err = source.BuildLock(ctx, receipt.Commit, "app")
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err = b.RecordRenderedLock(ctx, receipt.Commit, snapshot)
			if mode == "honest-restart" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("unproven image accepted")
			}
		})
	}
}
