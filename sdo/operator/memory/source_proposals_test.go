package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const proposalManifest = `apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  ports:
    - name: http
      port: 80
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: web
          image: registry.example/web:1
`

func TestApplyProposalSetsTheFieldOfTheNamedDocument(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "web.yaml"), []byte(proposalManifest), 0o644); err != nil {
		t.Fatal(err)
	}
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "web"}
	for _, proposal := range []program.SourceChangeProposal{
		{Ref: deployment, Path: "spec.replicas", Value: float64(3)},
		{Ref: deployment, Path: "spec.template.spec.containers[name=web].resources.limits.memory", Value: "512Mi"},
	} {
		if why := applyProposal(dir, "web.yaml", proposal); why != "" {
			t.Fatalf("%s: %s", proposal.Path, why)
		}
	}
	data, _ := os.ReadFile(filepath.Join(dir, "web.yaml"))
	text := string(data)
	for _, want := range []string{"replicas: 3", "memory: 512Mi", "port: 80", "kind: Service"} {
		if !strings.Contains(text, want) {
			t.Fatalf("manifest lacks %q:\n%s", want, text)
		}
	}
	if why := applyProposal(dir, "web.yaml", program.SourceChangeProposal{Ref: deployment, Path: "spec.template.spec.containers[name=absent].image", Value: "x"}); why == "" {
		t.Fatal("a path through an absent list member applied")
	}
	if why := applyProposal(dir, "web.yaml", program.SourceChangeProposal{Ref: sdk.ObjectRef{Kind: "Deployment", Name: "other"}, Path: "spec.replicas", Value: 2}); why == "" {
		t.Fatal("a proposal for an object the file does not declare applied")
	}
	if file, why := proposalFile("- Deployment/shop/web (source: helm)\n", program.SourceChangeProposal{Ref: deployment}); file != "" || why == "" {
		t.Fatalf("a rendered chart source resolved to %q", file)
	}
	if file, _ := proposalFile("- Deployment/shop/web (source: k8s/web.yaml)\n", program.SourceChangeProposal{Ref: deployment}); file != "k8s/web.yaml" {
		t.Fatalf("source file = %q", file)
	}
}
