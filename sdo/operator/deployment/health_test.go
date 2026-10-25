package deployment

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
)

type fakeHealthProbe struct{ status string }

func (probe fakeHealthProbe) Evaluate(context.Context, string, string, string) (string, error) {
	return probe.status, nil
}

func gitDeployment(t *testing.T, root string, args ...string) string {
	t.Helper()
	output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestIndependentHealthVerifierBindsAcceptedDetectorsAndDeployedSource(t *testing.T) {
	ctx := context.Background()
	root := candidateRepository(t)
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("deployed"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitDeployment(t, root, "add", "-A")
	gitDeployment(t, root, "commit", "-qm", "source")
	sourceTree := gitDeployment(t, root, "rev-parse", "HEAD^{tree}")
	if err := os.MkdirAll(filepath.Join(root, ".sdo", "diagnostics"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml"), []byte("health: ready\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitDeployment(t, root, "add", "-A")
	gitDeployment(t, root, "commit", "-qm", "judge")
	judgeCommit := gitDeployment(t, root, "rev-parse", "HEAD")
	image := "registry.example/app@sha256:" + strings.Repeat("a", 64)
	source := lifecycle.DeploymentReceipt{ProposalTreeDigest: sourceTree, ObservedRevision: sourceTree, StrategyID: "raw-manifests", Images: []string{image}, ResourceIDs: []string{"Deployment/default/app"}, Ready: true}
	registry, err := NewRegistry(Registration{ID: "raw-manifests", Config: &contracts.RawManifestStrategy{}, Apply: func(context.Context, string, string, proto.Message) (Observation, error) { return Observation{}, nil }, Verify: func(_ context.Context, _, _ string, prior lifecycle.DeploymentReceipt) (Observation, error) {
		return Observation{Revision: prior.ProposalTreeDigest, Images: prior.Images, ResourceIDs: prior.ResourceIDs, Ready: true}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	registry.AcceptedRoot = root
	verifier := IndependentHealthVerifier{AcceptedRoot: root, OperatorRoot: root, Readiness: registry, Probe: fakeHealthProbe{status: "clear"}}
	healthy, feedback, err := verifier.Verify(ctx, judgeCommit, "default", "application is healthy", source)
	if err != nil || !healthy || feedback != "" {
		t.Fatalf("healthy accepted revision rejected: %v %q %v", healthy, feedback, err)
	}
	verifier.Probe = fakeHealthProbe{status: "firing"}
	healthy, feedback, err = verifier.Verify(ctx, judgeCommit, "default", "application is healthy", source)
	if err != nil || healthy || !strings.Contains(feedback, "firing") {
		t.Fatalf("firing detector accepted: %v %q %v", healthy, feedback, err)
	}
	source.ProposalTreeDigest = strings.Repeat("b", 40)
	if _, _, err := verifier.Verify(ctx, judgeCommit, "default", "application is healthy", source); err == nil {
		t.Fatal("unaccepted source evidence passed")
	}
}

func TestHealthVerdictPendingIsKnownWithoutEstablishingHealth(t *testing.T) {
	status, err := decodeHealthVerdict([]byte(`{"status":"pending","findings":[]}`), nil)
	if err != nil || status != "pending" {
		t.Fatalf("pending verdict: %s %v", status, err)
	}
}
