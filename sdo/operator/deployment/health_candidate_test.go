package deployment

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

var _ lifecycle.CandidateVerifier = IndependentHealthVerifier{}

type recordingProbe struct {
	status string
	roots  *[]string
	scopes *[]HealthScope
}

func (p recordingProbe) Evaluate(_ context.Context, root, _, _ string, scope HealthScope) (HealthEvaluation, error) {
	*p.roots = append(*p.roots, root)
	if p.scopes != nil {
		*p.scopes = append(*p.scopes, scope)
	}
	return HealthEvaluation{Status: p.status}, nil
}

// candidateFixture is an accepted repository with synthetic detectors and a
// proposal-style clone whose HEAD is a candidate commit the accepted
// repository does not have, deployed by the Deployer from the clone.
func candidateFixture(t *testing.T) (accepted, candidate, commit string, deployer *Deployer, receipt lifecycle.DeploymentReceipt) {
	t.Helper()
	accepted, source := depRawFixture(t)
	writeFile(t, accepted, filepath.Join(layout.DiagnosticsDir, "synthetic", "detector.go"), "package synthetic\n")
	testutil.Git(t, accepted, "add", ".")
	testutil.Git(t, accepted, "commit", "-qm", "synthetic detectors")
	candidate = filepath.Join(t.TempDir(), "proposal")
	testutil.Git(t, accepted, "clone", "-q", accepted, candidate)
	writeFile(t, candidate, "k8s/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\ndata: {greeting: upstream}\n")
	// A clone does not inherit the fixture identity: set it for this commit rather than rely on the host.
	testutil.Git(t, candidate, "-c", "user.name=Synthetic", "-c", "user.email=synthetic@example.invalid", "commit", "-qam", "synthetic upstream change")
	commit = testutil.Git(t, candidate, "rev-parse", "HEAD")
	d, _ := depDeployer(t, accepted, &clusterFake{})
	receipt, err := d.ApplyAndVerify(context.Background(), candidate, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	return accepted, candidate, commit, &d, receipt
}

func writeFile(t *testing.T, root, path, body string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyCandidateJudgesAProposalCheckoutTheAcceptedRepositoryLacks(t *testing.T) {
	accepted, candidate, commit, deployer, receipt := candidateFixture(t)
	var roots []string
	verifier := IndependentHealthVerifier{AcceptedRoot: accepted, OperatorRoot: t.TempDir(), Readiness: deployer, Probe: recordingProbe{status: "clear", roots: &roots}}
	ctx := context.Background()

	verdict, err := verifier.VerifyCandidate(ctx, candidate, commit, depNamespace, "serve", receipt)
	if err != nil || verdict.Outcome != lifecycle.VerificationClear {
		t.Fatalf("verdict = %+v, err = %v", verdict, err)
	}
	if len(roots) != 1 || roots[0] != candidate {
		t.Fatalf("probe evaluated %v, want the candidate checkout's detectors", roots)
	}
	if deployer.AcceptedRoot != accepted {
		t.Fatalf("verifier rebound the shared deployer to %q", deployer.AcceptedRoot)
	}

	// Negative control: the accepted-root Verify cannot judge the candidate.
	if _, err := verifier.Verify(ctx, commit, depNamespace, "serve", receipt); err == nil {
		t.Fatal("Verify judged a commit that is not the accepted HEAD")
	}
	if len(roots) != 1 {
		t.Fatal("a rejected verification still evaluated detectors")
	}
}

func TestVerifyCandidateRefusesWhatItCannotBind(t *testing.T) {
	ctx := context.Background()
	for name, mutate := range map[string]func(t *testing.T, candidate string, commit *string, receipt *lifecycle.DeploymentReceipt){
		"commit is not HEAD": func(t *testing.T, candidate string, commit *string, _ *lifecycle.DeploymentReceipt) {
			*commit = testutil.Git(t, candidate, "rev-parse", "HEAD^")
		},
		"dirty checkout": func(t *testing.T, candidate string, _ *string, _ *lifecycle.DeploymentReceipt) {
			writeFile(t, candidate, "untracked.txt", "x")
		},
		"deployed tree not in history": func(_ *testing.T, _ string, _ *string, receipt *lifecycle.DeploymentReceipt) {
			receipt.BaseTree = strings.Repeat("b", 40)
			receipt.ObservedRevision = receipt.BaseTree
		},
		"live workloads differ": func(_ *testing.T, _ string, _ *string, receipt *lifecycle.DeploymentReceipt) {
			receipt.Images = []string{"other@sha256:" + strings.Repeat("a", 64)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			accepted, candidate, commit, deployer, receipt := candidateFixture(t)
			mutate(t, candidate, &commit, &receipt)
			var roots []string
			verifier := IndependentHealthVerifier{AcceptedRoot: accepted, OperatorRoot: t.TempDir(), Readiness: deployer, Probe: recordingProbe{status: "clear", roots: &roots}}
			if verdict, err := verifier.VerifyCandidate(ctx, candidate, commit, depNamespace, "serve", receipt); err == nil {
				t.Fatalf("verdict = %+v, want an error", verdict)
			}
			if len(roots) != 0 {
				t.Fatal("detectors evaluated before the candidate was bound")
			}
		})
	}
}

func TestVerifyCandidateReportsAFiringCandidateAsASourceFault(t *testing.T) {
	accepted, candidate, commit, deployer, receipt := candidateFixture(t)
	var roots []string
	verifier := IndependentHealthVerifier{AcceptedRoot: accepted, OperatorRoot: t.TempDir(), Readiness: deployer, Probe: recordingProbe{status: "firing", roots: &roots}}
	verdict, err := verifier.VerifyCandidate(context.Background(), candidate, commit, depNamespace, "serve", receipt)
	if err != nil || verdict.Outcome != lifecycle.VerificationSourceFault {
		t.Fatalf("verdict = %+v, err = %v", verdict, err)
	}
}

// An advance is judged by the gating detector set (the health objective and the
// designer's health detectors, never learned incident detectors), against the
// candidate's own deployment: the probe is told the classes and receives the
// images the candidate applied as pins.
func TestVerifyCandidateScopesTheProbeToGatingClassesAndTheCandidatesImages(t *testing.T) {
	accepted, candidate, commit, deployer, receipt := candidateFixture(t)
	receipt.ContainerImages = []rendered.DeployedImage{{Name: "web", Applied: "example.invalid/web@sha256:" + strings.Repeat("a", 64)}}
	var roots []string
	var scopes []HealthScope
	verifier := IndependentHealthVerifier{AcceptedRoot: accepted, OperatorRoot: t.TempDir(), Readiness: deployer, Probe: recordingProbe{status: "clear", roots: &roots, scopes: &scopes}}
	if _, err := verifier.VerifyCandidate(context.Background(), candidate, commit, depNamespace, "serve", receipt); err != nil {
		t.Fatal(err)
	}
	if len(scopes) != 1 {
		t.Fatalf("probe calls = %d", len(scopes))
	}
	got := scopes[0]
	if !slices.Equal(got.Classes, []sdk.DetectorClass{sdk.DetectorClassHealth}) {
		t.Fatalf("gating classes = %v, want health only (incident detectors do not gate an advance)", got.Classes)
	}
	if got.Pins.EffectiveTree == "" || got.Pins.EffectiveTree != receipt.EffectiveTree || got.Pins.BaseTree != receipt.BaseTree || len(got.Pins.Images) != 1 || got.Pins.Images[0].Applied != receipt.ContainerImages[0].Applied {
		t.Fatalf("pins = %+v, want the candidate receipt's images", got.Pins)
	}
}

func TestAcceptedDetectorProbePassesTheScopeToEvaluateOnce(t *testing.T) {
	args := AcceptedDetectorProbe{}.evaluationArgs("/root", "ns", HealthScope{Classes: evaluation.GatingClasses}, "/tmp/pins.json")
	for _, want := range [][]string{{"--classes", "health"}, {"--image-pins", "/tmp/pins.json"}} {
		if i := slices.Index(args, want[0]); i < 0 || args[i+1] != want[1] {
			t.Fatalf("args %v lack %v", args, want)
		}
	}
	if args := (AcceptedDetectorProbe{}).evaluationArgs("/root", "ns", HealthScope{}, ""); slices.Contains(args, "--classes") || slices.Contains(args, "--image-pins") {
		t.Fatalf("unscoped args = %v", args)
	}
}
