package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/layout"
)

var _ lifecycle.CandidateVerifier = IndependentHealthVerifier{}

type recordingProbe struct {
	status string
	roots  *[]string
}

func (p recordingProbe) Evaluate(_ context.Context, root, _, _ string) (HealthEvaluation, error) {
	*p.roots = append(*p.roots, root)
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
