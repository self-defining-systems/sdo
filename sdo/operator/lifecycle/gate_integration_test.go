//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/operability"
)

// gateWorld is a real accepted repository and broker with the memory a health
// judge extends: everything but the health detector.
type gateWorld struct {
	accepted string
	broker   *broker.Broker
	good     string
}

func newGateWorld(t *testing.T) *gateWorld {
	t.Helper()
	base := t.TempDir()
	w := &gateWorld{accepted: filepath.Join(base, "accepted"), good: filepath.Join("..", "memory", "validate", "testdata", "good")}
	if err := os.MkdirAll(w.accepted, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, w.accepted)
	copyTree(t, filepath.Join(w.good, ".sdo"), filepath.Join(w.accepted, ".sdo"), func(rel string) bool {
		return !strings.HasPrefix(rel, "diagnostics")
	})
	copyTree(t, filepath.Join(w.good, ".sdo", "playbooks", "base"), filepath.Join(w.accepted, ".sdo", "playbooks", "health-objective"), func(string) bool { return true })
	if err := os.WriteFile(filepath.Join(w.accepted, "app.txt"), []byte("synthetic app\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "seed")
	b, err := broker.New(w.accepted, filepath.Join(base, "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	w.broker = b
	return w
}

func copyTree(t *testing.T, from, to string, keep func(rel string) bool) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		if !keep(rel) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if strings.HasSuffix(rel, ".sh") {
			mode = 0o755
		}
		return os.WriteFile(target, data, mode)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (w *gateWorld) git(t *testing.T, root string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (w *gateWorld) head(t *testing.T) string { return w.git(t, w.accepted, "rev-parse", "HEAD") }

// judgeProposal opens proposal id and writes the health detector into it.
func (w *gateWorld) judgeProposal(t *testing.T, id string) {
	t.Helper()
	p, err := w.broker.CreateProposalForPhase(context.Background(), id, broker.InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	copyTree(t, filepath.Join(w.good, ".sdo", "diagnostics", "detectors", "health"), filepath.Join(p.Path, ".sdo", "diagnostics", "detectors", "health"), func(string) bool { return true })
}

// receipt is the deployment the judge's candidate is verified against (the
// onboarded one): its effective identity and digests are what the broker binds
// the report to at Accept.
func (w *gateWorld) receipt(t *testing.T) DeploymentReceipt {
	t.Helper()
	accepted, found, err := w.broker.AcceptedSourceEvidence(context.Background())
	if err != nil || !found {
		t.Fatalf("accepted deployment evidence: found=%v err=%v", found, err)
	}
	return deploymentFromBroker(accepted.Evidence)
}

type scriptedReadiness struct{ SourceReadinessVerifier }

func (scriptedReadiness) CheckLive(context.Context, string, *contracts.DeploymentPlan, DeploymentReceipt, error) ([]operability.Check, error) {
	return []operability.Check{
		{ID: "O1.deploy", Phase: operability.PhaseLive, Status: operability.StatusPass},
		{ID: "O2.stamps", Phase: operability.PhaseLive, Status: operability.StatusPass},
		{ID: "O6.budget", Phase: operability.PhaseLive, Status: operability.StatusPass},
	}, nil
}

// scriptedVerifier answers VerifyCandidate from a list and runs a hook first,
// recording what it was handed: the clean staged checkout.
type scriptedVerifier struct {
	HealthVerifier
	verdicts []HealthVerdict
	onVerify func(call int, root, commit string)
	roots    []string
	heads    []string
}

func (v *scriptedVerifier) VerifyCandidate(_ context.Context, root, commit, _, _ string, _ DeploymentReceipt) (HealthVerdict, error) {
	call := len(v.roots)
	v.roots = append(v.roots, root)
	if v.onVerify != nil {
		v.onVerify(call, root, commit)
	}
	return v.verdicts[min(call, len(v.verdicts)-1)], nil
}

func gateCoordinator(w *gateWorld, v *scriptedVerifier) Coordinator {
	return Coordinator{Broker: w.broker, Readiness: scriptedReadiness{}, Verifier: v}
}

func gateConfig() Config {
	return Config{OperationID: "op", Application: "app", Namespace: "ns", HealthObjective: "stay up", MaxAttempts: 1, PendingEvaluations: 1, InfrastructureTries: 1}
}

// The staged judge candidate is not the accepted head until the report accepts
// it: a designer-owned firing discards the stage and leaves HEAD where it was;
// the corrected proposal is then staged, verified in its own clean checkout and
// fast-forwarded as a single-parent commit.
func TestRealBrokerAcceptsOnlyAVerifiedCandidate(t *testing.T) {
	ctx := context.Background()
	w := newGateWorld(t)
	w.onboard(t)
	before := w.head(t)
	v := &scriptedVerifier{verdicts: []HealthVerdict{
		{Outcome: VerificationSourceFault, Summary: "firing", Detectors: []DetectorState{{DetectorID: "health", Status: "firing", Reason: "synthetic probe"}}},
		{Outcome: VerificationClear, Summary: "clear"},
	}}
	v.onVerify = func(call int, root, commit string) {
		if w.head(t) != before {
			t.Fatalf("verification %d ran after HEAD moved", call)
		}
		if got := w.git(t, root, "rev-parse", "HEAD"); got != commit || w.git(t, root, "status", "--porcelain") != "" {
			t.Fatalf("verification %d was not handed a clean checkout at %s", call, commit)
		}
	}
	c := gateCoordinator(w, v)
	deployment := w.receipt(t)

	w.judgeProposal(t, "judge-1")
	out, err := c.gateCandidate(ctx, "judge-1", gateConfig(), deployment, &contracts.ProposalRejectionLog{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range out.rejected {
		t.Logf("diagnostic %s %s: %s", d.GetRule(), d.GetPath(), d.GetMessage())
	}
	if out.ready || len(out.rejected) == 0 || out.rejected[0].GetRule() != "operability.O4.detectors" {
		t.Fatalf("outcome = %+v, want a designer correction; diagnostics: %v", out, out.rejected)
	}
	if w.head(t) != before {
		t.Fatal("a firing candidate moved the accepted head")
	}

	log := &contracts.ProposalRejectionLog{Rejections: []*contracts.ProposalRejection{broker.NewProposalRejection(1, out.rejected)}}
	out, err = c.gateCandidate(ctx, "judge-1", gateConfig(), deployment, log)
	if err != nil {
		t.Fatal(err)
	}
	if !out.ready || out.receipt.Commit == "" || w.head(t) != out.receipt.Commit {
		t.Fatalf("outcome = %+v head=%s, want the verified candidate accepted", out, w.head(t))
	}
	if parents := strings.Fields(w.git(t, w.accepted, "rev-list", "--parents", "-n1", "HEAD")); len(parents) != 2 || parents[1] != before {
		t.Fatalf("accepted commit parents = %v, want a single parent %s", parents, before)
	}
	if len(v.roots) != 2 || v.roots[0] == "" {
		t.Fatalf("verified roots = %v", v.roots)
	}
	// The accepted commit's complete-stage report is recorded and read back
	// verified: bound to the commit and the diagnostics tree there.
	report, found, err := w.broker.OperabilityReport(ctx, "judge-1", operability.StageComplete)
	if err != nil || !found {
		t.Fatalf("OperabilityReport = found %v, %v", found, err)
	}
	if report.Subject.AcceptedCommit != out.receipt.Commit || report.Decide(operability.StageItems(operability.StageComplete)...).Kind != operability.Accepted {
		t.Fatalf("recorded report %+v does not describe the accepted commit %s", report.Subject, out.receipt.Commit)
	}
}

// The broker refuses to fast-forward a staged judge candidate without a report,
// or with a report of another stage or diagnostics tree, whatever the
// coordinator believes; the head stays and the stage survives for a retry.
func TestRealBrokerRefusesAJudgeCandidateWithoutABoundReport(t *testing.T) {
	ctx := context.Background()
	w := newGateWorld(t)
	w.onboard(t)
	deployment := w.receipt(t)
	w.judgeProposal(t, "judge-1")
	candidate, err := w.broker.StageCandidate(ctx, "judge-1", broker.HealthJudge, broker.InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	before := w.head(t)
	evidence := func(mutate func(*operability.Report)) broker.CandidateEvidence {
		checks := []operability.Check{}
		for _, id := range []string{"O1.deploy", "O2.stamps", "O3.ready", "O4.detectors", "O5.memory", "O6.budget"} {
			checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass})
		}
		subject := operability.Subject{
			AcceptedCommit: candidate.Commit, BaseTree: deployment.BaseTree, EffectiveTree: deployment.EffectiveTree, PatchesDigest: deployment.PatchesDigest,
			RenderDigest: deployment.RenderDigest, EnvDigest: deployment.EnvDigest, DiagnosticsTree: candidate.DiagnosticsTree,
		}
		report, err := operability.New(operability.StageComplete, subject).With(checks...)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&report)
		return broker.CandidateEvidence{Commit: candidate.Commit, Tree: candidate.Tree, EffectiveTree: deployment.EffectiveTree, PatchesDigest: deployment.PatchesDigest, Operability: &report}
	}
	for name, mutate := range map[string]func(*operability.Report){
		"another diagnostics tree": func(r *operability.Report) { r.Subject.DiagnosticsTree = strings.Repeat("5", 40) },
		"the deployment stage":     func(r *operability.Report) { r.Stage = operability.StageDeployment },
	} {
		if _, err := w.broker.Accept(ctx, candidate, evidence(mutate)); !errors.Is(err, broker.ErrOperabilityUnbound) {
			t.Fatalf("%s: Accept = %v, want ErrOperabilityUnbound", name, err)
		}
	}
	missing := evidence(func(*operability.Report) {})
	missing.Operability = nil
	if _, err := w.broker.Accept(ctx, candidate, missing); !errors.Is(err, broker.ErrOperabilityUnbound) {
		t.Fatalf("Accept without a report = %v, want ErrOperabilityUnbound", err)
	}
	if w.head(t) != before {
		t.Fatal("a refused report moved the accepted head")
	}
	if _, err := w.broker.Accept(ctx, candidate, evidence(func(*operability.Report) {})); err != nil {
		t.Fatalf("Accept with a bound report: %v", err)
	}
}

// A peer commit lands while the candidate is being verified: Accept reports the
// stage stale, the gate stages the proposal again on the new head, verifies it
// again and accepts it on top of the peer's commit.
func TestRealBrokerStaleCandidateIsRestagedOnTheNewHead(t *testing.T) {
	ctx := context.Background()
	w := newGateWorld(t)
	w.onboard(t)
	before := w.head(t)
	v := &scriptedVerifier{verdicts: []HealthVerdict{{Outcome: VerificationClear, Summary: "clear"}}}
	var peer string
	v.onVerify = func(call int, _, _ string) {
		if call != 0 {
			return
		}
		// A peer publishes a new goal while the first verification runs.
		p, err := w.broker.CreateProposalForPhase(ctx, "peer-goal", broker.InitialPhase)
		if err != nil {
			t.Fatal(err)
		}
		goal := filepath.Join(p.Path, ".sdo", "goal.md")
		data, err := os.ReadFile(goal)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goal, append(data, []byte("\nPeer revision.\n")...), 0o644); err != nil {
			t.Fatal(err)
		}
		receipt, err := w.broker.Publish(ctx, "peer-goal", broker.Human, broker.InitialPhase)
		if err != nil {
			t.Fatal(err)
		}
		peer = receipt.Commit
	}
	c := gateCoordinator(w, v)
	w.judgeProposal(t, "judge-1")
	out, err := c.gateCandidate(ctx, "judge-1", gateConfig(), w.receipt(t), &contracts.ProposalRejectionLog{})
	if err != nil {
		t.Fatal(err)
	}
	if !out.ready || peer == "" || peer == before {
		t.Fatalf("outcome = %+v peer=%q", out, peer)
	}
	if parents := strings.Fields(w.git(t, w.accepted, "rev-list", "--parents", "-n1", "HEAD")); len(parents) != 2 || parents[1] != peer || w.head(t) != out.receipt.Commit {
		t.Fatalf("accepted history = %v, want the candidate restaged on the peer commit %s", parents, peer)
	}
	if len(v.roots) != 2 {
		t.Fatalf("verifications = %d, want the restaged candidate verified again", len(v.roots))
	}
}
