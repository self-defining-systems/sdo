//go:build integration

package broker

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"sdo.dev/operator/operability"
)

// onboardProposal opens the deployer's proposal as the lifecycle does and
// returns it with the deployed evidence and its passing report.
func onboardProposal(t *testing.T, w *advanceWorld, id string) (Proposal, SourceEvidence, operability.Report) {
	t.Helper()
	p, err := w.broker.CreateProposalForPhase(context.Background(), id, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{".sdo/deployment/values/" + id + ".yaml": "replicas: 2\n"})
	evidence := advanceEvidence(t, p.Path, id)
	return p, evidence, deploymentReportFor(t, evidence, p.BaseCommit)
}

// The stage-deployment report is bound by recomputation, never trusted: a report
// for another tree, patch series, render, environment or commit, one that fails
// Decide, or one of another stage cannot be attached, and a refusal records
// nothing and leaves the accepted head where it was.
func TestPublishVerifiedSourceRefusesAReportThatDoesNotBindOrDecide(t *testing.T) {
	ctx := context.Background()
	other := strings.Repeat("9", 64)
	failing := func(r *operability.Report, check operability.Check) {
		r.Items[0].Checks = append(r.Items[0].Checks, check)
	}
	w := newAdvanceWorld(t)
	for name, mutate := range map[string]func(*operability.Report){
		"another effective tree": func(r *operability.Report) { r.Subject.EffectiveTree = strings.Repeat("9", 40) },
		"another patch series":   func(r *operability.Report) { r.Subject.PatchesDigest = other },
		"another source tree":    func(r *operability.Report) { r.Subject.BaseTree = strings.Repeat("8", 40) },
		"another render":         func(r *operability.Report) { r.Subject.RenderDigest = other },
		"another environment":    func(r *operability.Report) { r.Subject.EnvDigest = other },
		"another base commit":    func(r *operability.Report) { r.Subject.AcceptedCommit = strings.Repeat("7", 40) },
		"unavailable subject": func(r *operability.Report) {
			r.Subject = r.Subject.WithUnavailable()
			r.Subject.EnvDigest = "unavailable"
		},
		"a failing check": func(r *operability.Report) {
			failing(r, operability.Check{ID: "O1.render", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseRender})
		},
		"a pending check": func(r *operability.Report) {
			failing(r, operability.Check{ID: "O1.ready", Phase: operability.PhaseLive, Status: operability.StatusPending, Cause: operability.CauseInfrastructure})
		},
		"a required item without checks": func(r *operability.Report) { r.Items[5].Checks = []operability.Check{} },
		"the complete stage":             func(r *operability.Report) { r.Stage = operability.StageComplete },
		"an unknown schema":              func(r *operability.Report) { r.Schema = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			id := "bind-" + strings.ReplaceAll(name, " ", "-")
			p, evidence, report := onboardProposal(t, w, id)
			mutate(&report)
			head := w.head(t)
			_, err := w.broker.PublishVerifiedSource(ctx, id, evidence, report)
			if !errors.Is(err, ErrOperabilityUnbound) {
				t.Fatalf("PublishVerifiedSource = %v, want ErrOperabilityUnbound", err)
			}
			if w.head(t) != head {
				t.Fatal("a refused report moved the accepted head")
			}
			if _, statErr := os.Stat(w.broker.operabilityPath(id, operability.StageDeployment)); !os.IsNotExist(statErr) {
				t.Fatalf("a refused report was recorded: %v", statErr)
			}
			if _, found, err := w.broker.OperabilityReport(ctx, id, operability.StageDeployment); err != nil || found {
				t.Fatalf("OperabilityReport after refusal = found %v, %v", found, err)
			}
			_ = p
		})
	}
}

// Evidence naming another report than the one supplied is refused.
func TestPublishVerifiedSourceRefusesEvidenceNamingAnotherReport(t *testing.T) {
	w := newAdvanceWorld(t)
	_, evidence, report := onboardProposal(t, w, "named")
	evidence.OperabilityDigest = strings.Repeat("a", 64)
	if _, err := w.broker.PublishVerifiedSource(context.Background(), "named", evidence, report); !errors.Is(err, ErrOperabilityUnbound) {
		t.Fatalf("PublishVerifiedSource = %v, want ErrOperabilityUnbound", err)
	}
}

// An accepted report is recorded with its digest in the evidence and as an
// SDO-Operability trailer, and read back verified.
func TestPublishVerifiedSourceRecordsTheReport(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	_, evidence, report := onboardProposal(t, w, "recorded")
	receipt, err := w.broker.PublishVerifiedSource(ctx, "recorded", evidence, report)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := report.Digest()
	if err != nil {
		t.Fatal(err)
	}
	stored, found, err := w.broker.SourceEvidence(ctx, "recorded")
	if err != nil || !found || stored.OperabilityDigest != digest {
		t.Fatalf("evidence = %+v found=%v err=%v, want digest %s", stored, found, err, digest)
	}
	if message := gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit); !strings.Contains("\n"+message+"\n", "\nSDO-Operability: deployment sha256:"+digest+"\n") {
		t.Fatalf("commit message lacks the SDO-Operability trailer:\n%s", message)
	}
	got, found, err := w.broker.OperabilityReport(ctx, "recorded", operability.StageDeployment)
	if err != nil || !found {
		t.Fatalf("OperabilityReport = found %v, %v", found, err)
	}
	if again, err := got.Digest(); err != nil || again != digest {
		t.Fatalf("read back digest %s (%v), want %s", again, err, digest)
	}
	// A redelivery after a crash keeps the recorded report.
	if _, err := w.broker.PublishVerifiedSource(ctx, "recorded", evidence, report); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
}

// Every read verifies: a stored report that was edited, removed, or whose
// evidence names none fails, the last two with the reinstall message.
func TestOperabilityReportIsVerifiedAtEveryRead(t *testing.T) {
	ctx := context.Background()
	publish := func(t *testing.T, id string) *advanceWorld {
		w := newAdvanceWorld(t)
		_, evidence, report := onboardProposal(t, w, id)
		if _, err := w.broker.PublishVerifiedSource(ctx, id, evidence, report); err != nil {
			t.Fatal(err)
		}
		return w
	}
	t.Run("edited", func(t *testing.T) {
		w := publish(t, "edited")
		path := w.broker.operabilityPath("edited", operability.StageDeployment)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		edited := strings.Replace(string(data), `"status":"pass"`, `"status":"note"`, 1)
		if edited == string(data) {
			t.Fatal("the stored report has no pass to edit")
		}
		if err := os.WriteFile(path, []byte(edited), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.broker.OperabilityReport(ctx, "edited", operability.StageDeployment); !errors.Is(err, ErrOperabilityUnbound) {
			t.Fatalf("OperabilityReport of an edited report = %v", err)
		}
	})
	t.Run("removed", func(t *testing.T) {
		w := publish(t, "removed")
		if err := os.Remove(w.broker.operabilityPath("removed", operability.StageDeployment)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.broker.OperabilityReport(ctx, "removed", operability.StageDeployment); !errors.Is(err, ErrSourceEvidencePredatesOperability) || !strings.Contains(err.Error(), "reinstall") {
			t.Fatalf("OperabilityReport without a file = %v", err)
		}
	})
	t.Run("evidence names none", func(t *testing.T) {
		w := publish(t, "unnamed")
		stored, found, err := w.broker.SourceEvidence(ctx, "unnamed")
		if err != nil || !found {
			t.Fatal(err)
		}
		stored.OperabilityDigest = ""
		if err := w.broker.writeSourceEvidence("unnamed", stored); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.broker.OperabilityReport(ctx, "unnamed", operability.StageDeployment); !errors.Is(err, ErrSourceEvidencePredatesOperability) {
			t.Fatalf("OperabilityReport with unnamed evidence = %v", err)
		}
	})
}

// A candidate's complete-stage report binds to the stage and to the accepted
// deployment: its commit, effective tree, diagnostics tree, and the source tree
// and digests of the newest deployment evidence.
func TestCandidateOperabilityIsRecomputedFromTheStageAndTheDeployment(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	_, evidence, report := onboardProposal(t, w, "deployed")
	if _, err := w.broker.PublishVerifiedSource(ctx, "deployed", evidence, report); err != nil {
		t.Fatal(err)
	}
	head := w.head(t)
	tree := gitOut(t, w.accepted, "rev-parse", head+"^{tree}")
	diagnostics, err := DiagnosticsTree(ctx, w.accepted, head)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{IncidentID: "judge", Actor: HealthJudge, Phase: InitialPhase, Path: w.accepted, Commit: head, Tree: tree}
	good := func() CandidateEvidence {
		subject := operability.Subject{
			AcceptedCommit: head, BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree, PatchesDigest: evidence.PatchesDigest,
			RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest, DiagnosticsTree: diagnostics,
		}
		complete, err := operability.New(operability.StageComplete, subject).With(passingChecks("O1.deploy", "O2.stamps", "O3.ready", "O4.detectors", "O5.memory", "O6.budget")...)
		if err != nil {
			t.Fatal(err)
		}
		return CandidateEvidence{Commit: head, Tree: tree, EffectiveTree: evidence.EffectiveTree, PatchesDigest: evidence.PatchesDigest, Operability: &complete}
	}
	other := strings.Repeat("9", 64)
	failed := operability.Check{ID: "O4.detectors", Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: operability.CauseDetectorFault}
	for name, mutate := range map[string]func(*CandidateEvidence){
		"no report":                func(e *CandidateEvidence) { e.Operability = nil },
		"another commit":           func(e *CandidateEvidence) { e.Operability.Subject.AcceptedCommit = strings.Repeat("7", 40) },
		"another diagnostics tree": func(e *CandidateEvidence) { e.Operability.Subject.DiagnosticsTree = strings.Repeat("6", 40) },
		"another source tree":      func(e *CandidateEvidence) { e.Operability.Subject.BaseTree = strings.Repeat("8", 40) },
		"another effective tree":   func(e *CandidateEvidence) { e.Operability.Subject.EffectiveTree = strings.Repeat("9", 40) },
		"another patch series":     func(e *CandidateEvidence) { e.Operability.Subject.PatchesDigest = other },
		"another render":           func(e *CandidateEvidence) { e.Operability.Subject.RenderDigest = other },
		"another environment":      func(e *CandidateEvidence) { e.Operability.Subject.EnvDigest = other },
		"the deployment stage":     func(e *CandidateEvidence) { e.Operability.Stage = operability.StageDeployment },
		"a failing detector":       func(e *CandidateEvidence) { e.Operability.Items[3].Checks = []operability.Check{failed} },
		"a pending detector": func(e *CandidateEvidence) {
			failed.Status = operability.StatusPending
			e.Operability.Items[3].Checks = []operability.Check{failed}
		},
		"a required item with no checks": func(e *CandidateEvidence) { e.Operability.Items[2].Checks = []operability.Check{} },
	} {
		t.Run(name, func(t *testing.T) {
			evidence := good()
			mutate(&evidence)
			if _, err := w.broker.requireCandidateOperability(ctx, candidate, evidence); !errors.Is(err, ErrOperabilityUnbound) {
				t.Fatalf("requireCandidateOperability = %v, want ErrOperabilityUnbound", err)
			}
		})
	}
	if reported, err := w.broker.requireCandidateOperability(ctx, candidate, good()); err != nil || reported == nil {
		t.Fatalf("a bound report is refused: %v", err)
	}
	// Other candidates carry no report.
	other2 := candidate
	other2.Actor = Human
	if _, err := w.broker.requireCandidateOperability(ctx, other2, good()); !errors.Is(err, ErrCandidateUnbound) {
		t.Fatalf("a non-judge candidate with a report = %v, want ErrCandidateUnbound", err)
	}
	if reported, err := w.broker.requireCandidateOperability(ctx, other2, CandidateEvidence{}); err != nil || reported != nil {
		t.Fatalf("a non-judge candidate without a report = %v, %v", reported, err)
	}
}
