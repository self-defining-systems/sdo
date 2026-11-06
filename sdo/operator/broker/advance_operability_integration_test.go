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

// An advance is accepted only with a stage-advance report that the broker
// recomputes: the subject must bind to the evidence's trees and digests and to
// the accepted head the advance was merged onto, and Decide must accept O1, O2,
// O4 and O6. A refusal records nothing and leaves the head where it was.
func TestPublishAdvanceRefusesAReportThatDoesNotBind(t *testing.T) {
	ctx := context.Background()
	other := strings.Repeat("9", 64)
	w := newAdvanceWorld(t)
	b := w.broker
	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id := "advance-" + upstream[:12]
	proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil {
		t.Fatal(err)
	}
	evidence := advanceEvidence(t, proposal.Path, "merged")
	failing := func(r *operability.Report, item int, c operability.Check) {
		r.Items[item].Checks = append(r.Items[item].Checks, c)
	}
	for name, mutate := range map[string]func(*operability.Report){
		"another effective tree": func(r *operability.Report) { r.Subject.EffectiveTree = strings.Repeat("9", 40) },
		"another patch series":   func(r *operability.Report) { r.Subject.PatchesDigest = other },
		"another source tree":    func(r *operability.Report) { r.Subject.BaseTree = strings.Repeat("8", 40) },
		"another render":         func(r *operability.Report) { r.Subject.RenderDigest = other },
		"another accepted head":  func(r *operability.Report) { r.Subject.AcceptedCommit = strings.Repeat("7", 40) },
		"a failing detector verdict": func(r *operability.Report) {
			failing(r, 3, operability.Check{ID: "O4.firing", Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: operability.CauseSourceFault})
		},
		"a failing deployment check": func(r *operability.Report) {
			failing(r, 0, operability.Check{ID: "O1.ready", Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: operability.CauseRollout})
		},
		"no detector verdict":  func(r *operability.Report) { r.Items[3].Checks = []operability.Check{} },
		"the deployment stage": func(r *operability.Report) { r.Stage = operability.StageDeployment },
	} {
		t.Run(name, func(t *testing.T) {
			head := w.head(t)
			report := advanceReportFor(t, evidence, head)
			mutate(&report)
			if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence, report); !errors.Is(err, ErrOperabilityUnbound) {
				t.Fatalf("PublishAdvance = %v, want ErrOperabilityUnbound", err)
			}
			if w.head(t) != head {
				t.Fatal("a refused report moved the accepted head")
			}
			if _, statErr := os.Stat(b.operabilityPath(id, operability.StageAdvance)); !os.IsNotExist(statErr) {
				t.Fatalf("a refused report was recorded: %v", statErr)
			}
		})
	}
	t.Run("evidence naming another report", func(t *testing.T) {
		withDigest := evidence
		withDigest.OperabilityDigest = strings.Repeat("a", 64)
		if _, err := b.PublishAdvance(ctx, id, upstream, w.seed, withDigest, advanceReportFor(t, evidence, w.head(t))); !errors.Is(err, ErrOperabilityUnbound) {
			t.Fatalf("PublishAdvance = %v, want ErrOperabilityUnbound", err)
		}
	})
}

// An accepted advance records its report: digest in the evidence, an
// SDO-Operability trailer on the commit, the report file read back verified,
// and a redelivery keeps the first report.
func TestPublishAdvanceRecordsTheReport(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	b := w.broker
	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id := "advance-" + upstream[:12]
	proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil {
		t.Fatal(err)
	}
	evidence := advanceEvidence(t, proposal.Path, "merged")
	report := advanceReportFor(t, evidence, w.head(t))
	receipt, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence, report)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := report.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if message := gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit); !strings.Contains("\n"+message+"\n", "\nSDO-Operability: advance sha256:"+digest+"\n") {
		t.Fatalf("advance commit lacks the SDO-Operability trailer:\n%s", message)
	}
	stored, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found || stored.Evidence.OperabilityDigest != digest {
		t.Fatalf("evidence = %+v found=%v err=%v, want digest %s", stored.Evidence, found, err, digest)
	}
	got, found, err := b.OperabilityReport(ctx, id, operability.StageAdvance)
	if err != nil || !found {
		t.Fatalf("OperabilityReport = found %v, %v", found, err)
	}
	if again, err := got.Digest(); err != nil || again != digest {
		t.Fatalf("read back digest %s (%v), want %s", again, err, digest)
	}
	// A redelivery keeps the first report even when its live observations differ.
	redelivered := advanceReportFor(t, evidence, receipt.Commit+"x")
	redelivered.Items[0].Checks[0].Detail = "observed again"
	if again, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence, redelivered); err != nil || again.Commit != receipt.Commit {
		t.Fatalf("redelivery = %+v %v, want the accepted %s", again, err, receipt.Commit)
	}
	// Reads verify: an edited report no longer matches the evidence.
	edited := got
	edited.Items[0].Checks[0].Detail = "edited"
	data, err := edited.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b.operabilityPath(id, operability.StageAdvance), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.OperabilityReport(ctx, id, operability.StageAdvance); !errors.Is(err, ErrOperabilityUnbound) {
		t.Fatalf("OperabilityReport of an edited report = %v", err)
	}
}

// A crash after the head moved leaves the advance's evidence pending
// (recorded before the head moves, finalized after). The redelivery and the
// accepted-evidence reads finalize it: the advance stays the newest deployment
// with its report digest.
func TestPublishAdvanceRedeliveredWithPendingEvidenceFinalizesIt(t *testing.T) {
	ctx := context.Background()
	w := newAdvanceWorld(t)
	b := w.broker
	upstream := w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id := "advance-" + upstream[:12]
	proposal, _, _, err := b.ProposeAdvance(ctx, id, w.seed, upstream)
	if err != nil {
		t.Fatal(err)
	}
	evidence := advanceEvidence(t, proposal.Path, "merged")
	receipt, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence, advanceReportFor(t, evidence, w.head(t)))
	if err != nil {
		t.Fatal(err)
	}
	finalized, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("accepted evidence: %v %v", found, err)
	}
	pending := finalized.Evidence
	pending.AcceptedCommit = ""
	if err := b.locked(func() error { return b.writeSourceEvidence(id, pending) }); err != nil {
		t.Fatal(err)
	}
	pins, err := b.AcceptedImagePins(ctx)
	if err != nil || pins.EffectiveTree != evidence.EffectiveTree {
		t.Fatalf("pins with pending advance evidence = %+v, %v", pins, err)
	}
	if err := b.locked(func() error { return b.writeSourceEvidence(id, pending) }); err != nil {
		t.Fatal(err)
	}
	if again, err := b.PublishAdvance(ctx, id, upstream, w.seed, evidence, advanceReportFor(t, evidence, w.head(t))); err != nil || again.Commit != receipt.Commit {
		t.Fatalf("redelivery = %+v %v", again, err)
	}
	after, found, err := b.AcceptedSourceEvidence(ctx)
	if err != nil || !found || after.Evidence.AcceptedCommit != receipt.Commit || after.Evidence.OperabilityDigest != finalized.Evidence.OperabilityDigest {
		t.Fatalf("evidence after redelivery = %+v found=%v err=%v", after.Evidence, found, err)
	}
}
