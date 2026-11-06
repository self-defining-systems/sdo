//go:build integration

package broker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/rendered"
)

// stageResponderPatch captures a one-line edit of the vendored file as the
// responder's patch and stages the proposal as a candidate.
func stageResponderPatch(t *testing.T, w *patchWorld, id string) Candidate {
	t.Helper()
	ctx := context.Background()
	seedIfNeeded(t, w)
	p, err := w.broker.CreateProposalForPhase(ctx, id, ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"lib/vendor.txt": vendorText + "extra\n"})
	if _, err := patchset.Create(ctx, p.Path, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: id, Reason: patchReason}); err != nil {
		t.Fatal(err)
	}
	candidate, err := w.broker.StageCandidate(ctx, id, Responder, ProposalPhase)
	if err != nil {
		t.Fatalf("stage the responder patch: %v", err)
	}
	return candidate
}

func seedIfNeeded(t *testing.T, w *patchWorld) {
	t.Helper()
	if gitOut(t, w.accepted, "ls-tree", "HEAD", ".sdo/schema-version") == "" {
		seedMemory(t, w)
	}
}

// A plain accept (the controller without a repairer: shell detectors or frozen
// memory) takes a responder's patch, but nothing deploys it and no deployment
// evidence is recorded, so the accepted evidence and the image pins still
// describe the last deployed effective tree. The closure records that path as
// source_repair_published_undeployed.
func TestPlainAcceptOfAResponderPatchLeavesEvidenceAndPinsAtTheLastDeployment(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	before, found, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("accepted evidence: %v %v", found, err)
	}
	candidate := stageResponderPatch(t, w, "inc-plain-2")
	if _, err := w.broker.Accept(ctx, candidate, boundEvidence(t, candidate)); err != nil {
		t.Fatal(err)
	}
	after, _, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Evidence.EffectiveTree != before.Evidence.EffectiveTree {
		t.Fatal("a plain accept recorded deployment evidence")
	}
	current := effectiveID(t, w.accepted)
	if current == before.Evidence.EffectiveTree {
		t.Fatal("test setup: the patch did not change the effective tree")
	}
	pins, err := w.broker.AcceptedImagePins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pins.EffectiveTree != before.Evidence.EffectiveTree {
		t.Fatalf("pins moved to %s without a deployment", pins.EffectiveTree)
	}
}

func effectiveID(t *testing.T, root string) string {
	t.Helper()
	tree := gitOut(t, root, "rev-parse", "HEAD^{tree}")
	effective, err := rendered.Effective(context.Background(), rendered.ObjectStore{Dir: root}, rendered.BaseTree(tree))
	if err != nil {
		t.Fatal(err)
	}
	defer effective.Close()
	return effective.ID
}

// AcceptDeployed accepts a staged responder repair only with evidence of the
// deployment of its effective tree, records that evidence for the accepted
// commit, and makes it the newest deployment: pins and the next advance follow.
func TestAcceptDeployedRecordsTheRepairAsTheNewestDeployment(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	onboarded, _, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate := stageResponderPatch(t, w, "inc-deploy")
	evidence := advanceEvidenceFor(t, candidate.Path, "repaired", true)
	if evidence.EffectiveTree == onboarded.Evidence.EffectiveTree {
		t.Fatal("test setup: the patch did not change the effective tree")
	}
	receipt, err := w.broker.AcceptDeployed(ctx, candidate, evidence)
	if err != nil {
		t.Fatalf("AcceptDeployed: %v", err)
	}
	requireHead(t, w.advanceWorld, receipt.Commit, "accepting the deployed repair")
	accepted, found, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("accepted evidence after the repair: %v %v", found, err)
	}
	if accepted.Evidence.EffectiveTree != evidence.EffectiveTree || accepted.Evidence.AcceptedCommit != receipt.Commit {
		t.Fatalf("accepted evidence = %+v, want the repair's deployment of %s at %s", accepted.Evidence, evidence.EffectiveTree, receipt.Commit)
	}
	pins, err := w.broker.AcceptedImagePins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pins.EffectiveTree != evidence.EffectiveTree || len(pins.Images) != len(evidence.ContainerImages) || pins.BaseTree != candidate.Tree {
		t.Fatalf("pins = %+v, want those of the repair deployment", pins)
	}
	// Redelivery after a crash is idempotent; different evidence is refused.
	if again, err := w.broker.AcceptDeployed(ctx, candidate, evidence); err != nil || again.Commit != receipt.Commit {
		t.Fatalf("redelivered AcceptDeployed = %+v, %v", again, err)
	}
	other := evidence
	other.Images = []string{"other@sha256:" + strings.Repeat("e", 64)}
	if _, err := w.broker.AcceptDeployed(ctx, candidate, other); err == nil {
		t.Fatal("a redelivery with different evidence was accepted")
	}
}

// The broker never trusts evidence it did not bind to the stage.
func TestAcceptDeployedRefusesEvidenceThatDoesNotBindToTheStage(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	candidate := stageResponderPatch(t, w, "inc-refuse")
	head := w.head(t)
	good := advanceEvidenceFor(t, candidate.Path, "repaired", true)
	for _, tc := range []struct {
		name   string
		mutate func(*SourceEvidence)
		want   error
	}{
		{"another tree", func(e *SourceEvidence) { e.BaseTree = strings.Repeat("1", 40) }, ErrCandidateUnbound},
		{"another effective tree", func(e *SourceEvidence) {
			e.EffectiveTree, e.ObservedRevision = strings.Repeat("2", 40), strings.Repeat("2", 40)
		}, ErrCandidateUnbound},
		{"another patch series", func(e *SourceEvidence) { e.PatchesDigest = strings.Repeat("3", 64) }, ErrCandidateUnbound},
		{"not ready", func(e *SourceEvidence) { e.Ready = false }, nil},
		{"claims an accepted commit", func(e *SourceEvidence) { e.AcceptedCommit = candidate.Commit }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := good
			tc.mutate(&evidence)
			_, err := w.broker.AcceptDeployed(ctx, candidate, evidence)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("AcceptDeployed = %v, want a refusal (%v)", err, tc.want)
			}
			requireHead(t, w.advanceWorld, head, "a refused deployment")
		})
	}
	// A deployment of another source than the plan the tree declares.
	other := good
	other.DeploymentSource = syntheticPlan(t, "elsewhere").GetSource()
	if _, err := w.broker.AcceptDeployed(ctx, candidate, other); err == nil {
		t.Fatal("evidence of another deployment source was accepted")
	}
	requireHead(t, w.advanceWorld, head, "a deployment of another source")
}

// A head that moved after staging means the deployment verified a stale tree:
// the candidate is refused, never accepted on top.
func TestAcceptDeployedRefusesAStaleCandidate(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	candidate := stageResponderPatch(t, w, "inc-stale")
	evidence := advanceEvidenceFor(t, candidate.Path, "repaired", true)
	writeFiles(t, w.accepted, map[string]string{".sdo/notes": "peer\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: peer commit")
	if _, err := w.broker.AcceptDeployed(ctx, candidate, evidence); !errors.Is(err, ErrCandidateStale) {
		t.Fatalf("AcceptDeployed on a moved head = %v, want ErrCandidateStale", err)
	}
}

// AcceptDeployed records the evidence, moves the head, then finalizes the
// evidence. A crash between the last two leaves the repair accepted with
// pending evidence; the redelivered repair must still become the newest
// deployment, whether the redelivery finalizes it or a read does.
func TestAcceptedRepairWithPendingEvidenceIsFinalizedOnRedelivery(t *testing.T) {
	for _, finalize := range []bool{true, false} {
		name := "read self-heals"
		if finalize {
			name = "redelivery finalizes"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			w := newPatchWorld(t, false)
			candidate := stageResponderPatch(t, w, "inc-crash")
			evidence := advanceEvidenceFor(t, candidate.Path, "repaired", true)
			// The crash: the pending record is durable and the head moved, the
			// finalizing write never happened.
			if err := w.broker.locked(func() error {
				if err := w.broker.writeSourceEvidence(candidate.IncidentID, evidence); err != nil {
					return err
				}
				return w.broker.advanceHead(ctx, candidate.Path, candidate.Commit)
			}); err != nil {
				t.Fatal(err)
			}
			redelivered, err := w.broker.StageCandidate(ctx, candidate.IncidentID, Responder, ProposalPhase)
			if err != nil || !redelivered.Accepted || redelivered.Commit != candidate.Commit {
				t.Fatalf("redelivery = %+v, %v; want the accepted commit", redelivered, err)
			}
			if finalize {
				deployed, err := w.broker.FinalizeAcceptedDeployment(ctx, candidate.IncidentID)
				if err != nil || !deployed {
					t.Fatalf("FinalizeAcceptedDeployment = %v, %v", deployed, err)
				}
			}
			accepted, found, err := w.broker.AcceptedSourceEvidence(ctx)
			if err != nil || !found {
				t.Fatalf("accepted evidence: %v %v", found, err)
			}
			if accepted.Evidence.EffectiveTree != evidence.EffectiveTree || accepted.Evidence.AcceptedCommit != candidate.Commit {
				t.Fatalf("accepted evidence = %+v, want the repair's deployment at %s", accepted.Evidence, candidate.Commit)
			}
			pins, err := w.broker.AcceptedImagePins(ctx)
			if err != nil || pins.EffectiveTree != evidence.EffectiveTree {
				t.Fatalf("pins = %+v, %v; want the repair's effective tree", pins, err)
			}
		})
	}
}

// Pending evidence of another tree (a later staged deployment of the same
// incident whose head never moved) never makes an accepted commit a deployment.
func TestFinalizeAcceptedDeploymentIgnoresEvidenceOfAnotherTree(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	candidate := stageResponderPatch(t, w, "inc-other-tree")
	other := advanceEvidenceFor(t, candidate.Path, "repaired", true)
	if _, err := w.broker.Accept(ctx, candidate, boundEvidence(t, candidate)); err != nil {
		t.Fatal(err)
	}
	other.BaseTree = strings.Repeat("a", 40)
	if err := w.broker.locked(func() error { return w.broker.writeSourceEvidence(candidate.IncidentID, other) }); err != nil {
		t.Fatal(err)
	}
	if deployed, err := w.broker.FinalizeAcceptedDeployment(ctx, candidate.IncidentID); err == nil && deployed {
		t.Fatal("evidence of another tree made the accepted commit a deployment")
	}
}
