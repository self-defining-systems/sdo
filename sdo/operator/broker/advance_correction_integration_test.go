//go:build integration

package broker

import (
	"context"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
)

// correctedWorld proposes an advance and returns the checkout and the candidate.
func correctedWorld(t *testing.T) (w *advanceWorld, id, upstream string, proposal Proposal, candidate string) {
	t.Helper()
	w = newAdvanceWorld(t)
	// The synthetic world has no architecture or schema stamp; a correction
	// changes memory, so the whole tree is validated.
	writeFiles(t, w.accepted, map[string]string{layout.SchemaVersionFile: "8\n", layout.ArchitectureFile: "---\nschema_version: 1\n---\n# Architecture\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: memory stamps")
	upstream = w.push(t, w.seed, map[string]*string{"app.txt": text("ONE\ntwo\nthree\n")})
	id = "advance-" + upstream[:12]
	proposal, candidate, _, err := w.broker.ProposeAdvance(context.Background(), id, w.seed, upstream)
	if err != nil || candidate == "" {
		t.Fatalf("ProposeAdvance = %q %v", candidate, err)
	}
	return w, id, upstream, proposal, candidate
}

// The deployment agent's correction of an advance (a deployment file and the
// plan) is committed on the advance checkout under the deployer's advance
// grants, deployed from there, and replayed by PublishAdvance onto its own
// recomputed merge: the accepted commit holds the merge and the correction,
// records the correction trailer, and the accepted plan is the corrected one.
func TestCorrectedAdvanceIsAcceptedWithTheCorrection(t *testing.T) {
	ctx := context.Background()
	w, id, upstream, proposal, candidate := correctedWorld(t)
	plan := syntheticPlan(t, "k8s")
	plan.ExpectedWorkloadIds = append(plan.ExpectedWorkloadIds, "Deployment/ns/extra")
	file := ".sdo/deployment/values/extra.yaml"
	writeFiles(t, proposal.Path, map[string]string{file: "replicas: 2\n"})
	corrected, rejection, err := w.broker.CorrectAdvance(ctx, id, plan)
	if err != nil || rejection != "" || corrected == "" || corrected == candidate {
		t.Fatalf("CorrectAdvance = %q %q %v", corrected, rejection, err)
	}
	if gitOut(t, proposal.Path, "rev-parse", "HEAD") != corrected || gitOut(t, proposal.Path, "status", "--porcelain", "--untracked-files=all") != "" {
		t.Fatal("the corrected checkout is not clean at the new candidate")
	}
	if parent := gitOut(t, proposal.Path, "rev-parse", corrected+"^"); parent != candidate {
		t.Fatalf("the correction sits on %s, not the merge candidate %s", parent, candidate)
	}
	accepted := w.head(t)
	evidence := advanceEvidence(t, proposal.Path, "corrected")
	receipt, err := publishAdvance(ctx, w.broker, id, upstream, w.seed, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if gitOut(t, w.accepted, "show", receipt.Commit+":"+file) != "replicas: 2" {
		t.Fatal("the accepted tree lacks the correction")
	}
	got, err := planfile.Read(w.accepted, planfile.Catalogs{})
	if err != nil || !slices.Equal(got.GetExpectedWorkloadIds(), plan.GetExpectedWorkloadIds()) {
		t.Fatalf("accepted plan = %v %v", got.GetExpectedWorkloadIds(), err)
	}
	message := "\n" + gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit) + "\n"
	for _, trailer := range []string{"SDO-Actor: upstream", "SDO-Advance-Correction: deployer", "SDO-Upstream: " + upstream} {
		if !strings.Contains(message, "\n"+trailer+"\n") {
			t.Errorf("accepted commit lacks %q:\n%s", trailer, message)
		}
	}
	if parents := strings.Fields(gitOut(t, w.accepted, "log", "-1", "--format=%P", receipt.Commit)); !slices.Equal(parents, []string{accepted}) {
		t.Fatalf("parents %v, want only %s", parents, accepted)
	}
}

// A correction the grants refuse is returned as feedback text and leaves the
// checkout at the candidate: app files are never edited, other memory is not
// the deployer's, and a correction that changes nothing is not one.
func TestCorrectAdvanceRejectsWhatTheDeployerMayNotChange(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"an app file":    {map[string]string{"app.txt": "edited\n"}, "app.txt"},
		"a new app file": {map[string]string{"new.txt": "x\n"}, "new.txt"},
		"other memory":   {map[string]string{layout.GoalFile: "# a different goal\n"}, layout.GoalFile},
		"nothing":        {nil, "changed nothing"},
	} {
		t.Run(name, func(t *testing.T) {
			w, id, _, proposal, candidate := correctedWorld(t)
			writeFiles(t, proposal.Path, tc.files)
			got, rejection, err := w.broker.CorrectAdvance(ctx, id, syntheticPlan(t, "k8s"))
			if err != nil || got != "" || !strings.Contains(rejection, tc.want) {
				t.Fatalf("CorrectAdvance = %q %q %v, want a rejection naming %q", got, rejection, err, tc.want)
			}
			if gitOut(t, proposal.Path, "rev-parse", "HEAD") != candidate || gitOut(t, proposal.Path, "status", "--porcelain", "--untracked-files=all") != "" {
				t.Fatal("a rejected correction left the checkout dirty")
			}
		})
	}
}

// An accepted advance without a correction is as before: no trailer.
func TestUncorrectedAdvanceHasNoCorrectionTrailer(t *testing.T) {
	w, id, upstream, proposal, _ := correctedWorld(t)
	receipt, err := publishAdvance(context.Background(), w.broker, id, upstream, w.seed, advanceEvidence(t, proposal.Path, "plain"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit), "SDO-Advance-Correction") {
		t.Fatal("an uncorrected advance carries the correction trailer")
	}
}
