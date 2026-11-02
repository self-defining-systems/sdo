//go:build integration

package broker_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/validate"
)

// A reflection that leaves out learning the memory edge can live without is
// accepted and records what it left out; the checks that protect memory quality
// (detector tests, mitigations that repair) still reject.
func TestReflectionRecordsCoverageGapsButStillRejectsInvalidCode(t *testing.T) {
	ctx := context.Background()
	binary := operatorCLIFixture(t)
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	// Two verified causes: the port repair has a program, the catalog repair has
	// none. The playbook uses a literal name and a wrong fault_class, and the
	// controller has no healthy capture.
	setup := func(t *testing.T) (*broker.Broker, string, broker.Proposal, *contracts.ReflectionDecision) {
		root := copyParityTree(t)
		testutil.InitGit(t, root)
		runGit(t, root, "config", "user.name", "Fixture")
		runGit(t, root, "config", "user.email", "fixture@example.test")
		runGit(t, root, "add", "-A")
		runGit(t, root, "commit", "-qm", "accepted memory")
		repository, err := broker.New(root, t.TempDir(), (broker.SubprocessValidator{Binary: binary, OperatorRoot: operatorRoot}).Validator())
		if err != nil {
			t.Fatal(err)
		}
		covered := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
		missing := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "catalog"}
		outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success",
			ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing", Resources: []*contracts.ObjectRef{covered}}, {Summary: "catalog", Resources: []*contracts.ObjectRef{missing}}},
		}
		parityFindingRepairProof(outcome)
		if _, err := repository.RecordOutcome(ctx, "incident", outcome); err != nil {
			t.Fatal(err)
		}
		proposal, err := repository.CreateProposalForPhase(ctx, "incident", broker.ReflectionPhase)
		if err != nil {
			t.Fatal(err)
		}
		parityProgram(t, proposal.Path)
		readme := filepath.Join(proposal.Path, ".sdo/playbooks/service-targetport-mismatch/README.md")
		if err := os.WriteFile(readme, []byte("---\nschema_version: 1\nowner: responder\nfault_class: wrong-class\noriginating_incident: incident\n---\n\nCheck the front target port against selected pods.\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return repository, root, proposal, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	}
	noCapture := validate.WithCaptureSource(ctx, "", "")
	t.Run("gaps commit", func(t *testing.T) {
		testutil.FakeGo(t)
		repository, root, _, decision := setup(t)
		receipt, err := repository.PublishReflection(noCapture, "incident", decision)
		if err != nil {
			t.Fatalf("a reflection with only coverage gaps was refused: %v", err)
		}
		recorded := strings.Join(gapKeys(receipt.CoverageGaps), "\n")
		for _, want := range []string{validate.RuleReflectionProgramRequired + ": verified cause 1", validate.RuleDetectorHealthyBaseline + ": controller pre-incident healthy capture is unavailable"} {
			if !strings.Contains(recorded, want) {
				t.Errorf("gap %q not recorded: %v", want, receipt.CoverageGaps)
			}
		}
		message, err := exec.Command("git", "-C", root, "log", "-1", "--format=%B", receipt.Commit).Output()
		if err != nil || !strings.Contains(string(message), "SDO-Coverage-Gap: "+validate.RuleReflectionProgramRequired) {
			t.Fatalf("accepted commit lacks the gap trailer: %v %s", err, message)
		}
		readme, err := exec.Command("git", "-C", root, "show", receipt.Commit+":.sdo/playbooks/service-targetport-mismatch/README.md").Output()
		if err != nil || !strings.Contains(string(readme), "fault_class: service-targetport-mismatch\n") {
			t.Fatalf("fault_class was not derived from the playbook ID: %v %s", err, readme)
		}
		index, err := exec.Command("git", "-C", root, "show", receipt.Commit+":.sdo/playbooks/README.md").Output()
		if err != nil || !strings.Contains(string(index), "(service-targetport-mismatch/README.md)") {
			t.Fatalf("playbook index was not generated: %v %s", err, index)
		}
	})
	for name, test := range map[string]struct {
		path, content, reason string
		fail                  testutil.GoRule
	}{
		"failing detector test still rejects": {".sdo/diagnostics/detectors/incidents/cache/detector_test.go", "package detector\n", validate.RuleDetectorTest,
			testutil.GoRule{Verb: "test", Covers: "detectors/incidents/cache", Marker: "detectors/incidents/cache/detector_test.go", Output: "--- FAIL: TestReject\n    sentinel detector failure"}},
		"invalid code still rejects": {".sdo/playbooks/service-targetport-mismatch/mitigate.go", "package targetport\n", validate.RuleDetectorCompile,
			testutil.GoRule{Verb: "build", Covers: "playbooks/service-targetport-mismatch", Marker: "playbooks/service-targetport-mismatch/mitigate.go", Output: "playbooks/service-targetport-mismatch/mitigate.go:2:12: undefined: UndefinedType"}},
	} {
		t.Run(name, func(t *testing.T) {
			testutil.FakeGo(t, test.fail)
			repository, _, proposal, decision := setup(t)
			if err := os.WriteFile(filepath.Join(proposal.Path, test.path), []byte(test.content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := repository.PublishReflection(noCapture, "incident", decision)
			var failure *validate.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("invalid code was accepted: %v", err)
			}
			found := false
			for _, d := range failure.Diagnostics {
				found = found || d.Rule == test.reason
			}
			if !found {
				t.Fatalf("want %s, got %v", test.reason, failure.Diagnostics)
			}
		})
	}
}

// gapKeys is the sorted "rule: message" form shared by the commit trailers.
func gapKeys(gaps []validate.Diagnostic) []string {
	keys := make([]string, 0, len(gaps))
	for _, gap := range gaps {
		keys = append(keys, strings.Join(strings.Fields(gap.Rule+": "+gap.Message), " "))
	}
	sort.Strings(keys)
	return keys
}
