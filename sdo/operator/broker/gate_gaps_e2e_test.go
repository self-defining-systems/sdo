package broker_test

import (
	"bytes"
	"context"
	"errors"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/validate"
)

// A reflection that leaves out learning the memory edge can live without is
// accepted and records what it left out; the checks that protect memory quality
// (detector tests, mitigations that repair) still reject.
func TestReflectionCommitsWithRecordedGapsAndStillRejectsBrokenLearning(t *testing.T) {
	ctx := context.Background()
	binary := operatorCLIFixture(t)
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := builder.SchemaIdentity(operatorRoot)
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
		repository, err := broker.New(root, t.TempDir(), broker.JobValidator(subprocessParityJob{binary: binary, operatorRoot: operatorRoot, schema: schema}))
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
	t.Run("repeat-unreachable gap is the same on the CLI and the broker", func(t *testing.T) {
		repository, _, proposal, decision := setup(t)
		path := filepath.Join(proposal.Path, ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "if faulty {") {
			t.Fatal("fixture detector changed")
		}
		// The detector never fires, so no finding selects the playbook.
		if err := os.WriteFile(path, []byte(strings.Replace(string(data), "if faulty {", `if faulty && service.Name == "" {`, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		result := &contracts.ReflectionResult{LearningDecision: decision.LearningDecision, ProposedArtifactIds: decision.PlaybookIds}
		raw, err := protojson.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		decisionFile := filepath.Join(t.TempDir(), "decision.json")
		if err := os.WriteFile(decisionFile, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		cli := exec.Command(binary, "memory", "check", "--json", "--decision", decisionFile)
		cli.Dir = proposal.Path
		cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR=", "SDO_INCIDENT_ID=incident")
		var stdout, stderr bytes.Buffer
		cli.Stdout, cli.Stderr = &stdout, &stderr
		if err := cli.Run(); err != nil {
			t.Fatalf("an unreachable repeat must not fail the check: %v: %s", err, stderr.String())
		}
		report := new(contracts.MemoryCheckReport)
		if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
			t.Fatal(err)
		}
		var cliGaps []validate.Diagnostic
		for _, gap := range report.CoverageGaps {
			cliGaps = append(cliGaps, validate.Diagnostic{Rule: gap.Rule, Message: gap.Message})
		}
		receipt, err := repository.PublishReflection(noCapture, "incident", decision)
		if err != nil {
			t.Fatalf("an unreachable repeat was refused: %v", err)
		}
		want, got := gapKeys(cliGaps), gapKeys(receipt.CoverageGaps)
		if !strings.Contains(strings.Join(want, "\n"), validate.RuleProgramRepeatReachable+":") {
			t.Fatalf("CLI did not record the unreachable repeat: %v", want)
		}
		if strings.Join(want, "\n") != strings.Join(got, "\n") {
			t.Fatalf("CLI and broker disagree on coverage gaps:\nCLI    %v\nbroker %v", want, got)
		}
	})
	for name, test := range map[string]struct {
		path, content, rule string
	}{
		"failing detector test still rejects": {".sdo/diagnostics/detectors/incidents/cache/detector_test.go", "package detector\nimport \"testing\"\nfunc TestReject(t *testing.T){t.Fatal(\"sentinel detector failure\")}\n", validate.RuleDetectorTest},
		"mitigation that does not repair still rejects": {".sdo/playbooks/service-targetport-mismatch/mitigate.go", `package targetport
import ("context"; "sdo.dev/operator/sdk/program")
func (p Program) Mitigate(ctx context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){
 verdict,err:=p.Diagnose(ctx,v,b);if err!=nil || verdict.Result!=program.Match{return program.Plan{},err}
 return program.Plan{Actions:[]program.Action{program.SetField{Ref:b.Objects["service"],Path:"spec.ports[0].targetPort",Value:program.FromObserved(b.Objects["service"],"spec.ports[0].targetPort")}}},nil
}
`, validate.RuleProgramMitigationRepair},
	} {
		t.Run(name, func(t *testing.T) {
			repository, _, proposal, decision := setup(t)
			formatted, err := format.Source([]byte(test.content))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(proposal.Path, test.path), formatted, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err = repository.PublishReflection(noCapture, "incident", decision)
			var failure *validate.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("broken learning was accepted: %v", err)
			}
			found := false
			for _, d := range failure.Diagnostics {
				found = found || d.Rule == test.rule
			}
			if !found {
				t.Fatalf("want %s, got %v", test.rule, failure.Diagnostics)
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
