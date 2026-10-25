package broker_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

func TestCLIAndBrokerAgreeOnReflectionResultContracts(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	if out, err := exec.Command("go", "build", "-p", "8", "-o", binary, "../cmd/sdo-operator").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	gap := &contracts.SdkGap{FaultClass: "catalog", Primitive: "catalog read", Reason: "unavailable", ScratchScript: "inspect catalog"}
	for _, test := range []struct {
		name   string
		result *contracts.ReflectionResult
		reject bool
	}{
		{"sdk_gap without gaps", &contracts.ReflectionResult{LearningDecision: "sdk_gap", NoChangeReason: "unavailable"}, true},
		{"no_change with gaps", &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "unavailable", SdkGaps: []*contracts.SdkGap{gap}}, true},
		{"gap missing fields", &contracts.ReflectionResult{LearningDecision: "sdk_gap", NoChangeReason: "unavailable", SdkGaps: []*contracts.SdkGap{{Reason: "unavailable"}}}, true},
		{"sdk gap", &contracts.ReflectionResult{LearningDecision: "sdk_gap", NoChangeReason: "unavailable", SdkGaps: []*contracts.SdkGap{gap}}, false},
		{"no change", &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"}, false},
		{"deprecated result payload", &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered", ArtifactProposal: &contracts.OperationalArtifactProposal{}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := copyParityTree(t)
			runGit(t, root, "init", "-q")
			runGit(t, root, "config", "user.name", "Fixture")
			runGit(t, root, "config", "user.email", "fixture@example.test")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-qm", "accepted memory")
			repository, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { t.Fatal("no-op decision reached compiler"); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repository.RecordOutcome(context.Background(), "incident", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}); err != nil {
				t.Fatal(err)
			}
			proposal, err := repository.CreateProposalForPhase(context.Background(), "incident", broker.ReflectionPhase)
			if err != nil {
				t.Fatal(err)
			}
			data, err := protojson.Marshal(test.result)
			if err != nil {
				t.Fatal(err)
			}
			decisionPath := filepath.Join(t.TempDir(), "decision.json")
			if err := os.WriteFile(decisionPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			cli := exec.Command(binary, "memory", "check", "--json", "--decision", decisionPath)
			cli.Dir = proposal.Path
			cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR=", "SDO_INCIDENT_ID=incident")
			var stdout, stderr bytes.Buffer
			cli.Stdout, cli.Stderr = &stdout, &stderr
			cliErr := cli.Run()
			report := new(contracts.MemoryCheckReport)
			if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
				t.Fatalf("CLI report: %v: %v %s", cliErr, err, stderr.String())
			}
			decision := &contracts.ReflectionDecision{LearningDecision: test.result.LearningDecision, NoChangeReason: test.result.NoChangeReason, SdkGaps: test.result.SdkGaps}
			var publishErr error
			if test.result.GetArtifactProposal() != nil {
				put(t, proposal.Path, ".git/"+validate.DecisionFile, string(data), 0600)
				_, publishErr = repository.Publish(context.Background(), "incident", broker.Responder, broker.ReflectionPhase)
			} else {
				_, publishErr = repository.PublishReflection(context.Background(), "incident", decision)
			}
			var diagnostics []validate.Diagnostic
			if publishErr != nil {
				var failure *validate.Failure
				if !errors.As(publishErr, &failure) {
					t.Fatalf("infrastructure error: %v", publishErr)
				}
				diagnostics = failure.Diagnostics
			}
			if (cliErr != nil) != test.reject || (publishErr != nil) != test.reject {
				t.Fatalf("verdict mismatch: CLI %v broker %v", cliErr, publishErr)
			}
			if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(diagnostics)}) {
				t.Fatalf("diagnostics mismatch: CLI %v broker %v", report.Diagnostics, diagnostics)
			}
			if test.reject && (len(diagnostics) != 1 || diagnostics[0].Rule != validate.RuleReflectionDecision) {
				t.Fatalf("SDK-gap contract diagnostic lost: %v", diagnostics)
			}
		})
	}
}
