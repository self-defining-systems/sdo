package broker_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// Recorded-style regression: one cause already has replay coverage; another
// verified cause does not. Both acceptance paths must inspect the same committed
// outcome, not just successfully compile the one authored program.
func TestCLIAndBrokerAgreeOnVerifiedCauseReplayCoverage(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	command := exec.Command("go", "build", "-p", "8", "-o", binary, "../cmd/sdo-operator")
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := builder.SchemaIdentity(operatorRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, classification := range []string{"success", "failed"} {
		t.Run(classification, func(t *testing.T) {
			root := copyParityTree(t)
			runGit(t, root, "init", "-q")
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
			outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: classification,
				ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing", Resources: []*contracts.ObjectRef{covered}}, {Summary: "catalog", Resources: []*contracts.ObjectRef{missing}}},
			}
			if classification == "failed" {
				// Only independently verified causes require learning on failed closures.
				outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "unverified", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "unverified"}}})
				outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: []*contracts.ObjectRef{covered}}, {CauseIndex: 1, Resources: []*contracts.ObjectRef{missing}}}}
			}
			if _, err := repository.RecordOutcome(ctx, "incident", outcome); err != nil {
				t.Fatal(err)
			}
			proposal, err := repository.CreateProposalForPhase(ctx, "incident", broker.ReflectionPhase)
			if err != nil {
				t.Fatal(err)
			}
			parityProgram(t, proposal.Path)
			// The existing detector now references the authored replay-backed program.
			replaceParityText(t, proposal.Path, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "  - base\n", "  - base\n  - service-targetport-mismatch\n")
			decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
			result := &contracts.ReflectionResult{LearningDecision: decision.LearningDecision, ProposedArtifactIds: decision.PlaybookIds}
			check := func() *contracts.MemoryCheckReport {
				t.Helper()
				data, err := protojson.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				decisionFile := filepath.Join(t.TempDir(), "decision.json")
				if err := os.WriteFile(decisionFile, data, 0600); err != nil {
					t.Fatal(err)
				}
				cli := exec.Command(binary, "memory", "check", "--json", "--decision", decisionFile)
				cli.Dir = proposal.Path
				cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR=", "SDO_INCIDENT_ID=incident")
				var stdout, stderr bytes.Buffer
				cli.Stdout, cli.Stderr = &stdout, &stderr
				cliErr := cli.Run()
				report := new(contracts.MemoryCheckReport)
				if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
					t.Fatalf("CLI: %v: %v: %s", cliErr, err, stderr.String())
				}
				_, publishErr := repository.PublishReflection(ctx, "incident", decision)
				var failure *validate.Failure
				if !errors.As(publishErr, &failure) {
					t.Fatalf("want shared content rejection: %v", publishErr)
				}
				if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
					t.Fatalf("CLI/broker diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
				}
				return report
			}
			report := check()
			if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != validate.RuleReflectionProgramRequired || !strings.Contains(report.Diagnostics[0].Message, "cause 1") {
				t.Fatalf("uncovered verified cause was omitted or unverified cause included: %v", report)
			}
		})
	}
}
