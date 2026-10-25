package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
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
	"sdo.dev/operator/sdk/sdktest"
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

func TestCLIAndBrokerAgreeOnUnobservableDetectorLocator(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	if out, err := exec.Command("go", "build", "-p", "8", "-o", binary, "../cmd/sdo-operator").CombinedOutput(); err != nil {
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
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing", Resources: []*contracts.ObjectRef{{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}}}}}
	if _, err := repository.RecordOutcome(ctx, "incident", outcome); err != nil {
		t.Fatal(err)
	}
	proposal, err := repository.CreateProposalForPhase(ctx, "incident", broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	parityProgram(t, proposal.Path)
	parityLabel(t, proposal.Path, func(label map[string]any) { delete(label, "provenance") })
	for _, phase := range []string{"open", "fixed", "healthy"} {
		path := filepath.Join(proposal.Path, ".sdo/corpus/targetport-mismatch", phase+".json")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot sdktest.Snapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		extra := snapshot.ServiceList[0].DeepCopy()
		extra.Name = "unlabelled-peer"
		extra.Spec.Selector = map[string]string{"app": "no-pods"}
		snapshot.ServiceList = append(snapshot.ServiceList, *extra)
		raw, err = json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		put(t, proposal.Path, ".sdo/corpus/targetport-mismatch/"+phase+".json", string(raw), 0644)
	}
	detector := parityDetector(t, "cache")
	detector = strings.Replace(detector, "import (", "import (\n targetport \"fixture-diagnostics/playbooks/service-targetport-mismatch\"\n \"sdo.dev/operator/sdk/program\"", 1)
	detector = strings.Replace(detector, `func New() sdk.Detector { return Detector{} }`, `func New() sdk.Detector {
 return program.DetectorFromProgram(targetport.Program{}, func(view sdk.DetectionContext) []program.Bindings {
  var result []program.Bindings
  for _, service := range view.Services() {
   result = append(result, program.Bindings{Objects:program.ObjectBindings{"service": {APIVersion:"v1", Kind:"Service", Namespace:service.Namespace, Name:service.Name}}})
  }
  return result
 }, Detector{}.Spec())
}`, 1)
	detector = strings.Replace(detector, `[]string{"base"}`, `[]string{"base", "service-targetport-mismatch"}`, 1)
	put(t, proposal.Path, ".sdo/diagnostics/detectors/incidents/cache/detector.go", detector, 0644)
	replaceParityText(t, proposal.Path, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "  - base\n", "  - base\n  - service-targetport-mismatch\n")
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	data, err := protojson.Marshal(&contracts.ReflectionResult{LearningDecision: decision.LearningDecision, ProposedArtifactIds: decision.PlaybookIds})
	if err != nil {
		t.Fatal(err)
	}
	decisionPath := filepath.Join(t.TempDir(), "decision.json")
	if err := os.WriteFile(decisionPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	cli := exec.Command(binary, "memory", "check", "--json", "--decision", decisionPath)
	cli.Dir = proposal.Path
	cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR=", "SDO_INCIDENT_ID=incident")
	var stdout, stderr bytes.Buffer
	cli.Stdout, cli.Stderr = &stdout, &stderr
	cliErr := cli.Run()
	var exit *exec.ExitError
	if !errors.As(cliErr, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("unobservable locator passed CLI or infrastructure failed: %v %s", cliErr, stderr.String())
	}
	report := new(contracts.MemoryCheckReport)
	if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
		t.Fatal(err)
	}
	_, publishErr := repository.PublishReflection(ctx, "incident", decision)
	var failure *validate.Failure
	if !errors.As(publishErr, &failure) {
		t.Fatalf("unobservable locator passed broker: %v", publishErr)
	}
	if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
		t.Fatalf("diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
	}
	if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != validate.RuleDetectorTest || !strings.Contains(report.Diagnostics[0].Message, "detector.corpus-observation") {
		t.Fatalf("locator failure was not actionable: %v", report)
	}
}
