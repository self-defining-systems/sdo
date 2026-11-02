//go:build integration

package broker_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/sdktest"
)

// Recorded-style regression: one cause already has replay coverage; another
// verified cause does not. Both acceptance paths must inspect the same committed
// outcome, not just successfully compile the one authored program.
func TestCLIAndBrokerAgreeOnVerifiedCauseReplayCoverage(t *testing.T) {
	ctx := context.Background()
	binary := operatorCLIFixture(t) // built with the real compiler, before the fake is installed
	// The coverage gap is a static reflection rule; both validators reach the
	// same rule over a synthetic compiler.
	testutil.FakeGo(t)
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, classification := range []string{"success", "failed"} {
		t.Run(classification, func(t *testing.T) {
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
			outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: classification,
				ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing", Resources: []*contracts.ObjectRef{covered}}, {Summary: "catalog", Resources: []*contracts.ObjectRef{missing}}},
			}
			if classification == "failed" {
				// Only independently verified causes require learning on failed closures.
				outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "unverified", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "unverified"}}})
				outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: []*contracts.ObjectRef{covered}}, {CauseIndex: 1, Resources: []*contracts.ObjectRef{missing}}}}
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
			// The learned detector is bound to the verified port repair; the
			// unrelated accepted cache detector retains its original playbook.
			if err := os.CopyFS(filepath.Join(proposal.Path, ".sdo/diagnostics/detectors/incidents/service-targetport-mismatch"), os.DirFS("../../../tests/fixtures/sdo/service_port_detector")); err != nil {
				t.Fatal(err)
			}
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
				if cliErr := cli.Run(); cliErr != nil {
					t.Fatalf("a coverage gap must not fail the check: %v: %s", cliErr, stderr.String())
				}
				report := new(contracts.MemoryCheckReport)
				if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
					t.Fatalf("CLI: %v: %s", err, stderr.String())
				}
				receipt, publishErr := repository.PublishReflection(validate.WithCaptureSource(ctx, "", ""), "incident", decision)
				if publishErr != nil {
					t.Fatalf("a coverage gap must not reject the reflection: %v", publishErr)
				}
				if len(receipt.CoverageGaps) == 0 || !strings.Contains(strings.Join(gapKeys(receipt.CoverageGaps), "\n"), validate.RuleReflectionProgramRequired+": verified cause 1") {
					t.Fatalf("broker receipt omitted the program gap: %v", receipt.CoverageGaps)
				}
				return report
			}
			report := check()
			if len(report.Diagnostics) != 0 || len(report.CoverageGaps) == 0 || report.CoverageGaps[0].Rule != validate.RuleReflectionProgramRequired || !strings.Contains(report.CoverageGaps[0].Message, "cause 1") {
				t.Fatalf("uncovered verified cause was refused or unrecorded: %v", report)
			}
		})
	}
}

func TestCLIAndBrokerAgreeOnUnobservableDetectorLocator(t *testing.T) {
	for _, mode := range []string{"unknown", "needs-judgement", "source-evidence-gap"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			checkUnobservableDetectorLocator(t, mode)
		})
	}
}

func checkUnobservableDetectorLocator(t *testing.T, mode string) {
	ctx := context.Background()
	binary := operatorCLIFixture(t)
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
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
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing", Resources: []*contracts.ObjectRef{{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}}}}}
	parityFindingRepairProof(outcome)
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
		raw = capturefixture.Recapture(t, raw, func(snapshot *sdktest.Snapshot) {
			extra := snapshot.ServiceList[0].DeepCopy()
			extra.Name = "unlabelled-peer"
			extra.Spec.Selector = map[string]string{"app": "no-pods"}
			snapshot.ServiceList = append(snapshot.ServiceList, *extra)
		})
		put(t, proposal.Path, ".sdo/corpus/targetport-mismatch/"+phase+".json", string(raw), 0644)
	}
	detector := independentParityRoutingDetector(t)
	behavior := `return nil, fmt.Errorf("selected Service observation unavailable")`
	if mode == "needs-judgement" {
		behavior = `return nil, fmt.Errorf("Which listener is intended?")`
	}
	if mode == "source-evidence-gap" {
		behavior = `_, err := program.ReadFrom(s).DiffAgainstSource(sdk.ObjectRef{APIVersion:"apps/v1", Kind:"Deployment", Namespace:"demo", Name:"catalog"}); return nil, err`
		recordings := `[`
		for index, phase := range []string{"open", "fixed", "healthy"} {
			if index > 0 {
				recordings += ","
			}
			recordings += `{"phase":"` + phase + `","method":"DiffAgainstSource","arguments":[{"api_version":"apps/v1","kind":"Deployment","namespace":"demo","name":"catalog"}],"error":"rendered deployment evidence unavailable: trusted deployment image evidence missing for catalog"}`
		}
		put(t, proposal.Path, ".sdo/corpus/targetport-mismatch/recordings.json", recordings+"]", 0644)
		detector = strings.Replace(detector, `"context"`, `"context"; "sdo.dev/operator/sdk/program"`, 1)
	} else {
		detector = strings.Replace(detector, `"context"`, `"context"; "fmt"`, 1)
	}
	detector = strings.Replace(detector, `for _, service := range s.Services() {`, `for _, service := range s.Services() { if service.Name == "unlabelled-peer" { `+behavior+` }`, 1)
	put(t, proposal.Path, ".sdo/diagnostics/detectors/incidents/cache/detector.go", detector, 0644)
	replaceParityText(t, proposal.Path, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "  - base\n", "  - service-targetport-mismatch\n")
	capture := t.TempDir()
	put(t, capture, "healthy/healthy.json", string(capturefixture.FromView(t, `{"namespace":"demo"}`)), 0644)
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
	cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR="+capture, "SDO_INCIDENT_ID=incident")
	var stdout, stderr bytes.Buffer
	cli.Stdout, cli.Stderr = &stdout, &stderr
	if err := cli.Run(); err != nil {
		t.Fatalf("unobservable replay blocked CLI: %v %s %s", err, stdout.String(), stderr.String())
	}
	report := new(contracts.MemoryCheckReport)
	if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
		t.Fatal(err)
	}
	receipt, err := repository.PublishReflection(validate.WithCaptureSource(ctx, capture, "incident"), "incident", decision)
	if err != nil {
		t.Fatalf("unobservable replay blocked broker: %v", err)
	}
	if len(report.Diagnostics) != 0 || len(report.CoverageGaps) == 0 {
		t.Fatalf("replay observation missing: %v", report)
	}
	cliKeys := gapKeys(reportGaps(report.CoverageGaps))
	brokerKeys := gapKeys(receipt.CoverageGaps)
	if strings.Join(cliKeys, "\n") != strings.Join(brokerKeys, "\n") {
		t.Fatalf("replay observations differ: CLI %v broker %v", cliKeys, brokerKeys)
	}
	observations := strings.Join(cliKeys, "\n")
	reason := map[string]string{"unknown": "selected Service observation unavailable", "needs-judgement": "Which listener is intended?", "source-evidence-gap": "trusted deployment image evidence missing"}[mode]
	if !strings.Contains(observations, reason) {
		t.Fatalf("replay reason lost: %s", observations)
	}

}

// Preserve the independently authored detection predicate and the accepted cache spec.
func independentParityRoutingDetector(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("../../../tests/fixtures/sdo/service_port_detector/detector.go")
	if err != nil {
		t.Fatal(err)
	}
	source := parityDetector(t, "cache")
	start := strings.Index(string(data), "func (Detector) Detect(")
	source = strings.Replace(source, "func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }", string(data[start:]), 1)
	source = strings.ReplaceAll(source, `DetectorID: "service-targetport-mismatch"`, `DetectorID: "cache"`)
	source = strings.ReplaceAll(source, `[]string{"base"}`, `[]string{"service-targetport-mismatch"}`)
	return source
}

func reportGaps(items []*contracts.MemoryDiagnostic) []validate.Diagnostic {
	result := make([]validate.Diagnostic, 0, len(items))
	for _, item := range items {
		result = append(result, validate.Diagnostic{Rule: item.Rule, Path: item.Path, Message: item.Message})
	}
	return result
}
