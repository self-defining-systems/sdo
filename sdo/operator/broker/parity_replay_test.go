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

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"

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
	command := exec.Command("go", "build", "-o", binary, "../cmd/sdo-operator")
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
	for _, mode := range []string{"unknown", "needs-judgement", "source-evidence-gap"} {
		t.Run(mode, func(t *testing.T) { checkUnobservableDetectorLocator(t, mode) })
	}
}

func checkUnobservableDetectorLocator(t *testing.T, mode string) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	if out, err := exec.Command("go", "build", "-o", binary, "../cmd/sdo-operator").CombinedOutput(); err != nil {
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
	testutil.InitGit(t, root)
	runGit(t, root, "config", "user.name", "Fixture")
	runGit(t, root, "config", "user.email", "fixture@example.test")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "accepted memory")
	repository, err := broker.New(root, t.TempDir(), broker.JobValidator(subprocessParityJob{binary: binary, operatorRoot: operatorRoot, schema: schema}))
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
	cliErr := cli.Run()
	var exit *exec.ExitError
	if !errors.As(cliErr, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("unobservable locator passed CLI or infrastructure failed: %v %s", cliErr, stderr.String())
	}
	report := new(contracts.MemoryCheckReport)
	if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
		t.Fatal(err)
	}
	_, publishErr := repository.PublishReflection(validate.WithCaptureSource(ctx, capture, "incident"), "incident", decision)
	var failure *validate.Failure
	if !errors.As(publishErr, &failure) {
		t.Fatalf("unobservable locator passed broker: %v", publishErr)
	}
	if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
		t.Fatalf("diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
	}
	if len(report.Diagnostics) != 2 || report.Diagnostics[0].Rule != validate.RuleDetectorTest || report.Diagnostics[1].Rule != validate.RuleProgramRepeatReachable || !strings.Contains(report.Diagnostics[0].Message, "detector.corpus-observation") {
		t.Fatalf("locator failure was not actionable: %v", report)
	}
	if mode == "needs-judgement" && !strings.Contains(report.Diagnostics[0].Message, "Which listener is intended?") {
		t.Fatalf("judgement context lost: %v", report)
	}
	if mode == "source-evidence-gap" && !strings.Contains(report.Diagnostics[0].Message, "trusted deployment image evidence missing") {
		t.Fatalf("source provenance gap was hidden: %v", report)
	}

}

func TestCLIAndBrokerRetainHealthyInputForProgramOnlyEdits(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	if out, err := exec.Command("go", "build", "-o", binary, "../cmd/sdo-operator").CombinedOutput(); err != nil {
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
	baseline := t.TempDir()
	put(t, baseline, "healthy.json", string(capturefixture.FromView(t, `{"namespace":"demo","services":[{"metadata":{"name":"healthy-only","namespace":"demo"},"spec":{"selector":{"app":"no-pods"},"ports":[{"port":80,"targetPort":8080}]}}]}`)), 0644)
	root := copyParityTree(t)
	testutil.InitGit(t, root)
	runGit(t, root, "config", "user.name", "Fixture")
	runGit(t, root, "config", "user.email", "fixture@example.test")
	parityProgram(t, root)
	source := independentParityRoutingDetector(t)
	source = strings.Replace(source, `"context"`, `"context"; "fmt"`, 1)
	source = strings.Replace(source, `for _, service := range s.Services() {`, `for _, service := range s.Services() { if service.Name == "healthy-only" { return nil, fmt.Errorf("healthy-only selected Pod observation unavailable") }`, 1)
	put(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", source, 0644)
	replaceParityText(t, root, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "  - base\n", "  - base\n  - service-targetport-mismatch\n")
	runGit(t, root, "add", "-f", ".sdo")
	runGit(t, root, "commit", "-qm", "accepted learned program and detector")
	repository, err := broker.New(root, t.TempDir(), broker.JobValidator(subprocessParityJob{binary: binary, operatorRoot: operatorRoot, schema: schema, baseline: baseline}))
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
	path := ".sdo/playbooks/service-targetport-mismatch/diagnose.go"
	data, err := os.ReadFile(filepath.Join(proposal.Path, path))
	if err != nil {
		t.Fatal(err)
	}
	put(t, proposal.Path, path, string(data)+"\n// Reviewed against fresh evidence.\n", 0644)
	capture := t.TempDir()
	raw, readErr := os.ReadFile(filepath.Join(baseline, "healthy.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	put(t, capture, "healthy/healthy.json", string(raw), 0644)
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	data, err = protojson.Marshal(&contracts.ReflectionResult{LearningDecision: decision.LearningDecision, ProposedArtifactIds: decision.PlaybookIds})
	if err != nil {
		t.Fatal(err)
	}
	decisionPath := filepath.Join(t.TempDir(), "decision.json")
	if err := os.WriteFile(decisionPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	cli := exec.Command(binary, "memory", "check", "--json", "--decision", decisionPath)
	cli.Dir = proposal.Path
	cli.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE="+baseline, "SDO_CAPTURED_CORPUS_DIR="+capture, "SDO_INCIDENT_ID=incident")
	var stdout, stderr bytes.Buffer
	cli.Stdout, cli.Stderr = &stdout, &stderr
	cliErr := cli.Run()
	var exit *exec.ExitError
	if !errors.As(cliErr, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("program edit dropped healthy input or infrastructure failed: %v %s", cliErr, stderr.String())
	}
	report := new(contracts.MemoryCheckReport)
	if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
		t.Fatal(err)
	}
	_, publishErr := repository.PublishReflection(validate.WithCaptureSource(ctx, capture, "incident"), "incident", decision)
	var failure *validate.Failure
	if !errors.As(publishErr, &failure) {
		t.Fatalf("broker dropped healthy replay input: %v", publishErr)
	}
	if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
		t.Fatalf("healthy input differs: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
	}
	if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != validate.RuleDetectorHealthyBaseline || !strings.Contains(report.Diagnostics[0].Message, "healthy-only") {
		t.Fatalf("healthy locator error omitted: %v", report)
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
