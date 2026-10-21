package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func learningTrustRepository(t *testing.T) (string, string) {
	t.Helper()
	root := nativeRepository(t)
	files := map[string]string{
		".sdo/playbooks/service-targetport-mismatch/diagnose.go": "package targetport\n",
		".sdo/playbooks/service-targetport-mismatch/mitigate.go": "package targetport\n",
		".sdo/corpus/learning/open.json":                         `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":9999}]}}]}`,
		".sdo/corpus/learning/fixed.json":                        `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":8080}]}}]}`,
	}
	files[".sdo/corpus/learning/healthy.json"] = files[".sdo/corpus/learning/fixed.json"]
	files[".sdo/corpus/learning/recordings.json"] = "[]"
	labels := map[string]any{"bindings": program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}, "root_cause_program_ids": []string{"service-targetport-mismatch"}, "provenance": map[string]string{
		"incident_id": "learning", "open_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/open.json"]))),
		"fixed_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/fixed.json"]))),
	}}
	data, _ := json.Marshal(labels)
	files[".sdo/corpus/learning/labels.json"] = string(data)
	for name, content := range files {
		full := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "learn\n\nSDO-Incident: learning\nSDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed")
	return root, nativeGit(t, root, "rev-parse", "HEAD")
}

func TestLearningRepairAtThresholdOneDispatchesFastPathWithoutResponder(t *testing.T) {
	root, commit := learningTrustRepository(t)
	loop, client := trustedPortLoop(t, false, false, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID, finding.Severity = "drift.service-targetport-mismatch", sdk.SeverityLatent
	finding.ParameterBindings = map[string]sdk.ObjectRef{"service": finding.PrimaryResource}
	request := goldenRequest(t)
	request.Namespace, request.SourceCommit, request.RepositoryBaseCommit = "app", commit, commit
	request.RepairPolicy, request.RepositoryWorktree = "recorded-actions", root
	request.Findings = []sdk.Finding{finding}
	outcomes := []memory.Outcome{{IncidentID: "learning", Record: &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success", SourceCommit: "before-learning", ProgramFailureSignals: []string{"adhoc_coverage_gap"}}}}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root,
		ProgramFastPath: true, ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: outcomes},
		LoopFactory: func(request IncidentRequest) *ResponderLoop { loop.Request = request; return loop }}
	prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	trust, err := dispatcher.programTrust(context.Background(), request)
	if err != nil || !trust["service-targetport-mismatch"].Trusted || trust["service-targetport-mismatch"].VerifiedSuccesses != 1 {
		t.Fatalf("validated learning replay earned no bound credit: %+v %v", trust, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := dispatcher.Dispatch(ctx, prepared)
	if err != nil || result.ResolutionPath != "fast_path" || result.Usage.GetInputTokens() != 0 || result.Usage.GetOutputTokens() != 0 {
		t.Fatalf("learning repeat: %+v %v", result, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("repeat launched responder: %+v %v", jobs, err)
	}
	dispatcher.ProgramTrustThreshold = 0
	trust, err = dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].Trusted || trust["service-targetport-mismatch"].VerifiedSuccesses != 1 {
		t.Fatalf("default threshold changed: %+v %v", trust, err)
	}
	defaultLoop, _ := trustedPortLoop(t, false, false, true)
	defaultLoop.Request = request
	defaultLoop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	defaultLoop.Trust = trust
	attempt := defaultLoop.TryTrustedFastPath(context.Background(), 3)
	if attempt.Attempted || attempt.Resolved || attempt.SkippedReason != FastPathSkipUntrustedProgram {
		t.Fatalf("first repeat used default-threshold fast path: %+v", attempt)
	}
}

func TestLearningCreditRequiresSuccessfulOutcomeAndOriginalProgramRevision(t *testing.T) {
	root, commit := learningTrustRepository(t)
	record := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "failure"}
	dispatcher := KubernetesJobDispatcher{ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: record}}}}
	replayLoop, _ := trustedPortLoop(t, false, false, true)
	dispatcher.LoopFactory = func(IncidentRequest) *ResponderLoop { return replayLoop }

	request := IncidentRequest{RepositoryWorktree: root, SourceCommit: commit}
	trust, err := dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].Trusted {
		t.Fatalf("failed learning earned trust: %+v %v", trust, err)
	}
	record.Classification = "success"
	record.ProgramFailureSignals = []string{"fast_path_failure:service-targetport-mismatch"}
	trust, err = dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].Trusted {
		t.Fatalf("program failure earned learning trust: %+v %v", trust, err)
	}
	record.ProgramFailureSignals = nil
	path := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")
	if err := os.WriteFile(path, []byte("package targetport\n// changed mitigation\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "change program")
	request.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
	trust, err = dispatcher.programTrust(context.Background(), request)
	if err != nil || trust["service-targetport-mismatch"].Trusted {
		t.Fatalf("new revision inherited learning trust: %+v %v", trust, err)
	}
}

func TestLearningEvidenceRejectsUnvalidatedSyntheticAndMismatchedCaptures(t *testing.T) {
	for _, test := range []struct {
		name    string
		edit    func(map[string]any)
		message string
	}{
		{name: "synthetic", edit: func(labels map[string]any) { labels["synthetic_no_match_reason"] = "synthetic" }},
		{name: "bad-hash", edit: func(labels map[string]any) { labels["provenance"].(map[string]any)["fixed_sha256"] = "wrong" }},
		{name: "wrong-incident", edit: func(labels map[string]any) { labels["provenance"].(map[string]any)["incident_id"] = "other" }},
		{name: "unvalidated", message: "unvalidated change"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, commit := learningTrustRepository(t)
			// Rewrite the introducing commit so rejection exercises its original
			// acceptance evidence rather than an edit to an existing program.
			labelPath := filepath.Join(root, ".sdo/corpus/learning/labels.json")
			raw, err := os.ReadFile(labelPath)
			if err != nil {
				t.Fatal(err)
			}
			var labels map[string]any
			if err := json.Unmarshal(raw, &labels); err != nil {
				t.Fatal(err)
			}
			if test.edit != nil {
				test.edit(labels)
			}
			raw, err = json.Marshal(labels)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(labelPath, raw, 0644); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, root, "add", ".")
			args := []string{"commit", "--amend", "--no-edit"}
			if test.message != "" {
				args = []string{"commit", "--amend", "-qm", test.message}
			}
			nativeGit(t, root, args...)
			commit = nativeGit(t, root, "rev-parse", "HEAD")
			evidence, err := memory.ReadProgramLearningEvidence(context.Background(), root, commit)
			if err != nil || len(evidence) != 0 {
				t.Fatalf("unsafe learning evidence: %+v %v", evidence, err)
			}
		})
	}
}

func TestLearningCreditCannotEraseSubsequentFastPathFailure(t *testing.T) {
	root, commit := learningTrustRepository(t)
	revision, err := programDependencyRevision(context.Background(), root, commit, "service-targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	learning := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success"}
	failure := &contracts.OutcomeRecord{IncidentId: "repeat", Classification: "failure", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "service-targetport-mismatch", ProgramRevision: revision}}, ProgramFailureSignals: []string{"fast_path_failure:service-targetport-mismatch"}}
	dispatcher := KubernetesJobDispatcher{ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: learning}, {IncidentID: "repeat", Record: failure}}}}
	replayLoop, _ := trustedPortLoop(t, false, false, true)
	dispatcher.LoopFactory = func(IncidentRequest) *ResponderLoop { return replayLoop }

	trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: root, SourceCommit: commit})
	state := trust["service-targetport-mismatch"]
	if err != nil || state.Trusted || state.Failures != 1 || state.VerifiedSuccesses != 0 {
		t.Fatalf("learning erased failed repair: %+v %v", state, err)
	}
	if len(learning.ProgramRuns) != 0 || len(learning.PlanApplications) != 0 {
		t.Fatal("trust projection rewrote immutable learning outcome")
	}
}

type learningReplayDiagnoser struct{ fixed program.Result }

func (p learningReplayDiagnoser) Spec() program.ProgramSpec { return (fastloopPortProgram{}).Spec() }
func (p learningReplayDiagnoser) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, service := range view.Services() {
		if service.Name == bindings["service"].Name && service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: p.fixed}, nil
}
func TestLearningCreditRequiresCapturedBoundFixedNoMatch(t *testing.T) {
	for _, fixed := range []program.Result{program.Unknown, program.Match, program.NoMatch} {
		t.Run(string(fixed), func(t *testing.T) {
			root, commit := learningTrustRepository(t)
			p := learningReplayDiagnoser{fixed: fixed}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, nil)
			if err != nil {
				t.Fatal(err)
			}
			dispatcher := KubernetesJobDispatcher{ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success"}}}}, LoopFactory: func(request IncidentRequest) *ResponderLoop {
				return NewResponderLoop(request, registry, nil, nil, PlanExecutor{}, 1)
			}}
			trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: root, SourceCommit: commit})
			if err != nil {
				t.Fatal(err)
			}
			state := trust[p.Spec().ID]
			if state.Trusted != (fixed == program.NoMatch) {
				t.Fatalf("fixed %s credit %+v", fixed, state)
			}
		})
	}
}
