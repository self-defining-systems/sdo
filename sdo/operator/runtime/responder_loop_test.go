package runtime

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponderLoopApplyReturnsLogicalPlaybooksAndBoundedEvidence(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref, Evidence: strings.Repeat("observation", 3000), Playbooks: []string{"health-objective"}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", Findings: []sdk.Finding{finding}}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{finding: finding}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if len(response.Findings) != 1 || response.Findings[0].Playbooks[0] != "health-objective" {
		t.Fatalf("physical playbook in tool response: %+v", response.Findings)
	}
	call := loop.ToolCalls()[0]
	if strings.Contains(call.ResponseJSON, ".sdo/") || len(call.ResponseJSON) > 2048 {
		t.Fatalf("unbounded or physical tool evidence: %d bytes", len(call.ResponseJSON))
	}
	var recorded map[string]any
	if err := json.Unmarshal([]byte(call.ResponseJSON), &recorded); err != nil || recorded["truncated"] != true {
		t.Fatalf("missing structured truncation marker: %s, %v", call.ResponseJSON, err)
	}
}

func TestResponderLoopStatusAndDiagnoseUseFreshFindings(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref}
	active := false
	detector := changingLoopDetector{find: func() []sdk.Finding {
		if active {
			return []sdk.Finding{finding}
		}
		return nil
	}}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{detector}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	logs := captureSlog(t)
	active = true
	status := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(status.Findings) != 1 || len(status.ProgramRuns) != 1 || status.ProgramRuns[0].Verdict.Result != program.Match {
		t.Fatalf("stale status: %+v", status)
	}
	active = false
	diagnosis := loop.Handle(context.Background(), ToolRequest{Command: "diagnose"})
	if len(diagnosis.Findings) != 0 || len(diagnosis.ProgramRuns) != 0 {
		t.Fatalf("stale diagnosis: %+v", diagnosis)
	}
	for _, command := range []string{"status", "diagnose"} {
		if !strings.Contains(logs.String(), "msg=responder_tool_timing incident_id=i command="+command+" capture_ms=") {
			t.Fatalf("handler duration for %s is not logged:\n%s", command, logs.String())
		}
	}
}

type changingLoopDetector struct{ find func() []sdk.Finding }

type snapshotDiagnoser struct{}

type transientImageProgram struct{}

func (transientImageProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "health-objective-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (transientImageProgram) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, pod := range view.Pods() {
		if pod.Labels["app"] != bindings["deployment"].Name {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Waiting != nil && (status.State.Waiting.Reason == "ImagePullBackOff" || status.State.Waiting.Reason == "ErrImagePull") {
				return program.Verdict{Result: program.Match}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p transientImageProgram) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, view, bindings)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.SetField{Ref: bindings["deployment"], Path: "spec.template.spec.containers[0].image", Value: "search:healthy", Reason: "restore pullable image"}}}, nil
}

func TestResponderPlanUsesSettledMatchedImagePullView(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel-reservation", Name: "search"}
	finding := sdk.Finding{DetectorID: "deployment-image-pull-backoff", RuleID: "unavailable-image-pull-backoff", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"deployment": ref}, Playbooks: []string{"health-objective-failure"}}
	snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}}}}
	provider := &changingProvider{snapshot: snapshot}
	registry, err := program.NewRegistry([]program.Diagnoser{transientImageProgram{}}, []program.Mitigator{transientImageProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": "search:broken"}}}}})}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic-incident", Namespace: ref.Namespace, Findings: []sdk.Finding{finding}}, registry, provider, []sdk.Detector{loopDetector{finding: finding}}, PlanExecutor{Client: client}, 5)
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Millisecond
	if runs := loop.InitialDiagnose(context.Background()); len(runs) == 0 || runs[0].Verdict.Result != program.NoMatch {
		t.Fatalf("initial diagnosis: %+v", runs)
	}
	snapshot.PodList = []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "search-pod", Namespace: ref.Namespace, Labels: map[string]string{"app": "search"}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}}}
	provider.snapshot = snapshot
	loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if runs := loop.ProgramRuns(); runs[len(runs)-1].Verdict.Result != program.Match {
		t.Fatalf("settled diagnosis: %+v", runs)
	}
	snapshot.PodList = nil
	provider.snapshot = snapshot
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "health-objective-failure", Bindings: program.Bindings{"deployment": ref}})
	if planned.Error != "" || planned.PlanID == "" || len(planned.Patches) != 1 {
		t.Fatalf("plan after pod state changed: %+v", planned)
	}
	repairs := []RepairActionReceipt{{ActionID: "restore-search-image", Resources: []sdk.ObjectRef{ref}, Success: true}}
	client.objects[ref] = planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": "search:other"}}}}})
	loop.CreditEquivalentRepairs(context.Background(), repairs)
	if len(loop.PlanApplications()) != 0 {
		t.Fatalf("different image received trust credit: %+v", loop.PlanApplications())
	}
	client.objects[ref] = planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": "search:healthy"}}}}})
	client.objects[ref].SetAnnotations(map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{}"})
	loop.CreditEquivalentRepairs(context.Background(), repairs)
	applications := loop.PlanApplications()
	if len(applications) != 1 || !applications[0].Result.VerificationPassed || applications[0].ProgramIDs[0] != "health-objective-failure" {
		t.Fatalf("verified equivalent repair was not credited: %+v", applications)
	}
}

func TestResponderRestartPlansFromLiveViewInsteadOfCapturedMatch(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel-reservation", Name: "search"}
	broken := sdktest.Snapshot{NamespaceName: ref.Namespace, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "search-pod", Namespace: ref.Namespace, Labels: map[string]string{"app": "search"}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}}}}
	root := t.TempDir()
	if err := memory.CaptureIncidentSnapshot(root, "incident", "open@detect", broken, ref.Namespace); err != nil {
		t.Fatal(err)
	}
	recovered := broken
	recovered.PodList = nil
	registry, err := program.NewRegistry([]program.Diagnoser{transientImageProgram{}}, []program.Mitigator{transientImageProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": "search:broken"}}}}})}}
	request := IncidentRequest{IncidentID: "incident", Namespace: ref.Namespace, Findings: []sdk.Finding{{DetectorID: "deployment-image-pull-backoff", RuleID: "unavailable-image-pull-backoff", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"deployment": ref}, Playbooks: []string{"health-objective-failure"}}}}
	preparer := NewResponderLoop(request, registry, staticProvider{snapshot: recovered}, nil, PlanExecutor{Client: client}, 5)
	preparer.CorpusSessionRoot = root
	preparer.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	request.InitialProgramRuns = preparer.InitialDiagnose(context.Background())
	if len(request.InitialProgramRuns) == 0 || request.InitialProgramRuns[0].Verdict.Result != program.NoMatch {
		t.Fatalf("dispatch diagnosis reused captured image-pull state: %+v", request.InitialProgramRuns)
	}
	loop := NewResponderLoop(request, registry, staticProvider{snapshot: recovered}, nil, PlanExecutor{Client: client}, 5)
	loop.CorpusSessionRoot = root
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "health-objective-failure", Bindings: program.Bindings{"deployment": ref}})
	if planned.Error == "" || planned.PlanID != "" {
		t.Fatalf("stale captured diagnosis authorized a restart plan: %+v", planned)
	}
}

func (snapshotDiagnoser) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "fault", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (snapshotDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, _ = view.DiffAgainstBaseline(sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"})
	return program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Fact: "moment", Observed: view.Services()[0].Annotations["moment"]}}}, nil
}

func (changingLoopDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "health"} }
func (d changingLoopDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.find(), nil
}

type loopProgram struct {
	candidateProgram
	ref sdk.ObjectRef
}

type loopDetector struct{ finding sdk.Finding }

func (d loopDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "health"} }
func (d loopDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.finding.Status == sdk.FindingActive {
		return []sdk.Finding{d.finding}, nil
	}
	return nil, nil
}

func (p loopProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: p.ref, Path: "spec.ports[0].targetPort", Value: 8080, Reason: "repair target port"}}}, nil
}

func TestResponderLoopAppliesOneBatchThenRefusesExhaustedBudgetAndLogsCalls(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	p := loopProgram{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}, ref: ref}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{finding: sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref, Playbooks: []string{"health-objective"}}}}, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: false}}, 1)
	loop.Trust = map[string]ProgramTrust{"targetport": {VerifiedSuccesses: 2}}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	if status := loop.Handle(context.Background(), ToolRequest{Command: "status"}); status.Trust["targetport"].VerifiedSuccesses != 2 {
		t.Fatalf("status trust: %+v", status)
	}
	plan := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "targetport", Bindings: program.Bindings{"service": ref}})
	if plan.Error != "" || plan.PlanID == "" || len(plan.Patches) != 1 {
		t.Fatalf("plan: %+v", plan)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}})
	if applied.Error != "" || applied.ApplicationID == "" || len(applied.ProgramRuns) == 0 || applied.Healthy {
		t.Fatalf("apply: %+v", applied)
	}
	if len(applied.Findings) != 1 || applied.Findings[0].Playbooks[0] != "health-objective" || strings.Contains(loop.ToolCalls()[2].ResponseJSON, ".sdo/") {
		t.Fatalf("apply leaked physical playbook path: %+v", applied.Findings)
	}
	denied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}})
	if denied.Error == "" || !denied.Unresolved {
		t.Fatalf("budget: %+v", denied)
	}
	if len(loop.ToolCalls()) != 4 || len(loop.PlanApplications()) != 1 || len(loop.ProgramRuns()) == 0 {
		t.Fatalf("outcome evidence missing: calls=%d apps=%d runs=%d", len(loop.ToolCalls()), len(loop.PlanApplications()), len(loop.ProgramRuns()))
	}
	if signals := loop.FailureSignals(); len(signals) != 2 || !contains(signals, "applied_diagnosis_still_match:targetport") || !contains(signals, "application_verification_failure:targetport") {
		t.Fatalf("failure signal: %v", signals)
	}
}

func TestResponderLoopRecordsAdhocCoverageGapAfterVerification(t *testing.T) {
	request := IncidentRequest{IncidentID: "i", Namespace: "app", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}}
	loop := NewResponderLoop(request, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	verified := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if verified.Error != "" || !verified.Healthy {
		t.Fatalf("verify: %+v", verified)
	}
	if len(verified.ProgramRuns) != 0 {
		t.Fatalf("clean verify returned unrelated candidate sweep: %+v", verified.ProgramRuns)
	}
	if signals := loop.FailureSignals(); len(signals) != 1 || signals[0] != "adhoc_coverage_gap" {
		t.Fatalf("coverage signal: %v", signals)
	}
}

func TestCleanVerifyDoesNotSweepUnrelatedPrograms(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: snapshot}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	verified := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if !verified.Healthy || len(verified.ProgramRuns) != 0 || !strings.Contains(verified.Summary, "return the structured incident result immediately") {
		t.Fatalf("clean verify: %+v", verified)
	}
}

func TestColdLearningCapturesResponderSourceReads(t *testing.T) {
	source, commit := foreignSource(t)
	snapshot := policyWorkloadSnapshot()
	request := IncidentRequest{IncidentID: "cold-learning", Namespace: "app", SourceCommit: commit}
	loop := NewResponderLoop(request, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{Source: source}, 1)
	loop.CorpusSessionRoot = t.TempDir()
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: s, Source: source, SourceCommit: commit}
	}
	loop.InitialDiagnose(context.Background())
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if response := loop.Handle(context.Background(), ToolRequest{Command: "source", Ref: &ref}); response.Error != "" {
		t.Fatal(response.Error)
	}
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
	if _, err := view.DiffAgainstSource(ref); err != nil {
		t.Fatalf("cold source inspection was not replayable: %v", err)
	}
}

func TestResponderLoopLogsPhaseTimingsPerToolCall(t *testing.T) {
	var logged strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i-timing", Namespace: "app"}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	loop.Handle(context.Background(), ToolRequest{Command: "status"})
	lines := strings.Split(strings.TrimSpace(logged.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("want one timing line per tool call, got %q", logged.String())
	}
	for _, want := range []string{"msg=responder_tool_timing", "incident_id=i-timing", "command=status", "capture_ms=", "refresh_ms=", "persist_ms=", "total_ms="} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("timing line missing %q: %s", want, lines[0])
		}
	}
}
