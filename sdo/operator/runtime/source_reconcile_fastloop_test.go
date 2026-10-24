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

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/sourceimage"
	"sdo.dev/operator/sdk/sdktest"
)

type sourceImageManifests struct{ objects []*unstructured.Unstructured }

func (s sourceImageManifests) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	for _, object := range s.objects {
		if samePlanRef(object, ref) {
			return object.DeepCopy(), nil
		}
	}
	return nil, ErrAcceptedManifestAbsent
}
func (s sourceImageManifests) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	return s.AcceptedManifest(ctx, revision, ref)
}

func TestHermeticSourceReconcileLearningAndTrustAtThreeSkipsResponder(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	client := fastloopClient("good:v1")
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	if _, err := client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deployment, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}
	if _, err := client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	deployment.APIVersion, deployment.Kind = ref.APIVersion, ref.Kind
	sourceRaw, _ := json.Marshal(deployment)
	var source unstructured.Unstructured
	if err := json.Unmarshal(sourceRaw, &source); err != nil {
		t.Fatal(err)
	}
	source.SetAPIVersion(ref.APIVersion)
	source.SetKind(ref.Kind)
	source.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "good:v1"
	sourceReader := sourceImageManifests{objects: []*unstructured.Unstructured{&source}}
	provider := fastloopObjects{client: client}
	before, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open, err := sdktest.CaptureSnapshot(before, "app")
	if err != nil {
		t.Fatal(err)
	}
	peer := open.DeploymentList[0].DeepCopy()
	peer.Name = "peer"
	peer.Spec.Template.Spec.Containers[0].Image = "good:v1"
	open.DeploymentList = append(open.DeploymentList, *peer)
	open.Objects = sdk.TypedObjects(open)
	peerSource := source.DeepCopy()
	peerSource.SetName("peer")
	sourceReader.objects = append(sourceReader.objects, peerSource)
	raw, _ := json.Marshal(open)
	var fixed sdktest.Snapshot
	_ = json.Unmarshal(raw, &fixed)
	fixed.DeploymentList[0].Spec.Template.Spec.Containers[0].Image = "good:v1"
	fixed.Objects = sdk.TypedObjects(fixed)
	p := sourceimage.Program{}
	corpus := sdktest.CorpusCase{Open: open, Fixed: fixed, Healthy: fixed, Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}}
	for _, phase := range []string{"open", "fixed"} {
		snapshot := open
		if phase == "fixed" {
			snapshot = fixed
		}
		recorder := &sdktest.RecordingReadView{ReadView: LiveReadView{DetectionContext: snapshot, Source: sourceReader, SourceCommit: "before-learning"}}
		for _, object := range []sdk.ObjectRef{ref, {APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: "peer"}} {
			if _, err := recorder.DiffAgainstSource(object); err != nil {
				t.Fatal(err)
			}
		}
		for _, call := range recorder.Calls {
			call.Phase = phase
			corpus.Recordings = append(corpus.Recordings, call)
		}
	}
	if err := sdktest.CheckMitigationRepairs(p, p, corpus); err != nil {
		t.Fatal(err)
	}
	// The introduction uses handwritten captures and broker acceptance trailers.
	programDir := filepath.Join(root, ".sdo/playbooks", p.Spec().ID)
	if err := os.MkdirAll(programDir, 0755); err != nil {
		t.Fatal(err)
	}
	code, err := os.ReadFile("../sdk/program/testdata/sourceimage/program.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(programDir, "diagnose.go"), code, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(programDir, "mitigate.go"), []byte("package sourceimage\n"), 0644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, ".sdo/corpus/image-origin")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	recordingsRaw, _ := json.Marshal(corpus.Recordings)
	openRaw, _ := json.Marshal(open)
	fixedRaw, _ := json.Marshal(fixed)
	labels, _ := json.Marshal(map[string]any{"bindings": corpus.Bindings, "peer_bindings": program.Bindings{Objects: program.ObjectBindings{"deployment": {APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: "peer"}}}, "root_cause_program_ids": []string{p.Spec().ID}, "provenance": sdktest.CorpusProvenance{IncidentID: "image-origin", OpenSHA256: fmt.Sprintf("%x", sha256.Sum256(openRaw)), FixedSHA256: fmt.Sprintf("%x", sha256.Sum256(fixedRaw))}})
	acceptedRaw, _ := json.Marshal(sourceReader.objects)
	for name, data := range map[string][]byte{"accepted.json": acceptedRaw, "open.json": openRaw, "fixed.json": fixedRaw, "healthy.json": fixedRaw, "recordings.json": recordingsRaw, "labels.json": labels} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	readme := "---\nschema_version: 1\nowner: responder\nfault_class: image-pull-failure\noriginating_incident: image-origin\n---\n\nRestore the accepted image of `<DEPLOYMENT>` after observing source contradiction and image-pull failure.\n"
	if err := os.WriteFile(filepath.Join(programDir, "README.md"), []byte(readme), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/README.md"), []byte("# Playbooks\n\n- [base](base/README.md)\n- [image-pull-failure](image-pull-failure/README.md)\n"), 0644); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	var notes []sdktest.MitigationReplayResult
	diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot, MitigationReplayNotes: &notes})
	if err != nil || len(diagnostics) != 0 {
		t.Fatalf("source program compile and generated replay gate: %v %+v", err, diagnostics)
	}
	if len(notes) != 1 || notes[0].Status != sdktest.MitigationReplayPassed {
		t.Fatalf("missing executed replay note: %+v", notes)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "learn source image\n\nSDO-Incident: image-origin\nSDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed")
	commit := nativeGit(t, root, "rev-parse", "HEAD")
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	makeLoop := func(request IncidentRequest) *ResponderLoop {
		loop := NewResponderLoop(request, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Source: sourceReader, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider}}, 5)
		loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
			return LiveReadView{DetectionContext: s, Source: sourceReader, SourceCommit: request.SourceCommit}
		}
		loop.DiagnosisSettleDelay = time.Millisecond
		loop.CorpusReplayPassed = true
		return loop
	}
	origin := &contracts.OutcomeRecord{IncidentId: "image-origin", Classification: "success", SourceCommit: "before-learning", Timestamps: &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(time.Unix(1, 0))}}
	outcomes := []memory.Outcome{{IncidentID: origin.IncidentId, Record: origin}}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.RepairPolicy = "recorded-actions"
	request.RepositoryWorktree = root
	request.SourceCommit = commit
	request.RepositoryBaseCommit = commit
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root, ProgramFastPath: true, ProgramTrustThreshold: 3, LoopFactory: makeLoop}
	for index := 0; index < 3; index++ {
		dispatcher.Outcomes = priorOutcomeReader{outcomes: outcomes}
		trust, err := dispatcher.programTrust(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		state := trust[p.Spec().ID]
		if state.VerifiedSuccesses != index || state.Trusted {
			t.Fatalf("before repeat %d trust %+v", index, state)
		}
		snapshot, err := provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		request.Findings, err = (fastloopDetector{}).Detect(ctx, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		request.IncidentID = fmt.Sprintf("image-adhoc-%d", index)
		loop := makeLoop(request)
		loop.InitialDiagnose(ctx)
		// Independently applied ad-hoc fix; live health plus equivalence must earn one success.
		deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
		if _, err := client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if healthy, err := (PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider}).Verify(ctx); err != nil || !healthy {
			t.Fatalf("independent live verification: %v %v", healthy, err)
		}
		loop.CreditEquivalentRepairs(ctx, []RepairActionReceipt{{ActionID: "image-fix", Success: true, Resources: []sdk.ObjectRef{ref}}, {ActionID: "other-fault", Success: true, Resources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "unrelated"}}}})
		if len(loop.PlanApplications()) != 1 {
			t.Fatalf("missing equivalent credit runs=%+v errors=%+v preview=%+v", loop.ProgramRuns(), loop.matchedPlanErrors, loop.matchedPlans)
		}
		record := equivalentRepairOutcome(t, loop)
		fillProgramRevisions(ctx, loop.runs, request.RepositoryWorktree, request.SourceCommit)
		record.Timestamps = &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(time.Now().Add(time.Duration(index) * time.Second))}
		outcomes = append(outcomes, memory.Outcome{IncidentID: record.IncidentId, Record: record})
		deployment.Spec.Template.Spec.Containers[0].Image = "missing:v1"
		if _, err := client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	dispatcher.Outcomes = priorOutcomeReader{outcomes: outcomes}
	trust, err := dispatcher.programTrust(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if state := trust[p.Spec().ID]; state.VerifiedSuccesses != 3 || state.Failures != 0 || !state.Trusted {
		t.Fatalf("three credited successes: %+v", state)
	}
	request.IncidentID = "image-fast-fourth"
	snapshot, _ := provider.Snapshot(ctx)
	request.Findings, err = (fastloopDetector{}).Detect(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request, err = dispatcher.PrepareDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(ctx, request)
	if err != nil || result.ResolutionPath != "fast_path" || result.Status != IncidentCompleted || result.Usage.GetInputTokens() != 0 || result.Usage.GetOutputTokens() != 0 {
		t.Fatalf("source fast path: %+v %v", result, err)
	}
	if len(result.PlanApplications) != 1 || len(result.PlanApplications[0].Result.ActionsApplied) != 1 {
		t.Fatalf("missing source application: %+v", result.PlanApplications)
	}
	if _, ok := result.PlanApplications[0].Result.ActionsApplied[0].(program.ReconcileFromSource); !ok {
		t.Fatalf("wrong action %T", result.PlanApplications[0].Result.ActionsApplied[0])
	}
	jobs, err := client.BatchV1().Jobs("app").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("responder dispatched: %+v %v", jobs, err)
	}
	repaired, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil || repaired.Spec.Template.Spec.Containers[0].Image != "good:v1" {
		t.Fatalf("image not reconciled: %+v %v", repaired, err)
	}

	request.IncidentID = "live-failure"
	deployment.Spec.Template.Spec.Containers[0].Image = "missing:v1"
	if _, err := client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	failedLoop := makeLoop(request)
	failedLoop.Trust = trust
	// A new post-apply finding is a cause-scoped failure. An unrelated
	// aggregate verifier failure alone must never revoke the image repair.
	failedLoop.Detectors = append(failedLoop.Detectors, newFindingAfterImageRepair{})
	failedAttempt := failedLoop.TryTrustedFastPath(ctx, 3)
	if !failedAttempt.Attempted || failedAttempt.Resolved || !failedAttempt.RolledBack {
		t.Fatalf("live failure did not roll back: %+v", failedAttempt)
	}
	failure := equivalentRepairOutcome(t, failedLoop)
	failure.Classification = "failure"
	failure.ProgramFailureSignals = failedLoop.FailureSignals()
	failure.Timestamps = &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(time.Now().Add(time.Hour))}
	dispatcher.Outcomes = priorOutcomeReader{outcomes: append(outcomes, memory.Outcome{IncidentID: failure.IncidentId, Record: failure})}
	blocked, err := dispatcher.programTrust(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	state := blocked[p.Spec().ID]
	if state.Trusted || state.Failures != 1 || state.VerifiedSuccesses != 0 {
		t.Fatalf("live failure did not block trust: %+v", state)
	}
	blockedLoop := makeLoop(request)
	blockedLoop.Trust = blocked
	if attempt := blockedLoop.TryTrustedFastPath(ctx, 3); attempt.Attempted || attempt.SkippedReason != FastPathSkipUntrustedProgram {
		t.Fatalf("failed revision used fast path: %+v", attempt)
	}
	t.Log("unreplayable plan accepted without origin credit; three live verified equivalent responder repairs => 3 successes, 0 failures; fourth image fault: ReconcileFromSource applied, independent health clear, zero responder Jobs/tokens")
}
