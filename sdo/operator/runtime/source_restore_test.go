package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// restoreView is a replay view with canned source comparisons.
type restoreView struct {
	*sdktest.ReplayReadView
	diffs map[string]sdk.SourceDiff
}

func (v restoreView) DiffAgainstSource(ref sdk.ObjectRef) (sdk.SourceDiff, error) {
	return v.diffs[ref.Kind+"/"+ref.Name], nil
}

func (v restoreView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}}}, nil
}

func newRestoreEvidence(request IncidentRequest) *sourceRestoreEvidence {
	evidence := &sourceRestoreEvidence{}
	evidence.set(request)
	return evidence
}

func restoreRequest(manager string) IncidentRequest {
	request := detectRequest()
	request.DriftEvents = []sdk.DriftEvent{departureEvent(dref("Service", "api"), manager, detectOnset.Add(-4*time.Second), "spec.ports")}
	return request
}

func restoreViewOf(diffs map[string]sdk.SourceDiff) restoreView {
	return restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: detectApp("create").Snapshot}, diffs: diffs}
}

func restoreAPI() program.Bindings {
	return program.Bindings{Values: map[string]any{SourceRestoreComponentParam: "api"}}
}

func TestSourceRestoreDiagnosesAndMitigatesANonSDODeparture(t *testing.T) {
	restore := sourceRestoreProgram{evidence: newRestoreEvidence(restoreRequest("kubectl"))}
	view := restoreViewOf(map[string]sdk.SourceDiff{"Service/api": {Changed: true}})
	verdict, err := restore.Diagnose(context.Background(), view, restoreAPI())
	if err != nil || verdict.Result != program.Match || len(verdict.Evidence) != 1 {
		t.Fatalf("verdict %+v, err %v", verdict, err)
	}
	plan, err := restore.Mitigate(context.Background(), view, restoreAPI())
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("plan %+v, err %v", plan, err)
	}
	if action, ok := plan.Actions[0].(program.ReconcileFromSource); !ok || action.Ref.Name != "api" {
		t.Fatalf("action %#v", plan.Actions[0])
	}
}

func TestSourceRestoreDoesNotMatchSDOsOwnWritesOrMatchingSource(t *testing.T) {
	view := restoreViewOf(map[string]sdk.SourceDiff{"Service/api": {Changed: true}})
	own := sourceRestoreProgram{evidence: newRestoreEvidence(restoreRequest(sdk.ResponderFieldManager))}
	if verdict, _ := own.Diagnose(context.Background(), view, restoreAPI()); verdict.Result != program.NoMatch {
		t.Fatalf("SDO's own recorded repair matched: %+v", verdict)
	}
	clean := sourceRestoreProgram{evidence: newRestoreEvidence(restoreRequest("kubectl"))}
	if verdict, _ := clean.Diagnose(context.Background(), restoreViewOf(nil), restoreAPI()); verdict.Result != program.NoMatch {
		t.Fatalf("an object equal to source matched: %+v", verdict)
	}
	deploying := restoreRequest("kubectl")
	deploying.DeploymentInFlight = true
	during := sourceRestoreProgram{evidence: newRestoreEvidence(deploying)}
	if verdict, _ := during.Diagnose(context.Background(), view, restoreAPI()); verdict.Result != program.NoMatch {
		t.Fatalf("a departure during a deployment matched: %+v", verdict)
	}
}

func TestSourceRestoreIsListedLikeAPlaybookAndNeverBoundByFindings(t *testing.T) {
	registry := withSourceRestore(nil, newRestoreEvidence(restoreRequest("kubectl")))
	if specs := registry.List(); len(specs) != 1 || specs[0].ID != SourceRestoreID {
		t.Fatalf("listed %v", specs)
	}
	if _, ok := registry.Mitigator(SourceRestoreID); !ok {
		t.Fatal("it must be mitigable like any playbook")
	}
	// A finding on any object never binds it: only suspects on every failing path do.
	finding := sdk.Finding{DetectorID: "d", Status: sdk.FindingActive, PrimaryResource: dref("Service", "api")}
	runs := SelectProgramCandidates(context.Background(), registry, restoreViewOf(nil), []sdk.Finding{finding}, time.Second)
	if len(runs) != 0 {
		t.Fatalf("a finding bound the built-in playbook: %v", runs)
	}
}

func restoreCandidateNames(view program.ReadView, request IncidentRequest) []string {
	var names []string
	for _, candidate := range sourceRestoreCandidates(view, request, request.Findings) {
		names = append(names, candidate.bindings.Values[SourceRestoreComponentParam].(string))
	}
	return names
}

// Every component on a failing path is a candidate, also one that a passing
// path shares and one on only some failing paths: the departure from accepted
// source is the evidence, the path only keeps components that cannot affect a
// failing operation out. jobs is on no path.
func TestSourceRestoreCandidatesAreTheComponentsOnAFailingPath(t *testing.T) {
	request := restoreRequest("kubectl")
	finding := probeFinding("create")
	request.Findings, request.OpeningKeys = []sdk.Finding{finding}, []string{findingKey(finding)}
	// front is also on the passing "list" path; api and db are only on the failing one.
	view := restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: detectApp("create").Snapshot}}
	if names := restoreCandidateNames(view, request); !slices.Equal(slices.Sorted(slices.Values(names)), []string{"api", "db", "front"}) {
		t.Fatalf("candidates %v, want front, api and db", names)
	}
	// Both tests failing: front is on every failing path, api and db on one.
	both := restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: detectApp("create", "list").Snapshot}}
	list := probeFinding("list")
	request.Findings, request.OpeningKeys = []sdk.Finding{finding, list}, []string{findingKey(finding), findingKey(list)}
	names := restoreCandidateNames(both, request)
	for _, want := range []string{"front", "api", "db"} {
		if !slices.Contains(names, want) {
			t.Fatalf("candidates %v, want %s among them", names, want)
		}
	}
	if slices.Contains(names, "jobs") {
		t.Fatalf("candidates %v: jobs is on no failing path", names)
	}
}

func TestSourceRestoreEarnsNoCreditThatAnyOtherPlaybookWould(t *testing.T) {
	outcome := func(id string) *contracts.OutcomeRecord {
		application := creditApplication("app-1", id, 10)
		return &contracts.OutcomeRecord{
			IncidentId: "i1", Classification: "success",
			RecoveryVerification: &contracts.RecoveryVerification{OpeningKeys: []string{"k1"}},
			PlanApplications:     []*contracts.PlanApplication{application},
			PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{
				{PlaybookId: id, Bindings: creditComponent("front"), ApplyReceipt: application, Result: "applied_cleared"},
			}}},
		}
	}
	if !ProgramCreditIndex([]*contracts.OutcomeRecord{outcome("fix")}).Has("fix") {
		t.Fatal("control: an ordinary playbook is credited by this outcome")
	}
	if ProgramCreditIndex([]*contracts.OutcomeRecord{outcome(SourceRestoreID)}).Has(SourceRestoreID) {
		t.Fatal("the built-in playbook earned a credited pair")
	}
	if _, ok := ProgramTrustStats([]*contracts.OutcomeRecord{outcome(SourceRestoreID)})[SourceRestoreID]; ok {
		t.Fatal("the built-in playbook earned trust")
	}
}

// restoreStage is the keyed-stage world with an application map and a source
// comparison under which Deployment api departs from source by kubectl, and
// one condition finding names api.
func restoreStage(t *testing.T, auto bool) (*sameWriteWorld, *DeterministicResponseStage, *IncidentState) {
	t.Helper()
	world, stage, state := keyedStageLoop(t, nil)
	api := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	finding := sdk.Finding{DetectorID: "traffic", RuleID: "scenario-slo.browse", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "api crash looping", Evidence: "synthetic", Fingerprint: "api", PrimaryResource: api}
	state.Findings = []sdk.Finding{finding}
	state.Request.Findings, state.Request.OpeningKeys = state.Findings, []string{findingKey(finding)}
	at := detectOnset.Add(-time.Minute)
	state.Request.DriftEvents = []sdk.DriftEvent{departureEvent(api, "kubectl", at, "spec.template")}
	state.Request.DetectedAt = detectOnset
	loop := stage.NewLoop(state.Request)
	loop.Request = state.Request
	loop.findings = state.Findings
	loop.BuiltinRestoreAuto = auto
	loop.Programs = withSourceRestore(nil, loop.restoreEvidence)
	application := program.AppMap{Components: []program.AppComponent{{Name: "api", Workload: "Deployment/api"}, {Name: "web", Workload: "Deployment/web"}}}
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		snapshot := v.(sdktest.Snapshot)
		snapshot.ApplicationMap = &application
		return restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot}, diffs: map[string]sdk.SourceDiff{"Deployment/api": {Changed: true}}}
	}
	return world, stage, state
}

func restoreAttempt(report *contracts.StageReport) *contracts.StageMitigation {
	for _, attempt := range report.GetMitigations() {
		if attempt.GetPlaybookId() == SourceRestoreID {
			return attempt
		}
	}
	return nil
}

// response.builtin_restore_auto is on by default: the stage applies the
// built-in restore when its diagnosis matches, before any agent.
func TestBuiltInRestoreIsAutomaticByDefault(t *testing.T) {
	if !settings.Default().Response.BuiltinRestoreAuto {
		t.Fatal("response.builtin_restore_auto must default to true")
	}
}

// With the switch off the agent decides: the stage lists the playbook as a match but never applies it.
func TestStageLeavesTheBuiltInRestoreToTheResponderWhenTheSwitchIsOff(t *testing.T) {
	world, stage, state := restoreStage(t, false)
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := restoreAttempt(report)
	if attempt == nil || attempt.GetSkipReason() != stageBuiltinRestoreManual || world.restarts.Load() != 0 {
		t.Fatalf("attempt %v, %d writes; want skipped %q", attempt, world.restarts.Load(), stageBuiltinRestoreManual)
	}
	if got := state.Request.PlaybookCoverage.GetMatches(); len(got) != 1 || got[0].GetPlaybookId() != SourceRestoreID {
		t.Fatalf("the playbook must be offered as a match: %v", got)
	}
}

// With response.builtin_restore_auto the stage goes on to plan and apply it.
func TestStageAppliesTheBuiltInRestoreWhenTheSwitchIsOn(t *testing.T) {
	_, stage, state := restoreStage(t, true)
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := restoreAttempt(report)
	if attempt == nil || attempt.GetSkipReason() == stageBuiltinRestoreManual {
		t.Fatalf("with the switch on the stage must not skip it as manual: %v", attempt)
	}
}

// A change that was already there while the application then stayed healthy did
// not break it: the automatic stage leaves it, the responder can still see and
// restore it.
func TestStageRestoresOnlyDeparturesNotSeenHealthy(t *testing.T) {
	for name, seenHealthy := range map[string]bool{"seen healthy": true, "made since": false} {
		t.Run(name, func(t *testing.T) {
			_, stage, state := restoreStage(t, true)
			state.Request.DriftEvents[0].SeenHealthy = seenHealthy
			report, err := stage.Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			attempt := restoreAttempt(report)
			planned := attempt != nil && attempt.GetSkipReason() == ""
			if planned == seenHealthy {
				t.Fatalf("automatic restore attempted=%t with seen_healthy=%t: %v", planned, seenHealthy, attempt)
			}
		})
	}
	request := restoreRequest("kubectl")
	request.DriftEvents[0].SeenHealthy = true
	manual := sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	view := restoreViewOf(map[string]sdk.SourceDiff{"Service/api": {Changed: true}})
	if verdict, err := manual.Diagnose(context.Background(), view, restoreAPI()); err != nil || verdict.Result != program.Match {
		t.Fatalf("outside the automatic stage the departure still matches: %+v, %v", verdict, err)
	}
}

// The opening condition's own component is a candidate wherever it stands, also
// as a dependency a hop away from the failing path; without the condition it is not.
func TestSourceRestoreCandidatesIncludeADependencyThatOpenedTheIncident(t *testing.T) {
	app := detectApp("create")
	app.ApplicationMap.Edges = []program.AppEdge{{From: "api", To: "jobs"}}
	view := restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: app.Snapshot}}
	request := restoreRequest("kubectl")
	probe := probeFinding("create")
	request.Findings, request.OpeningKeys = []sdk.Finding{probe}, []string{findingKey(probe)}
	if names := restoreCandidateNames(view, request); slices.Contains(names, "jobs") {
		t.Fatalf("candidates %v: a dependency off the failing path is not one by itself", names)
	}
	condition := sdk.Finding{DetectorID: "workload-health", RuleID: "crash-loop", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "jobs"}}
	request.Findings, request.OpeningKeys = []sdk.Finding{probe, condition}, []string{findingKey(probe), findingKey(condition)}
	set := buildSuspectSet(detectInput{Request: request, View: view, Findings: request.Findings})
	if !slices.ContainsFunc(set.nodes, func(n *candidateNode) bool { return n.Name == "jobs" && n.Class == PositionDependency }) {
		t.Fatal("jobs must be placed as a dependency of the failing path")
	}
	if names := restoreCandidateNames(view, request); !slices.Contains(names, "jobs") {
		t.Fatalf("candidates %v, want the opening condition's component jobs", names)
	}
}

// sharedRestoreWorld is front, on the failing create path and the passing list
// path, whose Service and mounted ConfigMap were both edited by kubectl. The
// create test fails while either differs from accepted source.
type sharedRestoreWorld struct {
	client *fake.Clientset
	app    detectFake
}

func (w sharedRestoreWorld) live(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return TypedPlanClient{Client: w.client}.Get(context.Background(), ref)
}

func (w sharedRestoreWorld) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	var object kruntime.Object = &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "front"}, Ports: []corev1.ServicePort{{Port: 80}}}}
	if ref.Kind == "ConfigMap" {
		object = &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}, Data: map[string]string{"mode": "accepted"}}
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	return &unstructured.Unstructured{Object: content}, err
}

func (w sharedRestoreWorld) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (w sharedRestoreWorld) departed() map[string]bool {
	service, _ := w.client.CoreV1().Services("app").Get(context.Background(), "front", metav1.GetOptions{})
	config, _ := w.client.CoreV1().ConfigMaps("app").Get(context.Background(), "front-config", metav1.GetOptions{})
	return map[string]bool{
		"Service/front":          len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 80,
		"ConfigMap/front-config": config.Data["mode"] != "accepted",
	}
}

func (w sharedRestoreWorld) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return w.app.Snapshot, nil
}

func (w sharedRestoreWorld) Wait(context.Context, []sdk.ObjectRef) error { return nil }

// With the default settings the stage puts both objects back at the page,
// before any agent, although a passing path shares the component.
func TestStageRestoresASharedComponentsServiceAndConfigMap(t *testing.T) {
	app := detectApp("create")
	for i := range app.DeploymentList {
		if app.DeploymentList[i].Name == "front" {
			app.DeploymentList[i].Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "front-config"}}}}}
		}
	}
	world := sharedRestoreWorld{app: app, client: fake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "app", UID: "synthetic-service", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "front"}, Ports: []corev1.ServicePort{{Port: 81}}}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "front-config", Namespace: "app", UID: "synthetic-config", ResourceVersion: "1"}, Data: map[string]string{"mode": "edited"}})}
	finding := probeFinding("create")
	finding.Severity = sdk.SeverityCritical
	request := detectRequest()
	request.RepairPolicy, request.SourceCommit = "recorded-actions", "synthetic-accepted"
	request.Findings, request.OpeningKeys = []sdk.Finding{finding}, []string{findingKey(finding)}
	before := detectOnset.Add(-4 * time.Second)
	request.DriftEvents = []sdk.DriftEvent{
		departureEvent(dref("Service", "front"), "kubectl", before, "spec.ports"),
		departureEvent(dref("ConfigMap", "front-config"), "kubectl", before, "data.mode"),
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: passingVerifier{}}
	loop := NewResponderLoop(request, nil, world, nil, executor, 5)
	loop.BuiltinRestoreAuto = settings.Default().Response.BuiltinRestoreAuto
	loop.ReadView = func(sdk.DetectionContext) program.ReadView {
		diffs := map[string]sdk.SourceDiff{}
		for key, changed := range world.departed() {
			diffs[key] = sdk.SourceDiff{Changed: changed}
		}
		return restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: app.Snapshot}, diffs: diffs}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	report, err := stage.Run(context.Background(), &IncidentState{Request: request, Findings: request.Findings})
	if err != nil {
		t.Fatal(err)
	}
	for key, departed := range world.departed() {
		if departed {
			t.Errorf("%s was not restored", key)
		}
	}
	if attempt := restoreAttempt(report); attempt == nil || !attempt.GetApplyReceipt().GetApplied() {
		t.Fatalf("want an applied source-restore: %v", report)
	}
}

// snapshotRestoreView reads objects through the snapshot's own sdk.ObjectReader
// (an absent object of a completely listed kind is ErrObjectAbsent) and has
// canned source comparisons.
type snapshotRestoreView struct {
	*sdktest.ReplayReadView
	diffs map[string]sdk.SourceDiff
}

func (v snapshotRestoreView) DiffAgainstSource(ref sdk.ObjectRef) (sdk.SourceDiff, error) {
	return v.diffs[ref.Kind+"/"+ref.Name], nil
}

// A source-declared ConfigMap a workload mounts, deleted outside SDO, is a
// missing object: the built-in playbook and `sdo detect` restore it with
// CreateFromSource, never with a field restore that needs a live object.
func TestSourceRestoreRecreatesADeletedSourceDeclaredObject(t *testing.T) {
	app := detectApp("create")
	for i := range app.DeploymentList {
		if app.DeploymentList[i].Name == "api" {
			app.DeploymentList[i].Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "script", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "init-script"}}}}}
		}
	}
	app.CompleteKindList = []schema.GroupVersionKind{{Version: "v1", Kind: "ConfigMap"}}
	request := detectRequest()
	deleted := detectOnset.Add(-4 * time.Second)
	request.DriftEvents = []sdk.DriftEvent{{Ref: dref("ConfigMap", "init-script"), Manager: "unknown", ObservedAt: deleted, Fields: []sdk.DriftField{{Path: "data.init.sh"}}}}
	view := snapshotRestoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: app.Snapshot}, diffs: map[string]sdk.SourceDiff{"ConfigMap/init-script": {Changed: true}}}
	restore := sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	verdict, err := restore.Diagnose(context.Background(), view, restoreAPI())
	if err != nil || verdict.Result != program.Match {
		t.Fatalf("verdict %+v, err %v", verdict, err)
	}
	plan, err := restore.Mitigate(context.Background(), view, restoreAPI())
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("plan %+v, err %v", plan, err)
	}
	if action, ok := plan.Actions[0].(program.CreateFromSource); !ok || action.Ref.Name != "init-script" {
		t.Fatalf("a deleted object is created from source, got %#v", plan.Actions[0])
	}
}
