package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

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
