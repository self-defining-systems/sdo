package runtime

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sync/atomic"
	"testing"
	"time"
)

// This fixture preserves the seven Deployment roles of a learned discovery
// repair without copying any experiment output into the source tree.
func sevenRoleFixture() (program.ProgramSpec, sdktest.Snapshot, program.ObjectBindings) {
	spec := program.ProgramSpec{ID: "consul-bind-and-discovery"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*driftService(9999)}}
	bindings := program.ObjectBindings{}
	for _, name := range []string{"deployment", "search", "rate", "reservation", "profile", "recommendation", "user"} {
		spec.Params = append(spec.Params, program.ParamSpec{Name: name, Kind: "Deployment", Required: true})
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
		bindings[name] = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
	}
	for i := 0; i < 12; i++ {
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("other-%02d", i), Namespace: "app"}})
	}
	spec.Params = append(spec.Params, program.ParamSpec{Name: "service", Kind: "Service", Required: true})
	bindings["service"] = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: snapshot.ServiceList[0].Name}
	return spec, snapshot, bindings
}
func TestProgramDiagnosisRequiresFindingSevenRoles(t *testing.T) {
	spec, snapshot, bindings := sevenRoleFixture()
	var calls atomic.Int32
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: spec, run: func(context.Context, program.Bindings) program.Verdict {
		calls.Add(1)
		return program.Verdict{Result: program.Match}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot}
	if runs := SelectProgramCandidates(context.Background(), registry, noEnumerationView{view}, nil, 20*time.Millisecond); len(runs) != 0 || calls.Load() != 0 {
		t.Fatalf("diagnosed without finding: %d runs, %d calls", len(runs), calls.Load())
	}
	finding := sdk.Finding{DetectorID: "discovery", RuleID: "bind", Status: sdk.FindingActive, PrimaryResource: bindings["service"], ParameterBindings: bindings, Playbooks: []string{spec.ID}}
	findings, err := sdk.DetectObserved(context.Background(), sevenRoleDetector{finding}, view)
	if err != nil {
		t.Fatal(err)
	}
	runs := SelectProgramCandidates(context.Background(), registry, view, findings, time.Second)
	if len(runs) != 1 || calls.Load() != 1 || runs[0].Verdict.Result != program.Match || !runs[0].Bindings.Equal(program.Bindings{Objects: bindings}) {
		t.Fatalf("finding diagnosis: %+v calls=%d", runs, calls.Load())
	}
}
func TestObjectChangeAndTimerNeverDiagnosePlaybooks(t *testing.T) {
	spec, snapshot, _ := sevenRoleFixture()
	var calls atomic.Int32
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: spec, run: func(context.Context, program.Bindings) program.Verdict {
		calls.Add(1)
		return program.Verdict{Result: program.Match}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.dispatcher = findingDiagnoseDispatcher{registry: registry, view: &sdktest.ReplayReadView{Snapshot: snapshot}}
	controller.ConfigureDrift(DriftControllerConfig{Events: &fixedObjectChanges{changes: []ObjectChange{objectChange(t, driftService(8080), driftService(9999), "v1", "Service")}}, View: func(s sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}, diff: program.Diff{Changed: false}}
	}})
	for _, elapsed := range []time.Duration{time.Second, time.Minute, 2 * time.Minute} {
		if err := controller.StepEvents(context.Background(), start.Add(elapsed), nil); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 || controller.IncidentOpen() {
		t.Fatalf("watch/timer diagnosed playbooks: %d calls, open=%v", calls.Load(), controller.IncidentOpen())
	}
	for _, state := range controller.tracker.Snapshot() {
		if state.DetectorID != "drift.unexplained" {
			t.Fatalf("program produced detector state: %+v", state)
		}
	}
}

// Enumeration has no read authority when no detector finding is being handled.
type noEnumerationView struct{ program.ReadView }

func (noEnumerationView) Namespace() string { panic("namespace enumeration without finding") }
func (noEnumerationView) Deployments() []appsv1.Deployment {
	panic("Deployment enumeration without finding")
}
func (noEnumerationView) Services() []corev1.Service { panic("Service enumeration without finding") }

type findingDiagnoseDispatcher struct {
	registry program.ProgramRegistry
	view     program.ReadView
}

func (d findingDiagnoseDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	SelectProgramCandidates(ctx, d.registry, d.view, r.Findings, time.Second)
	return completedResult(r.IncidentID), nil
}

func TestDriftAndControllerEntrypointsCannotInvokePlaybookDiagnosis(t *testing.T) {
	for _, path := range []string{"drift.go", "drift_verification.go", "controller.go", "ownership_adoption.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var name string
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				name = fn.Name
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			}
			if name == "RunDiagnose" || name == "Diagnose" || name == "SelectProgramCandidates" {
				t.Errorf("%s invokes playbook diagnosis outside finding handling: %s", path, name)
			}
			return true
		})
	}
}

type sevenRoleDetector struct{ finding sdk.Finding }

func (sevenRoleDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "discovery", Class: sdk.DetectorClassIncident, Playbooks: []string{"consul-bind-and-discovery"}}
}
func (d sevenRoleDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{d.finding}, nil
}

func TestRestoreRetiresProgramProducedFindings(t *testing.T) {
	start := time.Unix(100, 0)
	config := testControllerConfig()
	config.Namespace = "app"
	c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{}, start)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"drift.consul-bind-and-discovery", "drift.unexplained"} {
		finding := sdk.Finding{DetectorID: id, RuleID: "fault", Status: sdk.FindingActive, Severity: sdk.SeverityLatent, Summary: "fixture fault", Evidence: "recorded fixture evidence", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}
		c.tracker.SetPolicy(id, 1, 1, 0)
		c.tracker.Observe(start, id, []sdk.Finding{finding})
		c.batcher.AddAt(finding, start)
	}
	state := exportState(t, c)
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	states := c.tracker.Snapshot()
	if len(states) != 1 {
		t.Fatalf("retired program finding survived restart: %+v", states)
	}
	for _, state := range states {
		if state.DetectorID != "drift.unexplained" {
			t.Fatalf("source drift was lost: %+v", state)
		}
	}
	pending := c.batcher.Snapshot()
	if len(pending.Findings) != 1 || pending.Findings[0].DetectorID != "drift.unexplained" {
		t.Fatalf("retired program finding survived in pending batch: %+v", pending)
	}
}

// A Match bound from the finding explains it; neither a detector list nor a
// program ID resembling the rule is the link.
func TestCoverageExplainsByDiagnosedBindingNotProgramIdentity(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{DetectorID: "drift.routing", RuleID: "routing", Status: sdk.FindingActive, PrimaryResource: ref}
	run := ProgramRun{ProgramID: "routing", FindingFingerprint: FindingFingerprint(finding), Bindings: program.Bindings{Objects: program.ObjectBindings{"service": ref}}, Verdict: program.Verdict{Result: program.Match}}
	got := buildPlaybookCoverage([]sdk.Finding{finding}, []ProgramRun{run}, nil, nil)
	if len(got.Matches) != 1 || len(got.Matches[0].ExplainedFindingFingerprints) != 1 || len(got.UnexplainedFindingFingerprints) != 0 {
		t.Fatalf("diagnosed binding did not explain its finding: %+v", got)
	}
	run.FindingFingerprint = "another/finding"
	got = buildPlaybookCoverage([]sdk.Finding{finding}, []ProgramRun{run}, nil, nil)
	if len(got.Matches) != 1 || len(got.Matches[0].ExplainedFindingFingerprints) != 0 || len(got.UnexplainedFindingFingerprints) != 1 {
		t.Fatalf("program identity implied a link without a diagnosed binding: %+v", got)
	}
}
