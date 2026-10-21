package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type driftReadView struct {
	*sdktest.ReplayReadView
	diff program.Diff
}

func (v driftReadView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) { return v.diff, nil }

func TestIncidentRequestCarriesOnlyFindingDriftAndPreservesStatusHistory(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	old := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "reservation"},
		ObservedAt: start, Classification: sdk.DriftFault, ProgramID: "targetport", InvestigationVerdict: "bug"}
	current := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"},
		ObservedAt: start.Add(time.Minute), Classification: sdk.DriftFault, ProgramID: "targetport"}
	controller.driftEvents = []sdk.DriftEvent{old, current}
	finding := sdk.Finding{DetectorID: "drift.targetport", RuleID: "targetport", Status: sdk.FindingActive,
		PrimaryResource: current.Ref, Metadata: map[string]any{"drift_event": current}}
	request := controller.incidentRequest(start.Add(2*time.Minute), []sdk.Finding{finding})
	if len(request.DriftEvents) != 1 || request.DriftEvents[0].Ref != current.Ref {
		t.Fatalf("request carried unrelated historical drift: %+v", request.DriftEvents)
	}
	if events := controller.ExportState().DriftEvents; len(events) != 2 || events[0].Ref != old.Ref {
		t.Fatalf("status lost drift history: %+v", events)
	}
}

func TestIncidentRequestCarriesProgramSweepDriftFinding(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"},
		ObservedAt: start, Classification: sdk.DriftFault, ProgramID: "targetport"}
	finding := sdk.Finding{DetectorID: "drift.targetport", RuleID: "targetport", Status: sdk.FindingActive,
		PrimaryResource: event.Ref, Metadata: map[string]any{"drift_event": event}}
	request := controller.incidentRequest(start.Add(time.Second), []sdk.Finding{finding})
	if len(request.DriftEvents) != 1 || request.DriftEvents[0].ProgramID != "targetport" {
		t.Fatalf("program sweep evidence missing from request: %+v", request.DriftEvents)
	}
}

func TestDriftClassifierIgnoresHPAOwnedReplicas(t *testing.T) {
	before := int32(2)
	after := int32(3)
	old := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", ManagedFields: []metav1.ManagedFieldsEntry{
		{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:template":{}}}`)}},
		{Manager: "horizontal-pod-autoscaler", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}},
	}}, Spec: appsv1.DeploymentSpec{Replicas: &before}}
	next := old.DeepCopy()
	next.Spec.Replicas = &after
	event, _, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "apps/v1", "Deployment"), driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, diff: program.Diff{Changed: true}}, time.Unix(10, 0))
	if err != nil || event != nil {
		t.Fatalf("HPA replicas entered drift feed: %+v, %v", event, err)
	}
}

func TestDriftClassifierMatchesTargetPortOnAffectedBinding(t *testing.T) {
	old := driftService(8080)
	next := driftService(9999)
	next.ManagedFields = append(next.ManagedFields, metav1.ManagedFieldsEntry{Manager: "unknown-editor", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}})
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, bindings program.Bindings) program.Verdict {
		if bindings["service"].Name == "api" {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next, *driftService(8080)}}}, diff: program.Diff{Changed: true, Text: "accepted manifest differs"}}
	event, runs, err := (DriftClassifier{Programs: registry, Deadline: time.Second}).Classify(context.Background(), objectChange(t, old, next, "v1", "Service"), view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftFault || event.Manager != "unknown-editor" || len(runs) != 1 || runs[0].Verdict.Result != program.Match || len(event.Fields) != 1 || event.Fields[0].Path != "spec.ports[0].targetPort" {
		t.Fatalf("targetPort was not classified: event=%+v runs=%+v err=%v", event, runs, err)
	}
}

func TestWatchEventProgramsUseHintOnceForOwnedAndUnownedChanges(t *testing.T) {
	for _, owned := range []bool{true, false} {
		t.Run(map[bool]string{true: "owned", false: "unowned"}[owned], func(t *testing.T) {
			before, after := driftService(8080), driftService(9999)
			if !owned {
				before.ManagedFields, after.ManagedFields = nil, nil
			}
			calls := 0
			registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: scenarioPortProgram{}.Spec(), run: func(context.Context, program.Bindings) program.Verdict {
				calls++
				return program.Verdict{Result: program.Match}
			}}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			view := serviceDriftView{&sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*after, {ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "app"}}}}}}
			event, runs, err := (DriftClassifier{Programs: registry}).Classify(context.Background(), objectChange(t, before, after, "v1", "Service"), view, time.Unix(10, 0))
			if err != nil || event == nil || calls != 1 || len(runs) != 1 || runs[0].BindingSource != "hint" || runs[0].Bindings["service"].Name != "api" {
				t.Fatalf("watch binding was not diagnosed exactly once as hint: event=%+v runs=%+v calls=%d err=%v", event, runs, calls, err)
			}
			if event.Source != "watch event program match" {
				t.Fatalf("watch program source lost: %+v", event)
			}
			if owned && (len(event.Fields) != 1 || event.Fields[0].Old != "8080" || event.Fields[0].New != "9999" || event.Manager != "sdo-deployer") {
				t.Fatalf("owned field evidence replaced: %+v", event)
			}
		})
	}
}

func TestWatchEventProgramsSkipInitialButAllowSourceAbsentObjects(t *testing.T) {
	for _, initial := range []bool{false, true} {
		calls := 0
		registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: scenarioPortProgram{}.Spec(), run: func(context.Context, program.Bindings) program.Verdict {
			calls++
			return program.Verdict{Result: program.Match}
		}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		after := driftService(9999)
		change := objectChange(t, driftService(8080), after, "v1", "Service")
		change.Initial = initial
		view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*after}}}, diff: program.Diff{Changed: true, SourceAbsent: !initial}}
		event, runs, err := (DriftClassifier{Programs: registry}).Classify(context.Background(), change, view, time.Unix(10, 0))
		if initial && (err != nil || event != nil || len(runs) != 0 || calls != 0) {
			t.Fatalf("initial object ran a program: event=%+v runs=%+v calls=%d err=%v", event, runs, calls, err)
		}
		if !initial && (err != nil || event == nil || len(runs) != 1 || calls != 1) {
			t.Fatalf("source absence gated a learned detector: event=%+v runs=%+v calls=%d err=%v", event, runs, calls, err)
		}
	}
}

func TestDriftClassifierRecordsUnexplainedSDOChange(t *testing.T) {
	old := driftService(8080)
	next := driftService(8081)
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}, diff: program.Diff{Changed: true}}
	event, runs, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "v1", "Service"), view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained || len(runs) != 0 {
		t.Fatalf("unexplained change lost: event=%+v runs=%+v err=%v", event, runs, err)
	}
}

func TestDriftClassifierRecordsAddedDeclaredObject(t *testing.T) {
	added := driftService(9999)
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*added}}}, diff: program.Diff{Changed: true}}
	event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{New: driftObject(added, sdk.WatchKind{APIVersion: "v1", Kind: "Service"})}, view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained {
		t.Fatalf("added SDO object was not classified: %+v, %v", event, err)
	}
}

func TestControllerOpensLatentIncidentOnDriftWithinOneStep(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	old := driftService(8080)
	next := driftService(9999)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(old, next)
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPatch || request.URL.Query().Get("dryRun") != metav1.DryRunAll {
			t.Errorf("unexpected source normalization request: %s %s", request.Method, request.URL)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)}}}})
	}))
	defer server.Close()
	apiClient, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	source := planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}}}}}
	controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, Programs: registry, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Client: apiClient, Source: source, SourceCommit: "accepted"}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	state := controller.ExportState()
	if !state.IncidentOpen || len(state.IncidentRequest.Findings) != 1 || state.IncidentRequest.Findings[0].Severity != sdk.SeverityLatent || len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != sdk.DriftFault || state.IncidentView == nil || len(state.IncidentView.DriftEvents) != 1 {
		t.Fatalf("latent incident not opened in one reconcile: %+v", state)
	}
}

func TestResponderStatusIncludesDriftClassification(t *testing.T) {
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, ObservedAt: time.Unix(10, 0).UTC(), Classification: sdk.DriftUnexplained}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Namespace: "app", DriftEvents: []sdk.DriftEvent{event}}, nil,
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{}, 1)
	status := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(status.DriftEvents) != 1 || status.DriftEvents[0].Classification != sdk.DriftUnexplained {
		t.Fatalf("status lost drift classification: %+v", status)
	}
}

func TestControllerFullProgramSweepFallsBackAfterConfiguredInterval(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	service := driftService(9999)
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*service}}}
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Programs: registry, SweepInterval: 10 * time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(9*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if controller.IncidentOpen() || !controller.NextWake().Equal(start.Add(10*time.Second)) {
		t.Fatalf("program swept early: %+v", controller.ExportState())
	}
	if err := controller.StepEvents(context.Background(), start.Add(10*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() {
		t.Fatal("full sweep missed latent invariant")
	}
}

func TestControllerPersistsUnexplainedChangeWithoutIncident(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	old := driftService(8080)
	next := driftService(8081)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(old, next)
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	state := controller.ExportState()
	if state.IncidentOpen || len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != sdk.DriftUnexplained {
		t.Fatalf("unexplained event was not retained: %+v", state)
	}
}

func driftService(target int32) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}}}}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(target)}}}}
}

func objectChange(t *testing.T, old, next any, version, kind string) ObjectChange {
	t.Helper()
	return ObjectChange{Old: driftObject(old, sdk.WatchKind{APIVersion: version, Kind: kind}), New: driftObject(next, sdk.WatchKind{APIVersion: version, Kind: kind})}
}
