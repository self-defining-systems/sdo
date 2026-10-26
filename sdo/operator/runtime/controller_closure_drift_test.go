package runtime

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Readiness can recover after an image repair while a Service still routes to
// a port the selected Pod does not listen on. This health detector deliberately
// models that contract, rather than treating every source mismatch as health.
type closureImageHealth struct{ fastloopDetector }

func (closureImageHealth) Detect(ctx context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	return (fastloopDetector{}).Detect(ctx, sdktest.Snapshot{NamespaceName: v.Namespace(), DeploymentList: v.Deployments()})
}

type closureImageResponder struct{ requests chan IncidentRequest }

func (d closureImageResponder) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	d.requests <- r
	result := fastloopGateResult(r, []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: r.Namespace, Name: "frontend"}})
	result.ConfirmedRootCauses[0].Summary = "frontend image unavailable"
	return result, nil
}

type closureDriftFixture struct {
	c      *Controller
	client *fake.Clientset
	cache  *KubernetesCache
	source *unstructured.Unstructured
	start  time.Time
	closed []IncidentClosure
}

func newClosureDriftFixture(t *testing.T, owned, declared bool, namespace string) *closureDriftFixture {
	t.Helper()
	f := &closureDriftFixture{start: time.Unix(100, 0).UTC()}
	before := driftService(8087)
	before.Name, before.Namespace = "reservation", namespace
	before.Spec.Ports[0].Port = 8087
	before.ManagedFields[0].FieldsV1.Raw = bytes.ReplaceAll(before.ManagedFields[0].FieldsV1.Raw, []byte("80"), []byte("8087"))
	if !owned {
		before.ManagedFields = nil
	}
	object, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(before)
	if err != nil {
		t.Fatal(err)
	}
	f.source = &unstructured.Unstructured{Object: object}
	f.source.SetAPIVersion("v1")
	f.source.SetKind("Service")
	broken := before.DeepCopy()
	broken.Annotations = withAcceptance(t, f.source, f.source)
	broken.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	frontend := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "frontend", Image: "missing:v1"}}}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "reservation-pod", Namespace: namespace}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "reservation", Ports: []corev1.ContainerPort{{ContainerPort: 8087}}}}}}
	f.client = fake.NewSimpleClientset(broken, frontend, pod)
	f.cache, err = NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: f.client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace, config.FiringThreshold, config.ClearThreshold = "app", 1, 1
	config.VerificationTimeout = 5 * time.Second
	dispatcher := closureImageResponder{requests: make(chan IncidentRequest, 1)}
	f.c, err = NewController(config, []sdk.Detector{closureImageHealth{}}, fastloopObjects{f.client}, dispatcher, f.start)
	if err != nil {
		t.Fatal(err)
	}
	f.c.OnIncidentClosed = func(c IncidentClosure) { f.closed = append(f.closed, c) }
	f.c.ConfigureDrift(DriftControllerConfig{Events: f.cache, View: func(s sdk.DetectionContext) program.ReadView {
		if !declared {
			return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, SourceAbsent: true}}
		}
		return LiveReadView{DetectionContext: s, Source: planFakeSource{source: f.source}, SourceCommit: "accepted"}
	}})
	f.cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: namespace}).OnUpdate(before, broken)
	f.step(t, 0)
	executePendingEffect(t, f.c)
	request := awaitRequest(t, dispatcher.requests)
	if owned && declared && namespace == "app" && len(request.DriftEvents) != 1 {
		t.Fatalf("fixture did not admit owned drift: %+v", request)
	}
	awaitDispatchCompletionQueued(t, f.c)
	// The responder's image-only repair succeeds; routing is unchanged.
	frontend.Spec.Template.Spec.Containers[0].Image = "healthy:v1"
	// The fake API does not run a deployment controller: model the completed
	// rollout as well as the repaired image before expecting global closure.
	frontend.Status = appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, err := f.client.AppsV1().Deployments("app").Update(context.Background(), frontend, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *closureDriftFixture) step(t *testing.T, seconds int) {
	t.Helper()
	if err := f.c.Step(context.Background(), f.start.Add(time.Duration(seconds)*time.Second), nil); err != nil {
		t.Fatal(err)
	}
}
// settle steps at seconds and again once the closure stability window has
// held, when a verified success may commit.
func (f *closureDriftFixture) settle(t *testing.T, seconds int) {
	t.Helper()
	f.step(t, seconds)
	f.step(t, seconds+int(f.c.config.ClosureStabilityWindow/time.Second))
}

func (f *closureDriftFixture) restoreSource(t *testing.T) {
	t.Helper()
	service, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(8087)
	if _, err := f.client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
func closureOutcome(t *testing.T, closure IncidentClosure) string {
	t.Helper()
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "repair", "codex", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return derived.Record.GetClassification()
}

func TestClosureAdmittedDriftContracts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		owned, declared bool
		namespace       string
		repair          bool
	}{
		{"image_only", true, true, "app", false},
		{"both_fixed", true, true, "app", true},
		{"unowned", false, true, "app", false},
		{"undeclared", true, false, "app", false},
		{"other_namespace", true, true, "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClosureDriftFixture(t, tc.owned, tc.declared, tc.namespace)
			if tc.repair {
				f.restoreSource(t)
			}
			f.step(t, 1)
			blocked := tc.owned && tc.declared && tc.namespace == "app" && !tc.repair
			if blocked {
				if len(f.closed) != 0 {
					t.Fatalf("image repair closed with unresolved targetPort drift: %s", closureOutcome(t, f.closed[0]))
				}
				f.step(t, 6)
			} else {
				f.settle(t, 1)
			}
			if len(f.closed) != 1 {
				t.Fatalf("closures=%d", len(f.closed))
			}
			want := "success"
			if blocked {
				want = "failed"
			}
			if got := closureOutcome(t, f.closed[0]); got != want {
				t.Fatalf("classification=%s want %s", got, want)
			}
			if blocked {
				derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), f.closed[0], "repair", "codex", "fixture")
				if err != nil {
					t.Fatal(err)
				}
				if len(derived.Record.GetDriftEvents()) != 1 || len(f.closed[0].IncidentDetectorStates) != 1 || f.closed[0].IncidentDetectorStates[0].Status != DetectorEvaluationFiring {
					t.Fatalf("outcome lost unresolved drift: %v closure=%+v", derived.Record, f.closed[0])
				}
			}
		})
	}
}

func TestClosureDriftResolvedByRestoringSource(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.step(t, 1)
	if len(f.closed) != 0 {
		t.Fatal("closed before source restoration")
	}
	f.restoreSource(t)
	f.settle(t, 2)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatalf("source restoration did not verify: %+v", f.closed)
	}
}

// Hand-written reduction of L3 i04, from FX1's analysis: the image is repaired,
// Service reservation remains port 8087 -> targetPort 9999, and its Pod declares
// 8087. No experiment captures or oracle verdicts are inputs to the controller.
func TestClosureL3I04Reduction(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.step(t, 1)
	if len(f.closed) != 0 {
		t.Fatal("readiness recovery hid the remaining L3 routing fault")
	}
	f.step(t, 6)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "failed" {
		t.Fatal("incomplete composite repair did not fail at the verification deadline")
	}
	event := f.closed[0].Request.DriftEvents[0]
	if len(event.Fields) != 1 || event.Fields[0].New != "9999" || event.Fields[0].Source != "8087" {
		t.Fatalf("routing evidence=%+v", event)
	}
	t.Logf("L3 reduction: %s; %s/%s still targets %s rather than %s", closureOutcome(t, f.closed[0]), event.Ref.Kind, event.Ref.Name, event.Fields[0].New, event.Fields[0].Source)
}

func TestClosureDriftSourceErrorFailsClosed(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.c.drift.View = func(sdk.DetectionContext) program.ReadView { return failedSourceDriftView{} }
	f.step(t, 1)
	if len(f.closed) != 0 {
		t.Fatal("source read error verified drift")
	}
	f.step(t, 6)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "failed" {
		t.Fatal(fmt.Sprint(f.closed))
	}
	if len(f.closed[0].IncidentDetectorStates) != 1 || f.closed[0].IncidentDetectorStates[0].Status != DetectorEvaluationError {
		t.Fatal("source comparison error was not recorded in closure")
	}
}

func TestClosureDriftRestoredFieldsIgnoreUnadmittedChanges(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.restoreSource(t)
	service, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// This extra live/source difference has no admitted owned-field finding.
	service.Spec.Selector = map[string]string{"external": "change"}
	if _, err := f.client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.settle(t, 1)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatalf("unadmitted changes blocked restored source fields: %+v", f.closed)
	}
}

// Diff.Changed describes apply-normalized reconciliation; its complete source
// comparison remains authoritative for the admitted fields even if a dry-run
// would preserve a field whose manager changed after admission.
func TestClosureDriftUsesSourceFieldsWhenReconcileIsNoop(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	snapshot, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*snapshot}}, Source: planFakeSource{source: f.source}, SourceCommit: "accepted"}
	diff, err := view.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"})
	if err != nil {
		t.Fatal(err)
	}
	diff.Changed = false
	for _, finding := range f.c.currentIncidentRequest.Findings {
		if finding.DetectorID != "drift.unexplained" {
			continue
		}
		retained, _ := checkRetainedDrift(context.Background(), []sdk.Finding{finding}, driftReadView{diff: diff})
		if len(retained) != 1 {
			t.Fatal("status accepted apply normalization while admitted source fields differ")
		}
	}
	f.c.drift.View = func(sdk.DetectionContext) program.ReadView { return driftReadView{diff: diff} }
	f.step(t, 1)
	if len(f.closed) != 0 {
		t.Fatal("apply normalization hid an admitted live/source mismatch")
	}
}

// SDO social live regression: a removed extra selector key is absent on both
// sides of a complete source diff. Historical drift must clear like status.
func TestClosureDriftRemovedExtraFieldsAgreeWithStatus(t *testing.T) {
	for _, test := range []struct{ kind, path string }{
		{"Service", "spec.selector.injected"},
		{"ConfigMap", "metadata.labels.installer"},
		{"ConfigMap", "metadata.annotations.installer"},
		{"ConfigMap", "data.mongod.conf"},
	} {
		t.Run(test.kind+"/"+test.path, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app"}
			metadata := metav1.ObjectMeta{Name: "restored", Namespace: "app"}
			var object any
			if test.kind == "Service" {
				service := corev1.Service{ObjectMeta: metadata, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "backend"}}}
				snapshot.ServiceList = []corev1.Service{service}
				object = &service
			} else {
				config := corev1.ConfigMap{ObjectMeta: metadata, Data: map[string]string{"mongod.conf": "restored configuration"}}
				snapshot.ConfigMapList = []corev1.ConfigMap{config}
				object = &config
			}
			encoded, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
			if err != nil {
				t.Fatal(err)
			}
			source := &unstructured.Unstructured{Object: encoded}
			source.SetAPIVersion("v1")
			source.SetKind(test.kind)
			if test.kind == "Service" {
				snapshot.ServiceList[0].Annotations = withAcceptance(t, source, source)
			} else {
				snapshot.ConfigMapList[0].Annotations = withAcceptance(t, source, source)
			}
			event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: test.kind, Namespace: "app", Name: "restored"}, Fields: []sdk.DriftField{{Path: test.path, New: "injected"}}}
			finding := sdk.Finding{DetectorID: "drift.unexplained", Status: sdk.FindingActive, Fingerprint: test.path, PrimaryResource: event.Ref, Metadata: map[string]any{"drift_event": event}}
			viewFor := func(snapshot sdk.DetectionContext) program.ReadView {
				return LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			}
			view := viewFor(snapshot)
			diff, err := view.DiffAgainstSource(event.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if !driftFieldsRestored(diff, []sdk.DriftEvent{event}) {
				t.Fatal("restored field retained historical drift")
			}
			c := &Controller{drift: &DriftControllerConfig{View: viewFor}}
			clear, err := c.closureSourceRestored(context.Background(), snapshot, finding, []sdk.DriftEvent{event})
			if err != nil || !clear {
				t.Fatalf("controller verifier disagrees with clear source status: clear=%v err=%v", clear, err)
			}
			retained, _ := checkRetainedDrift(context.Background(), []sdk.Finding{finding}, view)
			if len(retained) != 0 {
				t.Fatalf("status retains restored fields: %+v", retained)
			}
		})
	}
}
