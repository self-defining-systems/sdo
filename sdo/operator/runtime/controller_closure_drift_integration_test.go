//go:build integration

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
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

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
	f.cache, err = newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: f.client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace, config.FiringThreshold, config.ClearThreshold = "app", 1, 1
	// These fixtures test source/rollout ownership, not budget expiry. The
	// verification budget must contain the unchanged 15-second health window.
	config.VerificationTimeout = time.Minute
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
		return LiveReadView{Carrier: inputs.New(s), Source: planFakeSource{source: f.source}, SourceCommit: "accepted"}
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

// Health decides closure. Drift that remains on the object (an owned field the
// responder did not restore) closes as success and stays on the outcome.
func TestClosureAdmittedDriftContracts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		owned, declared bool
		namespace       string
		repair          bool
		driftRecorded   bool
	}{
		{"image_only", true, true, "app", false, true},
		{"both_fixed", true, true, "app", true, false},
		{"unowned", false, true, "app", false, false},
		{"undeclared", true, false, "app", false, false},
		{"other_namespace", true, true, "other", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClosureDriftFixture(t, tc.owned, tc.declared, tc.namespace)
			if tc.repair {
				f.restoreSource(t)
			}
			f.settle(t, 1)
			if len(f.closed) != 1 {
				t.Fatalf("closures=%d", len(f.closed))
			}
			if got := closureOutcome(t, f.closed[0]); got != "success" {
				t.Fatalf("classification=%s want success", got)
			}
			if tc.driftRecorded {
				derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), f.closed[0], "repair", "codex", "fixture")
				if err != nil {
					t.Fatal(err)
				}
				if len(derived.Record.GetDriftEvents()) != 1 {
					t.Fatalf("outcome lost remaining drift: %v", derived.Record)
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
// 8087. Health is clear, so the incident closes; the routing difference stays on
// the outcome as information. No experiment captures or oracle verdicts are
// inputs to the controller.
func TestClosureL3I04ReductionClosesOnHealthWithDriftRecorded(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.settle(t, 1)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatalf("health-clear incident did not close: %+v", f.closed)
	}
	event := f.closed[0].Request.DriftEvents[0]
	if len(event.Fields) != 1 || event.Fields[0].New != "9999" || event.Fields[0].Source != "8087" {
		t.Fatalf("routing evidence=%+v", event)
	}
}

// A source that cannot be read is information about the drift, never a reason
// to hold a health-verified closure.
func TestClosureDriftSourceErrorDoesNotHoldClosure(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.c.drift.View = func(sdk.DetectionContext) program.ReadView { return failedSourceDriftView{} }
	f.settle(t, 1)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatal(fmt.Sprint(f.closed))
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
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*snapshot}}), Source: planFakeSource{source: f.source}, SourceCommit: "accepted"}
	diff, err := view.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"})
	if err != nil {
		t.Fatal(err)
	}
	diff.Changed = false
	for _, finding := range f.c.currentIncidentRequest.Findings {
		if finding.DetectorID != "drift.unexplained" {
			continue
		}
		retained := retainedDrift([]sdk.Finding{finding}, driftReadView{diff: diff})
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

