package runtime

import (
	"context"
	"testing"
	"time"

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
				return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
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
			retained := retainedDrift([]sdk.Finding{finding}, view)
			if len(retained) != 0 {
				t.Fatalf("status retains restored fields: %+v", retained)
			}
		})
	}
}

