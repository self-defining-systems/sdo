package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// failingDiffView fails the source comparison of one object, as a dry run
// that succeeded during the adoption scan but fails during classification.
type failingDiffView struct {
	program.ReadView
	ref sdk.ObjectRef
}

func (v failingDiffView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if ref == v.ref {
		return program.Diff{}, errors.New("webhook timeout")
	}
	return v.ReadView.DiffAgainstSource(ref)
}

// Every per-object failure site of an adoption pass leaves only that object
// without an acceptance record: the healthy object still gets its record and
// the failure is typed, so no single object can stop the pass.
func TestAdoptionObjectFailureSitesAreIsolated(t *testing.T) {
	alpha := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "alpha"}
	beta := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "beta"}
	driftedAlpha := settledDeployment("alpha")
	driftedAlpha.Spec.Template.Spec.Containers[0].Image = "alpha:2"
	noUID := settledDeployment("alpha")
	noUID.UID = ""
	widget := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "example.com/v1", "kind": "Widget", "metadata": map[string]any{"name": "widget", "namespace": "app"}, "spec": map[string]any{"size": int64(1)}}}
	elsewhere := deploymentSource("alpha")
	elsewhere.SetNamespace("other")
	nth := func(resource string, failFrom int32, err error) func(*fake.Clientset) {
		return func(client *fake.Clientset) {
			var calls atomic.Int32
			verb := "patch"
			if resource == "get" {
				verb, resource = "get", "deployments"
			}
			client.PrependReactor(verb, resource, func(action ktesting.Action) (bool, kruntime.Object, error) {
				name := ""
				switch typed := action.(type) {
				case ktesting.PatchAction:
					name = typed.GetName()
				case ktesting.GetAction:
					name = typed.GetName()
				}
				if name != "alpha" || calls.Add(1) < failFrom {
					return false, nil, nil
				}
				return true, nil, err
			})
		}
	}
	tests := []struct {
		name    string
		failing sdk.ObjectRef
		alpha   kruntime.Object
		sources adoptionSources
		inject  func(*fake.Clientset)
		view    bool
		want    string
	}{
		{name: "unsupported kind", failing: sdk.ObjectRef{APIVersion: "example.com/v1", Kind: "Widget", Namespace: "app", Name: "widget"}, alpha: settledDeployment("alpha"), sources: adoptionSources{widget, deploymentSource("alpha"), deploymentSource("beta")}, want: "unsupported accepted manifest"},
		{name: "outside namespace", failing: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "alpha"}, alpha: settledDeployment("alpha"), sources: adoptionSources{elsewhere, deploymentSource("beta")}, want: "outside namespace"},
		{name: "live read", failing: alpha, alpha: settledDeployment("alpha"), sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, inject: nth("get", 1, errors.New("etcd timeout")), want: "read live Deployment/alpha"},
		{name: "dry run", failing: alpha, alpha: settledDeployment("alpha"), sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, inject: nth("deployments", 1, errors.New("unknown")), want: "dry run adoption of Deployment/alpha"},
		{name: "missing UID", failing: alpha, alpha: noUID, sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, want: "lacks UID or resourceVersion"},
		{name: "real apply", failing: alpha, alpha: settledDeployment("alpha"), sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, inject: nth("deployments", 2, errors.New("admission denied")), want: "adopt Deployment/alpha"},
		{name: "conflict re-read", failing: alpha, alpha: settledDeployment("alpha"), sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, inject: func(client *fake.Clientset) {
			nth("deployments", 2, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "alpha", errors.New("moved")))(client)
			var reads atomic.Int32
			client.PrependReactor("get", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if action.(ktesting.GetAction).GetName() == "alpha" && reads.Add(1) > 1 {
					return true, nil, errors.New("etcd timeout")
				}
				return false, nil, nil
			})
		}, want: "re-read Deployment/alpha after adoption conflict"},
		{name: "classification", failing: alpha, alpha: driftedAlpha, sources: adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}, view: true, want: "classify pre-adoption difference"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(test.alpha, settledDeployment("beta"))
			if test.inject != nil {
				test.inject(fakeClient)
			}
			controller, client := fakeAdoptionController(t, fakeClient, test.sources)
			if test.view {
				controller.ConfigureDrift(DriftControllerConfig{Source: test.sources, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
					return failingDiffView{ReadView: LiveReadView{DetectionContext: snapshot, Source: test.sources, SourceCommit: "accepted", Client: client}, ref: test.failing}
				}})
			}
			controller.adoptionNeeded = true
			err := controller.adoptOwnership(context.Background(), time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"})
			var objectErr *ObjectAdoptionError
			if !errors.As(err, &objectErr) || objectErr.Ref != test.failing || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("failure not reported as a typed per-object error containing %q: %v", test.want, err)
			}
			if !recordedOn(t, client, beta, deploymentSource("beta")) {
				t.Fatalf("failure of %s left the healthy Deployment unrecorded: %v", test.failing.Name, err)
			}
			controller.recordAdoptionError(time.Unix(11, 0).UTC(), err)
			if !controller.adoptionNeeded || controller.ownershipAdoption.RetryAt.IsZero() {
				t.Fatalf("failed object not retried: %+v", controller.ownershipAdoption)
			}
		})
	}
}
