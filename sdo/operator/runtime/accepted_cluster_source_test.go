package runtime

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

func TestSourceIdentityWithoutConfigurationNeedsNoAdoptionWrite(t *testing.T) {
	controller, _, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	source.object = &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "app-identity", "namespace": "app"}}}
	client := fake.NewSimpleClientset()
	controller.drift.Client = client
	controller.drift.Source = source
	if err := controller.adoptOwnership(context.Background(), time.Unix(100, 0), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("identity-only source required API authority: %+v", client.Actions())
	}
}

type clusterEvidenceSnapshot struct {
	sdktest.Snapshot
	err error
}

func (s clusterEvidenceSnapshot) Object(sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, s.err
}

func TestAcceptedClusterSourceMissingObservationIsNotAbsence(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "old-role"}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not-captured", sdk.ErrObjectNotCaptured},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"}, ref.Name, errors.New("denied"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := LiveReadView{Carrier: inputs.New(clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: tc.err}), Source: planFakeSource{}, SourceCommit: "accepted"}
			_, err := view.snapshotObject(ref)
			if !errors.Is(err, tc.err) {
				t.Fatalf("missing observation became absence: %v", err)
			}
		})
	}
}

// Explicit deletion evidence, unlike an object omitted from an incomplete capture.
type confirmedAbsentSnapshot struct {
	sdktest.Snapshot
	Ref sdk.ObjectRef
}

func (s confirmedAbsentSnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref == s.Ref {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
	}
	return s.Snapshot.Object(ref)
}

