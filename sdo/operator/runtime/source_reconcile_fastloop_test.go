package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
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
