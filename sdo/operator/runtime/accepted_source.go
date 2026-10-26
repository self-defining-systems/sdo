package runtime

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// acceptedIncidentManifest keeps cluster identity separate from the application
// namespace that owns the accepted render lock. Foreign source is never a grant.
func acceptedIncidentManifest(ctx context.Context, source PlanSource, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Namespace == "" && namespace != "" {
		if reader, ok := source.(interface {
			AcceptedApplicationManifest(context.Context, string, string, sdk.ObjectRef) (*unstructured.Unstructured, error)
		}); ok {
			object, err := reader.AcceptedApplicationManifest(ctx, revision, namespace, ref)
			if err != nil {
				return nil, err
			}
			if object == nil || !samePlanRef(object, ref) {
				return nil, fmt.Errorf("accepted source identity differs from requested object")
			}
			return object, nil
		}
		return nil, fmt.Errorf("application-qualified accepted source unavailable")
	}
	return source.AcceptedManifest(ctx, revision, ref)
}

func (e PlanExecutor) acceptedClusterReconcile(ctx context.Context, incident IncidentRequest, action program.Action, ref sdk.ObjectRef) bool {
	switch a := action.(type) {
	case program.ReconcileFromSource:
	case *program.ReconcileFromSource:
		if a == nil {
			return false
		}
	default:
		return false
	}
	if ref.Namespace != "" || e.Source == nil || incident.SourceCommit == "" {
		return false
	}
	_, err := acceptedIncidentManifest(ctx, e.Source, incident.SourceCommit, incident.Namespace, ref)
	return err == nil
}
