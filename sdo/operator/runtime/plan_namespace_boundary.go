package runtime

import (
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Control credentials can impersonate the removal identity. Generic plans must
// never create workloads, request tokens, or execute commands in that namespace.
// This applies at the shared client boundary to authored programs and raw APIs.
func (c TypedPlanClient) checkPlanNamespace(namespace string) error {
	if c.ProtectedNamespace != "" && namespace == c.ProtectedNamespace {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, namespace, fmt.Errorf("generic plans cannot access the SDO control namespace"))
	}
	return nil
}
