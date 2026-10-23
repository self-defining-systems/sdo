package program

import (
	"fmt"
	"strings"

	"sdo.dev/operator/sdk"
)

func validateObjectAction(action Action) error {
	var ref sdk.ObjectRef
	var reason string
	create, remove := false, false
	switch a := action.(type) {
	case CreateFromSource:
		ref, reason, create = a.Ref, a.Reason, true
	case DeleteObject:
		ref, reason, remove = a.Ref, a.Reason, true
	case *CreateFromSource:
		if a == nil {
			return fmt.Errorf("nil CreateFromSource")
		}
		return validateObjectAction(*a)
	case *DeleteObject:
		if a == nil {
			return fmt.Errorf("nil DeleteObject")
		}
		return validateObjectAction(*a)
	default:
		return nil
	}
	if ref.Name == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("object action requires a named reference and reason")
	}
	allowed := ref.Namespace != "" && (ref.APIVersion == "v1" && (ref.Kind == "ConfigMap" || ref.Kind == "Service"))
	if remove {
		allowed = (ref.Namespace != "" && ref.APIVersion == "networking.k8s.io/v1" && ref.Kind == "NetworkPolicy") || (ref.Namespace == "" && ref.APIVersion == "admissionregistration.k8s.io/v1" && (ref.Kind == "MutatingWebhookConfiguration" || ref.Kind == "ValidatingWebhookConfiguration"))
	}
	if !allowed {
		operation := "DeleteObject"
		if create {
			operation = "CreateFromSource"
		}
		return fmt.Errorf("%s refuses kind/scope %s/%s", operation, ref.APIVersion, ref.Kind)
	}
	return nil
}
