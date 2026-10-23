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

func validateRuntimeAction(action Action) error {
	switch a := action.(type) {
	case DeletePod:
		if a.Ref.APIVersion != "v1" || a.Ref.Kind != "Pod" || a.Ref.Name == "" || a.Ref.Namespace == "" {
			return fmt.Errorf("DeletePod requires one named v1 Pod")
		}
	case ScaleWorkload:
		if a.Ref.APIVersion != "apps/v1" || (a.Ref.Kind != "Deployment" && a.Ref.Kind != "StatefulSet") || a.Ref.Name == "" || a.Ref.Namespace == "" {
			return fmt.Errorf("ScaleWorkload requires a named Deployment or StatefulSet")
		}
		return a.Replicas.Validate()
	case Exec:
		if a.Pod.APIVersion != "v1" || a.Pod.Kind != "Pod" || a.Pod.Namespace == "" || a.Pod.Name == "" || a.Container == "" || len(a.Command) == 0 {
			return fmt.Errorf("Exec requires one named Pod, container and command")
		}
		for _, v := range a.Command {
			if err := v.Validate(); err != nil {
				return err
			}
		}
	case *DeletePod:
		if a == nil {
			return fmt.Errorf("nil DeletePod")
		}
		return validateRuntimeAction(*a)
	case *ScaleWorkload:
		if a == nil {
			return fmt.Errorf("nil ScaleWorkload")
		}
		return validateRuntimeAction(*a)
	case *Exec:
		if a == nil {
			return fmt.Errorf("nil Exec")
		}
		return validateRuntimeAction(*a)
	}
	return nil
}
