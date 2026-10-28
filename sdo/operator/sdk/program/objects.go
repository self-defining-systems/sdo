package program

import (
	"fmt"
	"strings"

	"sdo.dev/operator/sdk"
)

func validateObjectAction(action Action) error {
	var ref sdk.ObjectRef
	var reason string
	switch a := action.(type) {
	case CreateFromSource:
		ref, reason = a.Ref, a.Reason
	case DeleteObject:
		ref, reason = a.Ref, a.Reason
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
	if ref.APIVersion == "" || ref.Kind == "" || (ref.Namespace == "" && !isDeleteObject(action)) {
		return fmt.Errorf("object action requires a namespaced object reference")
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
		if a.Ref.APIVersion != "apps/v1" || (a.Ref.Kind != "Deployment" && a.Ref.Kind != "StatefulSet" && a.Ref.Kind != "ReplicaSet") || a.Ref.Name == "" || a.Ref.Namespace == "" {
			return fmt.Errorf("ScaleWorkload requires a named Deployment, StatefulSet or ReplicaSet")
		}
		return a.Replicas.Validate()
	case RetireOldRevision:
		if a.Ref.APIVersion != "apps/v1" || a.Ref.Kind != "ReplicaSet" || a.Ref.Name == "" || a.Ref.Namespace == "" {
			return fmt.Errorf("RetireOldRevision requires one named apps/v1 ReplicaSet")
		}
		return nil
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
	case *RetireOldRevision:
		if a == nil {
			return fmt.Errorf("nil RetireOldRevision")
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

func isDeleteObject(a Action) bool {
	switch a.(type) {
	case DeleteObject, *DeleteObject:
		return true
	}
	return false
}
