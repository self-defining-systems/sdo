package program

import (
	"fmt"
	"sdo.dev/operator/sdk"
)

// AtomicWriteConflictError refuses a plan whose object edits cannot be combined
// without moving them across an opaque runtime or object-lifecycle effect.
type AtomicWriteConflictError struct{ Ref sdk.ObjectRef }

func (e *AtomicWriteConflictError) Error() string {
	return fmt.Sprintf("object edits to %s/%s require one atomic write; separate runtime and lifecycle effects into another plan", e.Ref.Kind, e.Ref.Name)
}

// DocumentWriteRef identifies actions that project edits onto an existing object.
// Their relative order within that object is preserved in its single API write.
func DocumentWriteRef(action Action) (sdk.ObjectRef, bool) {
	switch a := action.(type) {
	case SetField:
		return a.Ref, true
	case ScaleWorkload:
		return a.Ref, true
	case ReconcileFromSource:
		return a.Ref, true
	case Rollback:
		return a.Ref, true
	case RestartRollout:
		return a.Ref, true
	case RemoveServicePort:
		return a.Ref, true
	case PatchObject:
		return a.Ref, true
	case ApplyObject:
		return a.Ref, a.Mode == "" || a.Mode == "apply"
	case *SetField:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *ScaleWorkload:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *ReconcileFromSource:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *Rollback:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *RestartRollout:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *RemoveServicePort:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *PatchObject:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	case *ApplyObject:
		if a != nil {
			return DocumentWriteRef(*a)
		}
	}
	return sdk.ObjectRef{}, false
}

func validateAtomicWrites(p Plan) error {
	counts := map[sdk.ObjectRef]int{}
	documentsOnly := true
	for _, a := range p.Actions {
		ref, ok := DocumentWriteRef(a)
		documentsOnly = documentsOnly && ok
		if ok {
			counts[ref]++
		}
	}
	if !documentsOnly {
		for ref, count := range counts {
			if count > 1 {
				return &AtomicWriteConflictError{Ref: ref}
			}
		}
	}
	return nil
}
