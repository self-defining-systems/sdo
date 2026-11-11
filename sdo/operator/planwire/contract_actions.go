package planwire

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// contractActionProto converts Escalate and ProposeSourceChange; ok is false
// for any other action.
func contractActionProto(action program.Action) (*contracts.Action, bool, error) {
	if escalation, ok := escalateAction(action); ok {
		refs := make([]*contracts.ObjectRef, 0, len(escalation.Resources))
		for _, ref := range escalation.Resources {
			refs = append(refs, refProto(ref))
		}
		return &contracts.Action{Kind: &contracts.Action_Escalate{Escalate: &contracts.Escalate{Reason: escalation.Reason, Resources: refs}}}, true, nil
	}
	if change, ok := program.SourceChangeAction(action); ok {
		value, err := planValueProto(change.Value)
		if err != nil {
			return nil, true, err
		}
		return &contracts.Action{Kind: &contracts.Action_ProposeSourceChange{ProposeSourceChange: &contracts.ProposeSourceChange{Ref: refProto(change.Ref), Path: change.Path, Value: value, Reason: change.Reason}}}, true, nil
	}
	return nil, false, nil
}

// contractActionState decodes Escalate and ProposeSourceChange, or returns nil.
func contractActionState(action *contracts.Action) program.Action {
	switch kind := action.GetKind().(type) {
	case *contracts.Action_Escalate:
		var refs []sdk.ObjectRef
		for _, ref := range kind.Escalate.GetResources() {
			refs = append(refs, objectRefState(ref))
		}
		return program.Escalate{Reason: kind.Escalate.GetReason(), Resources: refs}
	case *contracts.Action_ProposeSourceChange:
		a := kind.ProposeSourceChange
		return program.ProposeSourceChange{Ref: objectRefState(a.GetRef()), Path: a.GetPath(), Value: planValueState(a.GetValue()), Reason: a.GetReason()}
	}
	return nil
}

func escalateAction(action program.Action) (program.Escalate, bool) {
	switch a := action.(type) {
	case program.Escalate:
		return a, true
	case *program.Escalate:
		if a != nil {
			return *a, true
		}
	}
	return program.Escalate{}, false
}
