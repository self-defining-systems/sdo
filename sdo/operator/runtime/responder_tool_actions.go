package runtime

import (
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type recordedToolAction struct {
	Kind   string        `json:"kind"`
	Ref    sdk.ObjectRef `json:"ref"`
	Reason string        `json:"reason,omitempty"`
}

func recordedActionReason(reason string) string {
	// Object and patch documents are never evidence, including when pasted
	// into the free-text reason. Apply the existing text redactor to prose.
	if strings.ContainsAny(reason, "{}[]\n\r") {
		return "[redacted]"
	}
	return redactDetectText(reason, 256)
}

func (l *ResponderLoop) recordedToolActions(request ToolRequest, response ToolResponse, applied []selectedPlan) []recordedToolAction {
	var out []recordedToolAction
	addPlan := func(plan program.Plan) {
		for _, action := range plan.Actions {
			wire, err := planActionProto(action)
			if err != nil || wire == nil {
				continue
			}
			message := wire.ProtoReflect()
			field := message.WhichOneof(message.Descriptor().Oneofs().ByName("kind"))
			if field == nil {
				continue
			}
			entry := message.Get(field).Message()
			item := recordedToolAction{Kind: string(entry.Descriptor().Name())}
			item.Ref, _ = planActionRef(action)
			if reason := entry.Descriptor().Fields().ByName("reason"); reason != nil {
				item.Reason = recordedActionReason(entry.Get(reason).String())
			}
			out = append(out, item)
		}
	}
	switch request.Command {
	case "plan":
		addPlan(l.plans[response.PlanID].plan)
	case "apply":
		for _, plan := range applied {
			addPlan(plan.plan)
		}
	case "rollback":
		addPlan(l.applications[request.ApplicationID].Plan)
	case "mitigate":
		// Accepted menu applications have real plan actions. Rejections
		// retain only identity and reason from the declared menu parameters.
		for _, round := range response.MenuExecution {
			if application, ok := l.applications[round.ApplicationID]; ok {
				addPlan(application.Plan)
			}
		}
		if len(out) == 0 {
			actions := rawMenuActions()
			addTarget := func(name, refText, reason string) {
				entry, ok := actions[name]
				if !ok {
					return
				}
				ref, _ := sdk.ParseObjectRef(refText)
				out = append(out, recordedToolAction{Kind: string(entry.action.Message().Name()), Ref: ref, Reason: recordedActionReason(reason)})
			}
			switch {
			case len(request.Targets) > 0:
				for _, target := range request.Targets {
					addTarget(target.Name, firstBindValue(target.Parameters["ref"]), firstBindValue(target.Parameters["reason"]))
				}
			case request.Selection != nil:
				addTarget(request.Selection.GetOptionId(), request.Parameters["ref"], request.Parameters["reason"])
			case request.SelectionSet != nil:
				for _, option := range request.SelectionSet.GetOptions() {
					addTarget(option.GetOptionId(), option.GetParameters()["ref"], option.GetParameters()["reason"])
				}
			}
		}
	}
	return out
}

func firstBindValue(values BindValues) string {
	if len(values) == 1 {
		return values[0]
	}
	return ""
}
