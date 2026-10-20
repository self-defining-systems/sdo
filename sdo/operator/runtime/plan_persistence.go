package runtime

import (
	"encoding/json"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

// The program SDK uses an Action interface. Persist its concrete kind so a
// restored application can still identify target refs and roll back.
type persistedAction struct {
	Kind   string          `json:"kind"`
	Action json.RawMessage `json:"action"`
}

type persistedPlan struct {
	Actions   []persistedAction `json:"Actions"`
	Rationale string            `json:"Rationale"`
}

type planApplicationFields PlanApplicationResult

func (result PlanApplicationResult) MarshalJSON() ([]byte, error) {
	plan, err := encodePersistedActions(result.Plan.Actions)
	if err != nil {
		return nil, fmt.Errorf("encode plan: %w", err)
	}
	applied, err := encodePersistedActions(result.ActionsApplied)
	if err != nil {
		return nil, fmt.Errorf("encode applied actions: %w", err)
	}
	return json.Marshal(struct {
		planApplicationFields
		Plan           persistedPlan     `json:"Plan"`
		ActionsApplied []persistedAction `json:"ActionsApplied"`
	}{planApplicationFields(result), persistedPlan{Actions: plan, Rationale: result.Plan.Rationale}, applied})
}

func (result *PlanApplicationResult) UnmarshalJSON(data []byte) error {
	var encoded struct {
		planApplicationFields
		Plan           persistedPlan     `json:"Plan"`
		ActionsApplied []persistedAction `json:"ActionsApplied"`
	}
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	plan, err := decodePersistedActions(encoded.Plan.Actions)
	if err != nil {
		return fmt.Errorf("decode plan: %w", err)
	}
	applied, err := decodePersistedActions(encoded.ActionsApplied)
	if err != nil {
		return fmt.Errorf("decode applied actions: %w", err)
	}
	*result = PlanApplicationResult(encoded.planApplicationFields)
	result.Plan = program.Plan{Actions: plan, Rationale: encoded.Plan.Rationale}
	result.ActionsApplied = applied
	return nil
}

func encodePersistedActions(actions []program.Action) ([]persistedAction, error) {
	if actions == nil {
		return nil, nil
	}
	encoded := make([]persistedAction, 0, len(actions))
	for _, action := range actions {
		var kind string
		switch action.(type) {
		case program.ReconcileFromSource, *program.ReconcileFromSource:
			kind = "reconcile_from_source"
		case program.Rollback, *program.Rollback:
			kind = "rollback"
		case program.SetField, *program.SetField:
			kind = "set_field"
		case program.RestartRollout, *program.RestartRollout:
			kind = "restart_rollout"
		default:
			return nil, fmt.Errorf("unsupported plan action %T", action)
		}
		payload, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("encode %s action: %w", kind, err)
		}
		if string(payload) == "null" {
			return nil, fmt.Errorf("nil %s action", kind)
		}
		encoded = append(encoded, persistedAction{Kind: kind, Action: payload})
	}
	return encoded, nil
}

func decodePersistedActions(encoded []persistedAction) ([]program.Action, error) {
	if encoded == nil {
		return nil, nil
	}
	decoded := make([]program.Action, 0, len(encoded))
	for _, entry := range encoded {
		var action program.Action
		switch entry.Kind {
		case "reconcile_from_source":
			action = &program.ReconcileFromSource{}
		case "rollback":
			action = &program.Rollback{}
		case "set_field":
			action = &program.SetField{}
		case "restart_rollout":
			action = &program.RestartRollout{}
		default:
			return nil, fmt.Errorf("unknown plan action kind %q", entry.Kind)
		}
		if len(entry.Action) == 0 || string(entry.Action) == "null" {
			return nil, fmt.Errorf("missing %s action", entry.Kind)
		}
		if err := json.Unmarshal(entry.Action, action); err != nil {
			return nil, fmt.Errorf("decode %s action: %w", entry.Kind, err)
		}
		switch value := action.(type) {
		case *program.ReconcileFromSource:
			decoded = append(decoded, *value)
		case *program.Rollback:
			decoded = append(decoded, *value)
		case *program.SetField:
			decoded = append(decoded, *value)
		case *program.RestartRollout:
			decoded = append(decoded, *value)
		}
	}
	return decoded, nil
}
