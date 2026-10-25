package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"sdo.dev/operator/runtime/jitview"
	"sdo.dev/operator/sdk/program"
)

type jitRequest struct {
	Command   string           `json:"command"`
	ProgramID string           `json:"program_id,omitempty"`
	Bindings  program.Bindings `json:"bindings,omitempty"`
}
type jitSpec struct {
	Spec        program.ProgramSpec `json:"spec"`
	CanMitigate bool                `json:"can_mitigate"`
}

// ServeWorkingProgram is the entrypoint of a builder-generated Go program.
// It has a read-only RPC view, and returns ordinary typed executor plans.
func ServeWorkingProgram(registry program.ProgramRegistry) {
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	reply := func(result any, err error) {
		message := jitview.Message{}
		if err != nil {
			message.Error = err.Error()
		} else {
			message.Result, err = json.Marshal(result)
			if err != nil {
				message.Error = err.Error()
			}
		}
		_ = encoder.Encode(message)
	}
	defer func() {
		if r := recover(); r != nil {
			reply(nil, fmt.Errorf("working program panic: %v", r))
		}
	}()
	var request jitRequest
	if err := decoder.Decode(&request); err != nil {
		reply(nil, err)
		return
	}
	if request.Command == "spec" {
		var specs []jitSpec
		for _, spec := range registry.List() {
			_, ok := registry.Mitigator(spec.ID)
			specs = append(specs, jitSpec{Spec: spec, CanMitigate: ok})
		}
		reply(specs, nil)
		return
	}
	view := &jitview.View{Input: decoder, Output: encoder}
	switch request.Command {
	case "diagnose":
		p, ok := registry.Diagnoser(request.ProgramID)
		if !ok {
			reply(nil, fmt.Errorf("unknown working program %q", request.ProgramID))
			return
		}
		verdict, err := p.Diagnose(context.Background(), view, request.Bindings)
		reply(verdict, err)
	case "mitigate":
		p, ok := registry.Mitigator(request.ProgramID)
		if !ok {
			reply(nil, fmt.Errorf("working program %q has no mitigator", request.ProgramID))
			return
		}
		plan, err := p.Mitigate(context.Background(), view, request.Bindings)
		reply(PlanApplicationResult{Plan: plan}, err)
	default:
		reply(nil, fmt.Errorf("unknown working program operation"))
	}
}
