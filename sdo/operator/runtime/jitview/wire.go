// Package jitview bridges compiled working programs to the controller's existing
// read-only view. Programs return plans; only the controller executes them.
package jitview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sdo.dev/operator/runtime/internal/lifecycleio"

	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk/program"
)

// StructuralPlanError preserves terminal plan defects across the process boundary.
type StructuralPlanError struct{ Message string }

func (e *StructuralPlanError) Error() string { return e.Message }

type Message struct {
	programread.Message
	StructuralPlanFailure string          `json:"structural_plan_failure,omitempty"`
	Result                json.RawMessage `json:"result,omitempty"`
}

func PipeView(input *json.Decoder, output *json.Encoder) *programread.View {
	return &programread.View{Exchange: func(request programread.Message) (programread.Message, error) {
		if err := output.Encode(request); err != nil {
			return programread.Message{}, err
		}
		var response programread.Message
		err := input.Decode(&response)
		return response, err
	}}
}

// Run exchanges reads on private process pipes. Its view exposes no writers.
func Run(ctx context.Context, binary string, request any, view program.ReadView) (json.RawMessage, error) {
	command := lifecycleio.Command(ctx, binary)
	input, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	command.Stderr = io.Discard
	if err = command.Start(); err != nil {
		return nil, err
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	encoder := json.NewEncoder(input)
	decoder := json.NewDecoder(io.LimitReader(output, 32<<20))
	if err = encoder.Encode(request); err != nil {
		return nil, err
	}
	for {
		var message Message
		if err = decoder.Decode(&message); err != nil {
			return nil, fmt.Errorf("working program stopped: %w", err)
		}
		if message.Method == "" {
			if message.StructuralPlanFailure != "" {
				return nil, &StructuralPlanError{Message: message.StructuralPlanFailure}
			}
			if message.Error != "" {
				return nil, errors.New(message.Error)
			}
			return message.Result, nil
		}
		response := programread.Read(view, message.Message)
		if err = encoder.Encode(response); err != nil {
			return nil, err
		}
	}
}
