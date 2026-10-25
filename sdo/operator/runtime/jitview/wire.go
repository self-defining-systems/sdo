// Package jitview bridges compiled working programs to the controller's existing
// read-only view. Programs return plans; only the controller executes them.
package jitview

//go:generate go run ./cmd/gen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"reflect"
	"strings"

	"sdo.dev/operator/sdk/program"
)

type Message struct {
	Method    string            `json:"method,omitempty"`
	Arguments []json.RawMessage `json:"arguments,omitempty"`
	Values    []json.RawMessage `json:"values,omitempty"`
	Result    json.RawMessage   `json:"result,omitempty"`
	Error     string            `json:"error,omitempty"`
}

type View struct {
	Input  *json.Decoder
	Output *json.Encoder
}

var _ program.ReadView = (*View)(nil)

func (v *View) call(method string, args, outputs []any) error {
	request := Message{Method: method}
	for _, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			return err
		}
		request.Arguments = append(request.Arguments, data)
	}
	if err := v.Output.Encode(request); err != nil {
		return err
	}
	var response Message
	if err := v.Input.Decode(&response); err != nil {
		return err
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	if len(response.Values) != len(outputs) {
		return errors.New("invalid program read response")
	}
	for i, out := range outputs {
		if err := json.Unmarshal(response.Values[i], out); err != nil {
			return err
		}
	}
	return nil
}

// Run exchanges reads on private process pipes. Its view exposes no writers.
func Run(ctx context.Context, binary string, request any, view program.ReadView) (json.RawMessage, error) {
	command := exec.CommandContext(ctx, binary)
	input, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr strings.Builder
	command.Stderr = &stderr
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
			if message.Error != "" {
				return nil, errors.New(message.Error)
			}
			return message.Result, nil
		}
		response := read(view, message)
		if err = encoder.Encode(response); err != nil {
			return nil, err
		}
	}
}

func read(view program.ReadView, request Message) (response Message) {
	defer func() {
		if r := recover(); r != nil {
			response = Message{Error: fmt.Sprintf("program read: %v", r)}
		}
	}()
	method, ok := reflect.TypeFor[program.ReadView]().MethodByName(request.Method)
	if !ok || view == nil || len(request.Arguments) != method.Type.NumIn() {
		return Message{Error: "invalid program read"}
	}
	args := make([]reflect.Value, len(request.Arguments))
	for i, raw := range request.Arguments {
		value := reflect.New(method.Type.In(i))
		if err := json.Unmarshal(raw, value.Interface()); err != nil {
			return Message{Error: err.Error()}
		}
		args[i] = value.Elem()
	}
	values := reflect.ValueOf(view).MethodByName(request.Method).Call(args)
	for _, value := range values {
		if value.Type() == reflect.TypeFor[error]() {
			if !value.IsNil() {
				response.Error = value.Interface().(error).Error()
			}
			continue
		}
		raw, err := json.Marshal(value.Interface())
		if err != nil {
			return Message{Error: err.Error()}
		}
		response.Values = append(response.Values, raw)
	}
	return response
}
