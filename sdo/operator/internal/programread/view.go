// Package programread forwards the SDK's read-only playbook interface across a
// process boundary without adding a second inventory of read methods.
package programread

//go:generate go run ./cmd/gen

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

type Message struct {
	Method           string                         `json:"method,omitempty"`
	Arguments        []json.RawMessage              `json:"arguments,omitempty"`
	Values           []json.RawMessage              `json:"values,omitempty"`
	Error            string                         `json:"error,omitempty"`
	UsageUnavailable *metrics.UsageUnavailableError `json:"usage_unavailable,omitempty"`
	ErrorClass       string                         `json:"error_class,omitempty"`
}

type View struct {
	mu       sync.Mutex
	Exchange func(Message) (Message, error)
}

var _ program.ReadView = (*View)(nil)

func (v *View) call(method string, args, outputs []any) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	request := Message{Method: method}
	for _, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			return err
		}
		request.Arguments = append(request.Arguments, data)
	}
	response, err := v.Exchange(request)
	if err != nil {
		return err
	}
	if len(response.Values) != len(outputs) && (response.Error == "" || len(response.Values) > 0) {
		return errors.New("invalid program read response")
	}
	for i, value := range response.Values {
		if err := json.Unmarshal(value, outputs[i]); err != nil {
			return err
		}
	}
	if response.UsageUnavailable != nil {
		return response.UsageUnavailable
	}
	if response.Error != "" {
		return sdk.ErrorWithClass(response.Error, response.ErrorClass)
	}
	return nil
}

func Read(view program.ReadView, request Message) (response Message) {
	defer func() {
		if r := recover(); r != nil {
			response = Message{Error: fmt.Sprintf("program read: %v", r)}
		}
	}()
	method, ok := reflect.TypeFor[program.ReadView]().MethodByName(request.Method)
	if !ok || view == nil || len(request.Arguments) != method.Type.NumIn() {
		return Message{Error: "invalid program read", ErrorClass: sdk.ReadClassInvalid}
	}
	args := make([]reflect.Value, len(request.Arguments))
	for i, raw := range request.Arguments {
		value := reflect.New(method.Type.In(i))
		if err := json.Unmarshal(raw, value.Interface()); err != nil {
			return Message{Error: err.Error(), ErrorClass: sdk.ReadClassInvalid}
		}
		args[i] = value.Elem()
	}
	values := reflect.ValueOf(view).MethodByName(request.Method).Call(args)
	for _, value := range values {
		if value.Type() == reflect.TypeFor[error]() {
			if !value.IsNil() {
				err := value.Interface().(error)
				response.Error = err.Error()
				response.ErrorClass = sdk.ReadErrorClass(err)
				var unavailable *metrics.UsageUnavailableError
				if errors.As(err, &unavailable) {
					response.UsageUnavailable = unavailable
				}
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
