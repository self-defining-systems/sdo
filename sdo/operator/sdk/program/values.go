package program

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"sdo.dev/operator/sdk"
)

// ValueOrigin identifies the authority from which an action value is resolved.
type ValueOrigin string

const (
	SourceValue    ValueOrigin = "source"
	ObservedValue  ValueOrigin = "observed"
	ParameterValue ValueOrigin = "parameter"
)

// Value is an opaque reference, never a resolved value or a Go literal.
// Its zero value is invalid. Only the controller resolves it at apply time.
type Value struct {
	origin ValueOrigin
	ref    sdk.ObjectRef
	path   string
	param  string
}

func FromSource(ref sdk.ObjectRef, path string) Value {
	return Value{origin: SourceValue, ref: ref, path: path}
}
func FromObserved(ref sdk.ObjectRef, path string) Value {
	return Value{origin: ObservedValue, ref: ref, path: path}
}
func FromParam(name string) Value   { return Value{origin: ParameterValue, param: name} }
func (v Value) Origin() ValueOrigin { return v.origin }
func (v Value) Ref() sdk.ObjectRef  { return v.ref }
func (v Value) Path() string        { return v.path }
func (v Value) Param() string       { return v.param }
func (v Value) Validate() error {
	switch v.origin {
	case ParameterValue:
		if strings.TrimSpace(v.param) == "" {
			return fmt.Errorf("value parameter name is required")
		}
	case SourceValue, ObservedValue:
		if strings.EqualFold(v.ref.Kind, "Secret") {
			return fmt.Errorf("Secret value resolution is forbidden")
		}
		if v.ref.APIVersion == "" || v.ref.Kind == "" || v.ref.Name == "" || v.path == "" {
			return fmt.Errorf("value requires a named typed reference and path")
		}
		if _, err := parseFieldPath(v.path); err != nil {
			return err
		}
	default:
		return fmt.Errorf("value requires source, observed or parameter provenance")
	}
	return nil
}

type valueWire struct {
	Origin ValueOrigin   `json:"origin"`
	Ref    sdk.ObjectRef `json:"ref,omitempty"`
	Path   string        `json:"path,omitempty"`
	Param  string        `json:"param,omitempty"`
}

func (v Value) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(valueWire{v.origin, v.ref, v.path, v.param})
}
func (v *Value) UnmarshalJSON(raw []byte) error {
	var w valueWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	candidate := Value{w.Origin, w.Ref, w.Path, w.Param}
	if err := candidate.Validate(); err != nil {
		return err
	}
	*v = candidate
	return nil
}

// ValueKind is the schema of a declared scalar value parameter.
type ValueKind string

const (
	StringValue   ValueKind = "string"
	IntValue      ValueKind = "int"
	QuantityValue ValueKind = "quantity"
	DurationValue ValueKind = "duration"
	BoolValue     ValueKind = "bool"
)

// ValidateValueBinding checks types without coercing strings to numbers or booleans.
// Quantities and durations use their Kubernetes and Go textual syntax.
func ValidateValueBinding(spec ParamSpec, value any) error {
	valid := false
	switch spec.ValueKind {
	case StringValue:
		_, valid = value.(string)
	case BoolValue:
		_, valid = value.(bool)
	case IntValue:
		if value != nil {
			switch reflect.TypeOf(value).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				valid = true
			case reflect.Float32, reflect.Float64:
				f := reflect.ValueOf(value).Convert(reflect.TypeOf(float64(0))).Float()
				valid = !math.IsInf(f, 0) && !math.IsNaN(f) && math.Trunc(f) == f
			}
			if n, ok := value.(json.Number); ok {
				_, err := n.Int64()
				valid = err == nil
			}
		}
	case QuantityValue:
		if s, ok := value.(string); ok {
			_, err := resource.ParseQuantity(s)
			valid = err == nil
		}
	case DurationValue:
		if s, ok := value.(string); ok {
			_, err := time.ParseDuration(s)
			valid = err == nil
		}
	}
	if !valid {
		return fmt.Errorf("parameter %q requires %s", spec.Name, spec.ValueKind)
	}
	return nil
}
