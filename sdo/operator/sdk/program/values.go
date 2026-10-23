package program

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	bound  *ValueInputs
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
		if strings.TrimSpace(v.param) == "" || v.ref != (sdk.ObjectRef{}) || v.path != "" {
			return fmt.Errorf("value parameter name is required")
		}
	case SourceValue, ObservedValue:
		if v.param != "" {
			return fmt.Errorf("object-derived value cannot include a parameter name")
		}
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
	Origin ValueOrigin    `json:"origin"`
	Ref    *sdk.ObjectRef `json:"ref,omitempty"`
	Path   string         `json:"path,omitempty"`
	Param  string         `json:"param,omitempty"`
}

func (v Value) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	var ref *sdk.ObjectRef
	if v.origin != ParameterValue {
		ref = &v.ref
	}
	return json.Marshal(valueWire{v.origin, ref, v.path, v.param})
}
func (v *Value) UnmarshalJSON(raw []byte) error {
	var w valueWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&w); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing value reference data")
	}
	var ref sdk.ObjectRef
	if w.Ref != nil {
		ref = *w.Ref
	}
	candidate := Value{origin: w.Origin, ref: ref, path: w.Path, param: w.Param}
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

// ValueInputs is supplied by the controller, never read from a durable receipt.
// Observed must read the same immutable ReadView snapshot used by Mitigate.
type ValueInputs struct {
	SourceCommit string
	Source       func(sdk.ObjectRef) (map[string]any, error)
	Observed     func(sdk.ObjectRef) (map[string]any, error)
	Spec         ProgramSpec
	Bindings     Bindings
}

// ValueProvenance records authority without copying a resolved value.
type ValueProvenance struct {
	Origin          ValueOrigin   `json:"origin"`
	Ref             sdk.ObjectRef `json:"ref,omitempty"`
	Path            string        `json:"path,omitempty"`
	SourceCommit    string        `json:"source_commit,omitempty"`
	ResourceVersion string        `json:"resource_version,omitempty"`
	Parameter       string        `json:"parameter,omitempty"`
}

func ResolveValue(v Value, in ValueInputs) (any, ValueProvenance, error) {
	p := ValueProvenance{Origin: v.origin, Ref: v.ref, Path: v.path, Parameter: v.param}
	if err := v.Validate(); err != nil {
		return nil, p, err
	}
	if v.bound != nil {
		in.Spec = v.bound.Spec
		in.Bindings = v.bound.Bindings
		in.Observed = v.bound.Observed
	}
	if v.origin == ParameterValue {
		value, err := in.Bindings.Parameter(in.Spec, v.param)
		return value, p, err
	}
	var object map[string]any
	var err error
	switch v.origin {
	case SourceValue:
		if in.Source == nil || in.SourceCommit == "" {
			return nil, p, fmt.Errorf("accepted source is required for value")
		}
		object, err = in.Source(v.ref)
		p.SourceCommit = in.SourceCommit
	case ObservedValue:
		if in.Observed == nil {
			return nil, p, fmt.Errorf("program observation snapshot is required for value")
		}
		object, err = in.Observed(v.ref)
	}
	if err != nil || object == nil {
		return nil, p, fmt.Errorf("value object unavailable")
	}
	if strings.EqualFold(fmt.Sprint(object["kind"]), "Secret") {
		return nil, p, fmt.Errorf("Secret value resolution is forbidden")
	}
	if v.origin == ObservedValue {
		meta, _ := object["metadata"].(map[string]any)
		p.ResourceVersion, _ = meta["resourceVersion"].(string)
		if p.ResourceVersion == "" {
			return nil, p, fmt.Errorf("observed value requires resourceVersion")
		}
	}
	value, err := FieldValue(object, v.path)
	if err != nil {
		return nil, p, fmt.Errorf("value path unavailable")
	}
	return value, p, nil
}

// ResolveSetFieldValue selects a document entry only for object-derived values;
// declared parameters already describe the entry's schema value.
func ResolveSetFieldValue(a SetField, in ValueInputs) (any, ValueProvenance, error) {
	value, p, err := ResolveValue(a.Value, in)
	if err == nil && a.DocumentFormat != "" && a.Value.Origin() != ParameterValue {
		var doc map[string]any
		doc, err = planDocument(value, a.DocumentFormat)
		if err == nil {
			value, err = documentEntry(doc, a.DocumentPath)
		}
	}
	return value, p, err
}

// BindPlanValues pins each value to its own program invocation. This survives
// batching plans with different parameter names or observation snapshots.
// The controller calls this after Mitigate returns, replacing any program binding.
func BindPlanValues(plan Plan, in ValueInputs) Plan {
	out := plan
	out.Actions = make([]Action, len(plan.Actions))
	bind := func(v Value) Value { v.bound = &in; return v }
	for i, action := range plan.Actions {
		switch a := action.(type) {
		case SetField:
			a.Value = bind(a.Value)
			out.Actions[i] = a
		case ScaleWorkload:
			a.Replicas = bind(a.Replicas)
			out.Actions[i] = a
		case Exec:
			a.Command = append([]Value(nil), a.Command...)
			for j := range a.Command {
				a.Command[j] = bind(a.Command[j])
			}
			out.Actions[i] = a
		case *SetField:
			if a != nil {
				copy := *a
				copy.Value = bind(copy.Value)
				out.Actions[i] = copy
			} else {
				out.Actions[i] = action
			}
		case *ScaleWorkload:
			if a != nil {
				copy := *a
				copy.Replicas = bind(copy.Replicas)
				out.Actions[i] = copy
			} else {
				out.Actions[i] = action
			}
		case *Exec:
			if a != nil {
				copy := BindPlanValues(Plan{Actions: []Action{*a}}, in)
				out.Actions[i] = copy.Actions[0]
			} else {
				out.Actions[i] = action
			}
		default:
			out.Actions[i] = action
		}
	}
	return out
}
