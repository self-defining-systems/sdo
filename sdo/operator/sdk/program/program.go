package program

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
)

type Kind string

type ProgramSpec struct {
	ID     string
	Params []ParamSpec
}

type ParamSpec struct {
	Name     string
	Kind     Kind
	Required bool
}

type Bindings map[string]sdk.ObjectRef

func ValidateBindings(spec ProgramSpec, b Bindings) error {
	params := make(map[string]ParamSpec, len(spec.Params))
	for _, param := range spec.Params {
		params[param.Name] = param
		if param.Required {
			if _, ok := b[param.Name]; !ok {
				return fmt.Errorf("required parameter %q is unbound", param.Name)
			}
		}
	}
	for name, ref := range b {
		param, ok := params[name]
		if !ok {
			return fmt.Errorf("unknown parameter %q", name)
		}
		if ref.Kind != string(param.Kind) {
			return fmt.Errorf("parameter %q: kind %q, want %q", name, ref.Kind, param.Kind)
		}
	}
	return nil
}

type Diagnoser interface {
	Spec() ProgramSpec
	Diagnose(context.Context, ReadView, Bindings) (Verdict, error)
}

type Mitigator interface {
	Spec() ProgramSpec
	Mitigate(context.Context, ReadView, Bindings) (Plan, error)
}

type ReadView interface {
	sdk.DetectionContext
	Logs(pod sdk.ObjectRef, container string, since time.Duration) (string, error)
	ProbeTCP(to sdk.ObjectRef, port int32) (ProbeResult, error)
	ProbeHTTP(to sdk.ObjectRef, req HTTPProbe) (ProbeResult, error)
	DiffAgainstSource(ref sdk.ObjectRef) (Diff, error)
	DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error)
}

type ProbeResult struct {
	Success bool   `json:"success"`
	Detail  string `json:"detail,omitempty"`
}

type HTTPProbe struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
}

type Diff struct {
	// SourceAbsent is true only after a successful accepted-source lookup finds no object.
	SourceAbsent bool   `json:"source_absent,omitempty"`
	Changed      bool   `json:"changed"`
	Text         string `json:"text,omitempty"`
}

type Result string

const (
	Match          Result = "Match"
	NoMatch        Result = "NoMatch"
	Unknown        Result = "Unknown"
	NeedsJudgement Result = "NeedsJudgement"
)

type Evidence struct {
	Resource sdk.ObjectRef `json:"resource"`
	Fact     string        `json:"fact"`
	Observed string        `json:"observed"`
	Expected string        `json:"expected"`
}

type Verdict struct {
	Result   Result     `json:"result"`
	Evidence []Evidence `json:"evidence,omitempty"`
	Question string     `json:"question,omitempty"`
}

func (v Verdict) Validate() error {
	switch v.Result {
	case Match, NoMatch, Unknown:
		if v.Question != "" {
			return fmt.Errorf("question requires NeedsJudgement")
		}
	case NeedsJudgement:
		if v.Question == "" {
			return fmt.Errorf("NeedsJudgement requires a question")
		}
	default:
		return fmt.Errorf("invalid verdict result %q", v.Result)
	}
	return nil
}

type Action interface{ action() }

type ReconcileFromSource struct{ Ref sdk.ObjectRef }

func (ReconcileFromSource) action() {}

type Rollback struct {
	Ref        sdk.ObjectRef
	ToRevision string
}

func (Rollback) action() {}

type SetField struct {
	Ref    sdk.ObjectRef
	Path   string
	Value  any
	Reason string
}

func (SetField) action() {}

type RestartRollout struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (RestartRollout) action() {}

type Plan struct {
	Actions   []Action
	Rationale string
}

// Allowed SetField paths use dotted Kubernetes field names and numeric list
// indexes. The patterns are:
//   - spec.ports[N].{port,targetPort,protocol,name}
//   - spec.selector[.KEY] and spec.replicas
//   - spec.template.spec.{containers,initContainers}[N].image
//   - spec.template.spec.{containers,initContainers}[N].ports[N].{containerPort,hostPort,protocol,name}
//   - spec.template.spec.{containers,initContainers}[N].{livenessProbe,readinessProbe,startupProbe,env[N],resources}[.FIELD...]
//
// N is a numeric list index. A path must match a whole pattern.
var allowedFieldPaths = []*regexp.Regexp{
	regexp.MustCompile(`^spec\.ports\[[0-9]+\]\.(port|targetPort|protocol|name)$`),
	regexp.MustCompile(`^spec\.selector(\.[A-Za-z0-9_./-]+)?$`),
	regexp.MustCompile(`^spec\.replicas$`),
	regexp.MustCompile(`^spec\.template\.spec\.(containers|initContainers)\[[0-9]+\]\.(image|ports\[[0-9]+\]\.(containerPort|hostPort|protocol|name)|((liveness|readiness|startup)Probe|env\[[0-9]+\]|resources)(\.[A-Za-z0-9_./-]+)*)$`),
}

func (p Plan) Validate() error {
	for _, action := range p.Actions {
		if action == nil {
			return fmt.Errorf("nil action")
		}
		field, ok := action.(SetField)
		if !ok {
			if pointer, isPointer := action.(*SetField); isPointer && pointer != nil {
				field, ok = *pointer, true
			}
		}
		if !ok {
			continue
		}
		allowed := false
		for _, pattern := range allowedFieldPaths {
			if pattern.MatchString(field.Path) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("SetField path %q is not allowed", field.Path)
		}
	}
	return nil
}

func RunDiagnose(ctx context.Context, p Diagnoser, v ReadView, b Bindings, deadline time.Duration) (verdict Verdict) {
	defer func() {
		if recovered := recover(); recovered != nil {
			verdict = unknown(fmt.Errorf("panic: %v", recovered))
		}
	}()
	if err := ValidateBindings(p.Spec(), b); err != nil {
		return unknown(err)
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	type outcome struct {
		verdict Verdict
		err     error
	}
	result := make(chan outcome, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- outcome{err: fmt.Errorf("panic: %v", recovered)}
			}
		}()
		checked := &checkedReadView{ReadView: v}
		verdict, err := p.Diagnose(ctx, checked, b)
		if readErr := checked.failure(); readErr != nil {
			err = readErr
		}
		result <- outcome{verdict, err}
	}()
	select {
	case <-ctx.Done():
		return unknown(ctx.Err())
	case got := <-result:
		if got.err != nil {
			return unknown(got.err)
		}
		if err := got.verdict.Validate(); err != nil {
			return unknown(err)
		}
		return got.verdict
	}
}

func unknown(err error) Verdict {
	return Verdict{Result: Unknown, Evidence: []Evidence{{Fact: "diagnosis", Observed: err.Error(), Expected: "successful diagnosis"}}}
}

// checkedReadView prevents an unavailable observation from being treated as clearance.
type checkedReadView struct {
	ReadView
	mu  sync.Mutex
	err error
}

func (v *checkedReadView) check(err error) {
	if err != nil {
		v.mu.Lock()
		defer v.mu.Unlock()
		if v.err == nil {
			v.err = err
		}
	}
}
func (v *checkedReadView) failure() error { v.mu.Lock(); defer v.mu.Unlock(); return v.err }
func (v *checkedReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	result, err := v.ReadView.Logs(ref, container, since)
	v.check(err)
	return result, err
}
func (v *checkedReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (ProbeResult, error) {
	result, err := v.ReadView.ProbeTCP(ref, port)
	v.check(err)
	return result, err
}
func (v *checkedReadView) ProbeHTTP(ref sdk.ObjectRef, request HTTPProbe) (ProbeResult, error) {
	result, err := v.ReadView.ProbeHTTP(ref, request)
	v.check(err)
	return result, err
}
func (v *checkedReadView) DiffAgainstSource(ref sdk.ObjectRef) (Diff, error) {
	result, err := v.ReadView.DiffAgainstSource(ref)
	v.check(err)
	return result, err
}
func (v *checkedReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error) {
	result, err := v.ReadView.DiffAgainstBaseline(ref)
	v.check(err)
	return result, err
}
