package program

import (
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"

	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

type Kind string

type ProgramSpec struct {
	ID     string
	Params []ParamSpec
}

// ParamSpec declares roles bound by triggering detector findings; Diagnose reads
// additional objects through ReadView by name or relation, never role enumeration.
type ParamSpec struct {
	Name     string
	Kind     Kind
	Required bool
	// ValueKind selects a scalar parameter instead of an object Kind.
	ValueKind ValueKind
	// Default is optional; nil means no default.
	Default any
}

// ObjectBindings binds declared object parameters to named Kubernetes resources.
type ObjectBindings map[string]sdk.ObjectRef

// Bindings contains object identities and separately typed scalar parameters.
type Bindings struct {
	Objects ObjectBindings `json:"objects,omitempty"`
	Values  map[string]any `json:"values,omitempty"`
}

func ValidateBindings(spec ProgramSpec, b Bindings) error {
	params := make(map[string]ParamSpec, len(spec.Params))
	for _, p := range spec.Params {
		if p.Name == "" {
			return fmt.Errorf("parameter name is required")
		}
		if _, ok := params[p.Name]; ok {
			return fmt.Errorf("duplicate parameter %q", p.Name)
		}
		if (p.Kind == "") == (p.ValueKind == "") {
			return fmt.Errorf("parameter %q requires exactly one object or value kind", p.Name)
		}
		if p.ValueKind != "" {
			switch p.ValueKind {
			case StringValue, IntValue, QuantityValue, DurationValue, BoolValue:
			default:
				return fmt.Errorf("parameter %q has an unknown value kind", p.Name)
			}
		}
		params[p.Name] = p
		if p.Default != nil {
			if p.ValueKind == "" {
				return fmt.Errorf("object parameter %q cannot have a value default", p.Name)
			}
			if err := ValidateValueBinding(p, p.Default); err != nil {
				return err
			}
		}
		_, objectBound := b.Objects[p.Name]
		_, valueBound := b.Values[p.Name]
		if p.Required && !objectBound && !valueBound && p.Default == nil {
			return fmt.Errorf("required parameter %q is unbound", p.Name)
		}
	}
	for name, ref := range b.Objects {
		p, ok := params[name]
		if !ok {
			return fmt.Errorf("unknown parameter %q", name)
		}
		if p.ValueKind != "" || ref.Kind != string(p.Kind) {
			return fmt.Errorf("parameter %q: kind %q, want %q", name, ref.Kind, p.Kind)
		}
	}
	for name, value := range b.Values {
		p, ok := params[name]
		if !ok {
			return fmt.Errorf("unknown parameter %q", name)
		}
		if err := ValidateValueBinding(p, value); err != nil {
			return err
		}
	}
	return nil
}

// Parameter resolves one declared value, including a validated default.
func (b Bindings) Parameter(spec ProgramSpec, name string) (any, error) {
	if err := ValidateBindings(spec, b); err != nil {
		return nil, err
	}
	for _, p := range spec.Params {
		if p.Name != name || p.ValueKind == "" {
			continue
		}
		v, ok := b.Values[name]
		if !ok {
			v = p.Default
		}
		if v == nil {
			return nil, fmt.Errorf("value parameter %q is unbound", name)
		}
		return v, nil
	}
	return nil, fmt.Errorf("undeclared value parameter %q", name)
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
	WebhookCAMatchesSecret(WebhookCA) (bool, error)
	sdk.ObjectReader
	sdk.DetectionContext
	sdk.ScopedObservationSource
	sdk.ObservationClock
	StatefulSets() []appsv1.StatefulSet
	HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	metrics.Source
	metrics.AlertSource
	metrics.QuerySource
	traffic.Source
	servicehealth.ReachabilitySource
	Logs(pod sdk.ObjectRef, container string, since time.Duration) (string, error)
	LogsWithOptions(pod sdk.ObjectRef, options corev1.PodLogOptions) (string, error)
	metrics.UsageSource
	ProbeTCP(to sdk.ObjectRef, port int32) (ProbeResult, error)
	ProbeHTTP(to sdk.ObjectRef, req HTTPProbe) (ProbeResult, error)
	// DiffAgainstSource compares quantities by value and excludes controller bookkeeping (revision, last-applied and restart annotations, status and managedFields), accepted server defaults, and replicas targeted by an HPA.
	DiffAgainstSource(ref sdk.ObjectRef) (Diff, error)
	DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error)
	DatabaseUsers(DatabaseTarget) ([]DatabaseUser, error)
	Observe(ExternalOperation) (int32, error)
	KubernetesRead(KubernetesRequest) (json.RawMessage, error)
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

// Diff is the shared accepted-source or baseline comparison result.
type Diff = sdk.SourceDiff

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

// CreateFromSource restores a missing named object from accepted source.
type CreateFromSource struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (CreateFromSource) action() {}

// DeleteObject irreversibly deletes a named object in the incident namespace.
// Its receipt retains a nonsecret preimage; compensation never recreates it.
type DeleteObject struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (DeleteObject) action() {}

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
	Value  Value
	Reason string
	// DocumentFormat and DocumentPath select a JSON/YAML entry in one ConfigMap key.
	// JSON edits preserve untouched number tokens; serialization may normalize formatting.
	DocumentFormat string
	DocumentPath   []string
}

func (SetField) action() {}

// DeletePod removes one named pod with UID/version preconditions. A graceful
// delete leaves the Pod terminating: that counts as deleted but the Pod may
// still run until it stops.
type DeletePod struct{ Ref sdk.ObjectRef }

func (DeletePod) action() {}

// ScaleWorkload restores replicas from accepted source, observations or a parameter.
// A ReplicaSet target has kubectl-scale parity, e.g. retiring an old-template
// ReplicaSet whose pods block a Deployment rollout.
type ScaleWorkload struct {
	Ref      sdk.ObjectRef
	Replicas Value
}

func (ScaleWorkload) action() {}

// Exec executes a provenance-derived command. Only its exit code is reported.
// The install grants the controller pods/exec (get, create) in the application
// namespace; the Kubernetes API enforces it.
type Exec struct {
	Pod       sdk.ObjectRef
	Container string
	Command   []Value
}

func (Exec) action() {}

type RestartRollout struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (RestartRollout) action() {}

type Plan struct {
	Actions   []Action
	Rationale string
}

func (p Plan) Validate() error {
	if err := validateAtomicWrites(p); err != nil {
		return err
	}
	for _, action := range p.Actions {
		if err := validateScriptAction(action); err != nil {
			return err
		}
		if err := validateGeneralAction(action); err != nil {
			return err
		}
		if err := validateDatabaseAction(action); err != nil {
			return err
		}
		if err := validateServicePortAction(action); err != nil {
			return err
		}
		if err := validateRuntimeAction(action); err != nil {
			return err
		}
		if err := validateObjectAction(action); err != nil {
			return err
		}
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
		if err := ValidateSetField(field); err != nil {
			return err
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

func (v *checkedReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedDeployments(namespace)
	v.check(err)
	return result, err
}

func (v *checkedReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedServices(namespace)
	v.check(err)
	return result, err
}

func (v *checkedReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedEndpoints(namespace)
	v.check(err)
	return result, err
}

func (v *checkedReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfigurations()
	v.check(err)
	return result, err
}

func (v *checkedReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfigurations()
	v.check(err)
	return result, err
}

func (v *checkedReadView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.ReadView).ObservationStatuses()
}
func (v *checkedReadView) ObservationGeneration() string {
	return sdk.ScopedSource(v.ReadView).ObservationGeneration()
}

func (v *checkedReadView) ObservedAt() time.Time {
	if clock, ok := v.ReadView.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v *checkedReadView) TrafficWindow(name string) (traffic.Window, bool) {
	if source, ok := v.ReadView.(traffic.Source); ok {
		r, ok := source.TrafficWindow(name)
		if !ok {
			v.check(fmt.Errorf("traffic %q unavailable", name))
		} else if r.LoadError != "" {
			v.check(fmt.Errorf("traffic %q: %s", name, r.LoadError))
		}
		return r, ok
	}
	return traffic.Window{}, false
}

func (v *checkedReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if source, ok := v.ReadView.(servicehealth.ReachabilitySource); ok {
		return source.ServiceReachability()
	}
	return servicehealth.ReachabilityWindow{}, false
}

func (v *checkedReadView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfiguration(name)
	v.check(err)
	return result, err
}

func (v *checkedReadView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfiguration(name)
	v.check(err)
	return result, err
}
