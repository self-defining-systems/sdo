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
	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/metrics"
)

type Kind string

// ProgramSpec names a playbook program (its directory ID) and its roles.
// It declares the roles and values a finding or the responder binds; Diagnose
// and Mitigate must return identical specs.
type ProgramSpec struct {
	ID     string
	Params []ParamSpec
}

// ParamSpec declares one role or value a triggering finding binds.
// Diagnose reads additional objects through ReadView by name or relation,
// never role enumeration.
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

// ReadView is a program's frozen, recorded read access to the incident
// namespace; every read is replayed from the corpus at check time.
type ReadView interface {
	inputs.Sources
	// WebhookCAMatchesSecret reports whether one webhook's caBundle equals a Secret key without exposing either value.
	WebhookCAMatchesSecret(WebhookCA) (bool, error)
	// AdmissionWebhooksFor lists the webhooks admitting Pods for a Namespace or workload.
	// Cluster webhook names need no observation grant.
	AdmissionWebhooksFor(subject sdk.ObjectRef) ([]AdmissionWebhook, error)
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
	// AppMap is the accepted application map.
	AppMap() (AppMap, error)
	// DataChecks are the accepted data-check files by ID; datacheck.Verdicts
	// re-judges them over this view's frozen reads.
	datacheck.Accepted
	servicehealth.ReachabilitySource
	Logs(pod sdk.ObjectRef, container string, since time.Duration) (string, error)
	// LogsWithOptions reads bounded container logs; Previous selects the last terminated instance.
	LogsWithOptions(pod sdk.ObjectRef, options corev1.PodLogOptions) (string, error)
	// LogMatches is `kubectl logs --since=<window> | grep <pattern>`: it scans the whole window (up to
	// MaxLogScanLines newest lines) and returns the newest max matching lines (at most MaxLogMatchLines), oldest
	// first. pattern is an RE2 regular expression. ScanTruncated says the scan did not cover the window, so an empty
	// answer then means unknown, not absent. Prefer it to Logs when a Diagnose looks for a particular line.
	LogMatches(pod sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (LogMatchResult, error)
	metrics.UsageSource
	// ProbeTCP connects to one port of a Pod or Service and records whether it accepted.
	ProbeTCP(to sdk.ObjectRef, port int32) (ProbeResult, error)
	// ProbeHTTP sends one recorded HTTP request to a Service, on its first TCP port (HTTPProbePort).
	ProbeHTTP(to sdk.ObjectRef, req HTTPProbe) (ProbeResult, error)
	// DiffAgainstSource compares quantities by value and excludes controller bookkeeping (revision, last-applied and restart annotations, status and managedFields), accepted server defaults, and replicas targeted by an HPA. Changed is whether replacing the object by accepted source (a server dry-run update, the object ReconcileFromSource writes) would change it, so a swapped probe handler, a foreign list entry and a removed declared field are drift.
	DiffAgainstSource(ref sdk.ObjectRef) (Diff, error)
	DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error)
	// DatabaseUsers lists nonsecret user names and direct roles of one database catalog.
	DatabaseUsers(DatabaseTarget) ([]DatabaseUser, error)
	Observe(ExternalOperation) (int32, error)
	KubernetesRead(KubernetesRequest) (json.RawMessage, error)
	// Read returns bounded typed rows: store records over a RepairData scope, or
	// named fields of an unauthenticated HTTP status response. See Query.
	Read(Query) (Observation, error)
}

type ProbeResult struct {
	Success bool   `json:"success"`
	Detail  string `json:"detail,omitempty"`
	// Status is the HTTP status, set when the request got a response.
	Status int `json:"status,omitempty"`
	// Fields holds the scalars an HTTPProbe asked for by JSON pointer.
	Fields Row `json:"fields,omitempty"`
}

// HTTPProbe is the request ProbeHTTP sends.
type HTTPProbe struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	// Fields maps a name to an RFC 6901 pointer to a scalar in the JSON
	// response body; the answer carries those scalars and drops the body.
	Fields map[string]string `json:"fields,omitempty"`
}

// Diff is the shared accepted-source or baseline comparison result.
// A delete-style Diagnose matches on diff.PresentButNotInSource(), not SourceAbsent, which stays true after the delete.
// A recreate-style Diagnose matches on diff.MissingButInSource(): an object accepted source declares and that is
// absent live is a diff (LiveAbsent), not an error, and it turns false once CreateFromSource has recreated it.
type Diff = sdk.SourceDiff

// Result is a diagnosis verdict: Match means the fault is present, NoMatch
// absent, Unknown that evidence is unavailable.
type Result string

const (
	Match          Result = "Match"
	NoMatch        Result = "NoMatch"
	Unknown        Result = "Unknown"
	NeedsJudgement Result = "NeedsJudgement"
)

// Evidence is one observed fact supporting a verdict.
type Evidence struct {
	Resource sdk.ObjectRef `json:"resource"`
	Fact     string        `json:"fact"`
	Observed string        `json:"observed"`
	Expected string        `json:"expected"`
}

// Verdict is what Diagnose returns; NeedsJudgement requires a Question.
// CauseClass is a cause label. Unrecognized labels count as other. A Match whose
// Reach is out-of-reach lets the automatic stage escalate the incident instead
// of repairing it (see Escalate).
type Verdict struct {
	Result     Result     `json:"result"`
	Evidence   []Evidence `json:"evidence,omitempty"`
	Question   string     `json:"question,omitempty"`
	CauseClass string     `json:"cause_class,omitempty"`
	Reach      string     `json:"reach,omitempty"`
	// Reads is the trace of the external reads the Diagnose made, in order,
	// each with what it returned. RunDiagnose fills it; a program never sets
	// it. It is how a NoMatch is attributed to the condition that produced it:
	// a trace that ends at a log read means the log condition failed and the
	// command after it never ran.
	Reads []ReadStep `json:"reads,omitempty"`
}

func (v Verdict) Validate() error {
	if err := validateCause(v.CauseClass, v.Reach); err != nil {
		return err
	}
	if v.Reach != "" && v.Result != Match {
		return fmt.Errorf("reach states a matched cause; a %s verdict has none", v.Result)
	}
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

// ReconcileFromSource restores a whole source-owned object to accepted source,
// removing additions SourceDiff reports; prefer SetField for one field.
// An object accepted source declares that is absent is created from source,
// exactly as CreateFromSource (the receipt names the create).
// For a Secret the controller writes the literal data of the accepted render
// (the keys the source declares; other keys stay) and nothing records a value:
// the receipt names the keys. It refuses a Secret whose source has no literal
// data or whose render is not deterministic (a generated password); then
// Escalate. Restart the readers that started after the foreign change with
// RestartRollout later in the same plan, because a running Pod keeps the value
// it started with. Identify readers from their pod template Secret references.
// After it restores a StatefulSet's pod template, each Pod still on another
// revision than status.updateRevision that is failing (ImagePullBackOff,
// ErrImagePull, CrashLoopBackOff) or not Ready past a short bound is deleted,
// one at a time in ordinal order, so the controller recreates it at the
// restored revision; the receipt names each deleted Pod and why.
type ReconcileFromSource struct{ Ref sdk.ObjectRef }

func (ReconcileFromSource) action() {}

// Rollback restores a workload's fields from an explicit earlier revision.
type Rollback struct {
	Ref        sdk.ObjectRef
	ToRevision string
}

func (Rollback) action() {}

// SetField writes one schema-checked field or ConfigMap JSON/YAML entry.
// The Value is accepted source, an observation or a declared parameter.
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

// Escalate closes the incident as escalated, without a repair.
// The program's Diagnose matched an out-of-reach cause (Verdict.Reach ReachOutOfReach) the
// application namespace cannot fix, such as a node, the network fabric, an
// external dependency or cluster policy. It writes nothing. It must be the only
// action of its plan. Resources name the out-of-reach objects the verdict's
// evidence observed; the controller records them as the confirmed cause.
type Escalate struct {
	Reason    string
	Resources []sdk.ObjectRef
}

func (Escalate) action() {}

// RestartRollout restarts a workload's Pods like kubectl rollout restart.
// It stamps the Pod template's kubectl.kubernetes.io/restartedAt annotation, and
// the workload's own update strategy replaces the Pods. Pods the controller
// will not replace (an OnDelete strategy, or an OrderedReady StatefulSet
// waiting on an unready Pod) are deleted so they restart too; the receipt names
// each deleted Pod and why. Ref must be one named apps/v1 Deployment,
// StatefulSet or DaemonSet.
type RestartRollout struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (RestartRollout) action() {}

// Plan is what Mitigate returns: actions applied in order as one batch.
// An empty plan means nothing to do.
type Plan struct {
	Actions   []Action
	Rationale string
}

func (p Plan) Validate() error {
	if err := validateAtomicWrites(p); err != nil {
		return err
	}
	if err := validateEscalation(p); err != nil {
		return err
	}
	if err := validateSourceChanges(p); err != nil {
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
		if err := validateDataAction(action); err != nil {
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
		if change, ok := SourceChangeAction(action); ok {
			if err := change.Validate(); err != nil {
				return err
			}
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

// RunDiagnose runs a Diagnose over v with a deadline and the read rules every
// diagnosis shares. Reads fail by class (sdk.ReadErrorClass): a refused or
// absent read is an observation the program may reason on and never changes the
// verdict; an unavailable read is "SDO could not observe" and an invalid read is a
// program bug. A failed read prevents clearance, not a positive finding:
//
//   - a latched invalid read makes the verdict Unknown;
//   - a latched unavailable read turns NoMatch into Unknown, because the program
//     cleared on missing evidence;
//   - a latched unavailable read leaves Match standing, and every failed read is
//     appended as evidence ("read unavailable") so the closing turn, the outcome
//     and the audit see what the program did not observe;
//   - Unknown and NeedsJudgement are unchanged.
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
		verdict  Verdict
		err      error
		failures []readFailure
	}
	result := make(chan outcome, 1)
	checked := &checkedReadView{ReadView: ReplayProgramView(v, v), readChecks: &readChecks{maxReads: MaxDiagnoseReads}}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == sdk.ErrReadReplyLimit {
					result <- outcome{err: sdk.ErrReadReplyLimit}
				} else {
					result <- outcome{err: fmt.Errorf("panic: %v", recovered)}
				}
			}
		}()
		verdict, err := p.Diagnose(ctx, checked, b)
		if aborted := ReadAbortError(checked.ReadView); aborted != nil {
			err = aborted
		}
		result <- outcome{verdict, err, checked.failures()}
	}()
	traced := func(v Verdict) Verdict {
		v.Reads = checked.trace()
		return v
	}
	select {
	case <-ctx.Done():
		return traced(unknown(ctx.Err()))
	case got := <-result:
		if got.err != nil {
			return traced(unknown(got.err))
		}
		if err := got.verdict.Validate(); err != nil {
			return traced(unknown(err))
		}
		return traced(withReadFailures(normalizeCause(got.verdict), got.failures))
	}
}

func unknown(err error) Verdict {
	return Verdict{Result: Unknown, Evidence: []Evidence{{Fact: "diagnosis", Observed: err.Error(), Expected: "successful diagnosis"}}}
}

// checkedReadView prevents an unavailable observation from being treated as clearance.
type checkedReadView struct {
	ReadView
	*readChecks
}

// TransformReadSource preserves this invocation's checks while replacing the read source.
func (v *checkedReadView) TransformReadSource(transform func(ReadView) ReadView) ReadView {
	return &checkedReadView{ReadView: transform(v.ReadView), readChecks: v.readChecks}
}

type readChecks struct {
	mu     sync.Mutex
	failed []readFailure
	// maxReads bounds Read calls when positive (a Diagnose); reads counts them.
	maxReads, reads int
	steps           []ReadStep
}

// maxTraceSteps bounds the trace one Diagnose keeps.
const maxTraceSteps = 32

// ReadStep is one external read a Diagnose made and what it returned.
type ReadStep struct {
	// Op is the read kind: logs, log_matches, observe, read, database_users or kubernetes_read.
	Op string `json:"op"`
	// Target names what was read: namespace/pod/container, a store collection or an API path.
	Target string `json:"target"`
	// Outcome is the result: a count, an exit code or "error: ...".
	Outcome string `json:"outcome"`
}

func (v *checkedReadView) record(op, target, outcome string, err error) {
	if err != nil {
		outcome = "error: " + err.Error()
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.steps) < maxTraceSteps {
		v.steps = append(v.steps, ReadStep{Op: op, Target: target, Outcome: outcome})
	}
}

func (v *checkedReadView) trace() []ReadStep {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]ReadStep(nil), v.steps...)
}

func podTarget(ref sdk.ObjectRef, container string) string {
	return ref.Namespace + "/" + ref.Name + "/" + container
}

// readFailure is one read the diagnosis could not trust: SDO could not observe it
// (unavailable) or the program was not allowed to make it (invalid).
type readFailure struct {
	method string
	err    error
	class  string
}

func (f readFailure) observed() string { return f.method + ": " + f.err.Error() }

// maxReadFailures bounds the failed reads one diagnosis keeps; later ones are
// dropped, so a program that loops on a dead source stays bounded.
const maxReadFailures = 32

// withReadFailures applies the verdict rule to the reads a diagnosis latched.
func withReadFailures(verdict Verdict, failures []readFailure) Verdict {
	if len(failures) == 0 {
		return verdict
	}
	var invalid []readFailure
	for _, f := range failures {
		if f.class == sdk.ReadClassInvalid {
			invalid = append(invalid, f)
		}
	}
	switch {
	case len(invalid) > 0:
		return unknownFromReads(invalid)
	case verdict.Result == NoMatch:
		return unknownFromReads(failures)
	case verdict.Result == Match:
		evidence := append([]Evidence(nil), verdict.Evidence...)
		for _, f := range failures {
			evidence = append(evidence, Evidence{Fact: "read unavailable", Observed: f.observed(), Expected: "observation"})
		}
		verdict.Evidence = evidence
	}
	return verdict
}

func unknownFromReads(failures []readFailure) Verdict {
	v := Verdict{Result: Unknown}
	for _, f := range failures {
		v.Evidence = append(v.Evidence, Evidence{Fact: "diagnosis", Observed: f.observed(), Expected: "successful diagnosis"})
	}
	return v
}

// check latches a failed read whose class is unavailable or invalid; refused and
// absent reads are observations and never latch.
func (v *checkedReadView) check(method string, err error) {
	if err == nil {
		return
	}
	class := sdk.ReadErrorClass(err)
	if class != sdk.ReadClassUnavailable && class != sdk.ReadClassInvalid {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.failed) >= maxReadFailures {
		return
	}
	v.failed = append(v.failed, readFailure{method: method, err: err, class: class})
}

// failure returns the first latched failed read's error, or nil. A detector's
// CheckReads uses it: whatever it could not observe prevents clearance.
func (v *checkedReadView) failure() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.failed) == 0 {
		return nil
	}
	return v.failed[0].err
}

// failures returns the latched failed reads, oldest first.
func (v *checkedReadView) failures() []readFailure {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]readFailure(nil), v.failed...)
}
func (v *checkedReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	result, err := v.ReadView.Logs(ref, container, since)
	v.check("Logs", err)
	v.record("logs", podTarget(ref, container), fmt.Sprintf("%d bytes", len(result)), err)
	return result, err
}
func (v *checkedReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (ProbeResult, error) {
	result, err := v.ReadView.ProbeTCP(ref, port)
	v.check("ProbeTCP", err)
	return result, err
}
func (v *checkedReadView) ProbeHTTP(ref sdk.ObjectRef, request HTTPProbe) (ProbeResult, error) {
	result, err := v.ReadView.ProbeHTTP(ref, request)
	v.check("ProbeHTTP", err)
	return result, err
}
func (v *checkedReadView) DiffAgainstSource(ref sdk.ObjectRef) (Diff, error) {
	result, err := v.ReadView.DiffAgainstSource(ref)
	v.check("DiffAgainstSource", err)
	return result, err
}
func (v *checkedReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error) {
	result, err := v.ReadView.DiffAgainstBaseline(ref)
	v.check("DiffAgainstBaseline", err)
	return result, err
}

// Unchecked returns the view beneath this fail-closed read tracking. An edge that
// reads on a program's behalf, such as the memory host's frozen diffs, uses it
// so a read the program never made cannot fail the program's evaluation.
func (v *checkedReadView) Unchecked() ReadView { return v.ReadView }

// UnderlyingReadView preserves evidence sinks across the fail-closed wrapper.
func (v *checkedReadView) UnderlyingReadView() ReadView { return v.ReadView }

func (v *checkedReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedDeployments(namespace)
	v.check("ScopedDeployments", err)
	return result, err
}

func (v *checkedReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedServices(namespace)
	v.check("ScopedServices", err)
	return result, err
}

func (v *checkedReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedEndpoints(namespace)
	v.check("ScopedEndpoints", err)
	return result, err
}

func (v *checkedReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfigurations()
	v.check("MutatingWebhookConfigurations", err)
	return result, err
}

func (v *checkedReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfigurations()
	v.check("ValidatingWebhookConfigurations", err)
	return result, err
}

func (v *checkedReadView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.ReadView).ObservationStatuses()
}
func (v *checkedReadView) ObservationGeneration() string {
	return sdk.ScopedSource(v.ReadView).ObservationGeneration()
}

func (v *checkedReadView) TrafficWindow(name string) (traffic.Window, bool) {
	if source, ok := v.ReadView.(traffic.Source); ok {
		r, ok := source.TrafficWindow(name)
		if !ok {
			v.check("TrafficWindow", fmt.Errorf("traffic %q unavailable", name))
		} else if r.LoadError != "" {
			v.check("TrafficWindow", fmt.Errorf("traffic %q: %s", name, r.LoadError))
		}
		return r, ok
	}
	return traffic.Window{}, false
}

func (v *checkedReadView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfiguration(name)
	v.check("MutatingWebhookConfiguration", err)
	return result, err
}

func (v *checkedReadView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfiguration(name)
	v.check("ValidatingWebhookConfiguration", err)
	return result, err
}
