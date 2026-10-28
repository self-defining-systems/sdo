package sdktest

import (
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk/servicehealth"

	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

// PrimitiveCall stores one live ReadView call for deterministic replay.
type PrimitiveCall struct {
	// Phase separates observations made before and after repair. Empty means open.
	Phase     string          `json:"phase,omitempty"`
	Method    string          `json:"method"`
	Arguments json.RawMessage `json:"arguments"`
	Result    json.RawMessage `json:"result"`
	Error     string          `json:"error,omitempty"`
}

// RecordingReadView serializes recorded calls and OnRecord callbacks. ReadView
// must support concurrent reads; inspect Calls only after all readers finish.
type RecordingReadView struct {
	program.ReadView
	Calls    []PrimitiveCall
	OnRecord func(PrimitiveCall)
	mu       sync.Mutex
}

func (v *RecordingReadView) record(method string, arguments any, result any, err error) {
	args, _ := json.Marshal(arguments)
	value, _ := json.Marshal(result)
	call := PrimitiveCall{Method: method, Arguments: args, Result: value}
	if err != nil {
		call.Error = err.Error()
	}
	v.mu.Lock()
	v.Calls = append(v.Calls, call)
	if v.OnRecord != nil {
		v.OnRecord(call)
	}
	v.mu.Unlock()
}

func (v *RecordingReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	result, err := v.ReadView.Logs(ref, container, since)
	v.record("Logs", []any{ref, container, since}, result, err)
	return result, err
}
func (v *RecordingReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	result, err := v.ReadView.ProbeTCP(ref, port)
	v.record("ProbeTCP", []any{ref, port}, result, err)
	return result, err
}
func (v *RecordingReadView) ProbeHTTP(ref sdk.ObjectRef, req program.HTTPProbe) (program.ProbeResult, error) {
	result, err := v.ReadView.ProbeHTTP(ref, req)
	v.record("ProbeHTTP", []any{ref, req}, result, err)
	return result, err
}
func (v *RecordingReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	result, err := v.ReadView.DiffAgainstSource(ref)
	v.record("DiffAgainstSource", []any{ref}, result, err)
	return result, err
}
func (v *RecordingReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	result, err := v.ReadView.DiffAgainstBaseline(ref)
	v.record("DiffAgainstBaseline", []any{ref}, result, err)
	return result, err
}

// ReplayReadView consumes each recorded call at most once, including when
// diagnoses share the view. Snapshot and Calls must remain immutable during use.
type ReplayReadView struct {
	Snapshot
	Calls []PrimitiveCall
	used  map[int]bool
	mu    sync.Mutex
}

func (v *ReplayReadView) replay(method string, arguments any, result any) error {
	args, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for index, call := range v.Calls {
		if v.used[index] || call.Method != method || !sameJSON(call.Arguments, args) {
			continue
		}
		if v.used == nil {
			v.used = make(map[int]bool)
		}
		v.used[index] = true
		if call.Error != "" {
			return errors.New(call.Error)
		}
		return json.Unmarshal(call.Result, result)
	}
	return fmt.Errorf("unrecorded %s call with arguments %s", method, args)
}

func sameJSON(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func (v *ReplayReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	var result string
	err := v.replay("Logs", []any{ref, container, since}, &result)
	return result, err
}
func (v *ReplayReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	var result program.ProbeResult
	err := v.replay("ProbeTCP", []any{ref, port}, &result)
	return result, err
}
func (v *ReplayReadView) ProbeHTTP(ref sdk.ObjectRef, req program.HTTPProbe) (program.ProbeResult, error) {
	var result program.ProbeResult
	err := v.replay("ProbeHTTP", []any{ref, req}, &result)
	return result, err
}
func (v *ReplayReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	var result program.Diff
	err := v.replay("DiffAgainstSource", []any{ref}, &result)
	return result, err
}
func (v *ReplayReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	var result program.Diff
	err := v.replay("DiffAgainstBaseline", []any{ref}, &result)
	return result, err
}

type CorpusCase struct {
	ScriptOrigins          map[string]string `json:"script_origins,omitempty"`
	Open                   Snapshot
	Fixed                  Snapshot
	Healthy                Snapshot
	Recordings             []PrimitiveCall
	RootCauseProgramIDs    []string
	NoMatchProgramIDs      []string
	Bindings               program.Bindings
	PeerBindings           program.Bindings
	Provenance             CorpusProvenance
	SyntheticNoMatchReason string
	// HealthImpact marks visible impact in Open. Nil defaults to true for
	// existing fault fixtures; false permits silent health detectors.
	HealthImpact *bool
}

// CapturedFixedNoMatchPhase identifies observed negative evidence only.
const CapturedFixedNoMatchPhase = "fixed"

type CorpusProvenance struct {
	IncidentID  string `json:"incident_id"`
	OpenAt      string `json:"open_at,omitempty"`
	OpenSHA256  string `json:"open_sha256"`
	FixedSHA256 string `json:"fixed_sha256"`
}

// LoadCorpusCase reads open.json, fixed.json, healthy.json, recordings.json,
// and labels.json from one .sdo/corpus/<case> directory.
func LoadCorpusCase(dir string) (CorpusCase, error) { return loadCorpusCase(dir, true) }

// LoadCorpusMetadata reads labels and available receipt recordings without
// requiring replay snapshots. Static provenance never depends on replay inputs.
func LoadCorpusMetadata(dir string) (CorpusCase, error) { return loadCorpusCase(dir, false) }

func loadCorpusCase(dir string, snapshots bool) (CorpusCase, error) {
	var c CorpusCase
	for _, item := range []struct {
		name   string
		target any
	}{
		{"open.json", &c.Open}, {"fixed.json", &c.Fixed}, {"healthy.json", &c.Healthy}, {"recordings.json", &c.Recordings},
		{"labels.json", &struct {
			RootCauseProgramIDs    *[]string          `json:"root_cause_program_ids"`
			NoMatchProgramIDs      *[]string          `json:"no_match_program_ids"`
			Bindings               *program.Bindings  `json:"bindings"`
			PeerBindings           *program.Bindings  `json:"peer_bindings"`
			Provenance             *CorpusProvenance  `json:"provenance"`
			SyntheticNoMatchReason *string            `json:"synthetic_no_match_reason"`
			HealthImpact           **bool             `json:"health_impact"`
			ScriptOrigins          *map[string]string `json:"script_origins,omitempty"`
		}{&c.RootCauseProgramIDs, &c.NoMatchProgramIDs, &c.Bindings, &c.PeerBindings, &c.Provenance, &c.SyntheticNoMatchReason, &c.HealthImpact, &c.ScriptOrigins}},
	} {
		if !snapshots && (item.name == "open.json" || item.name == "fixed.json" || item.name == "healthy.json") {
			continue
		}
		path := filepath.Join(dir, item.name)
		raw, err := os.ReadFile(path)
		if !snapshots && item.name == "recordings.json" && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return c, fmt.Errorf("corpus %s: %w", path, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(item.target); err != nil {
			return c, fmt.Errorf("corpus %s: %w", path, err)
		}
	}
	for _, snapshot := range []Snapshot{c.Open, c.Fixed, c.Healthy} {
		if err := snapshot.ValidateObservation(); err != nil {
			return c, err
		}
	}
	c.Open.SourceName, c.Fixed.SourceName, c.Healthy.SourceName = "open.json", "fixed.json", "healthy.json"
	return c, nil
}

// ValidateCorpusProvenance checks that persisted case snapshots still match
// their recorded controller capture, or an explicitly labeled NoMatch case.
func ValidateCorpusProvenance(dir string) error {
	c, err := LoadCorpusCase(dir)
	if err != nil {
		return err
	}
	if c.Provenance.IncidentID == "" {
		if c.SyntheticNoMatchReason != "" && len(c.RootCauseProgramIDs) == 0 && len(c.NoMatchProgramIDs) > 0 {
			return nil
		}
		return errors.New("corpus case lacks captured incident provenance or synthetic NoMatch reason")
	}
	if c.Provenance.OpenAt == CapturedFixedNoMatchPhase && (len(c.RootCauseProgramIDs) > 0 || len(c.NoMatchProgramIDs) == 0) {
		return errors.New("fixed capture is negative evidence only")
	}
	if c.SyntheticNoMatchReason != "" {
		return errors.New("captured corpus cannot also be synthetic")
	}
	for name, digest := range map[string]string{"open.json": c.Provenance.OpenSHA256, "fixed.json": c.Provenance.FixedSHA256, "healthy.json": c.Provenance.FixedSHA256} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if digest == "" || fmt.Sprintf("%x", sha256.Sum256(data)) != digest {
			return fmt.Errorf("corpus %s does not match captured snapshot digest", name)
		}
	}
	if c.Open.NamespaceName == "" || c.Open.NamespaceName != c.Fixed.NamespaceName || c.Fixed.NamespaceName != c.Healthy.NamespaceName {
		return errors.New("captured corpus namespace mismatch")
	}
	return nil
}

// PhaseRecordings selects actual observations for one corpus phase. Healthy
// snapshots are the verified fixed snapshot, so they share its observations.
func (c CorpusCase) PhaseRecordings(phase string) []PrimitiveCall {
	if c.Provenance.OpenAt == CapturedFixedNoMatchPhase && phase == "open" {
		phase = "fixed"
	}
	var result []PrimitiveCall
	explicitHealthy := false
	for _, call := range c.Recordings {
		if call.Phase == "healthy" {
			explicitHealthy = true
		}
	}
	for _, call := range c.Recordings {
		recorded := call.Phase
		if recorded == "" {
			recorded = "open"
		}
		if recorded == phase || phase == "healthy" && !explicitHealthy && recorded == "fixed" {
			result = append(result, call)
		}
	}
	return result
}

// RecordPrimitiveCalls repeats previously observed reads on a fresh view. It
// records errors as observations; malformed templates fail explicitly.
func RecordPrimitiveCalls(view program.ReadView, calls []PrimitiveCall) ([]PrimitiveCall, error) {
	recorder := &RecordingReadView{ReadView: view}
	for _, call := range calls {
		var args []json.RawMessage
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("invalid %s recording arguments", call.Method)
		}

		switch call.Method {
		case "AdmissionWebhooksFor":
			var subject sdk.ObjectRef
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid admission webhook arguments")
			}
			if err := json.Unmarshal(args[0], &subject); err != nil {
				return nil, err
			}
			_, _ = recorder.AdmissionWebhooksFor(subject)
			continue

		case "WebhookCAMatchesSecret":
			var target program.WebhookCA
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid webhook CA arguments")
			}
			if err := json.Unmarshal(args[0], &target); err != nil {
				return nil, err
			}
			_, _ = recorder.WebhookCAMatchesSecret(target)
			continue

		case "ExternalEffect", "JobCompletion", "KubernetesEffect", "RunScriptReceipt", "RepairReceipt":
			// Effects are historical write evidence, never repeat mutations.
			continue
		case "Observe":
			if len(args) != 1 {
				return nil, errors.New("invalid Observe arguments")
			}
			var o program.ExternalOperation
			if err := json.Unmarshal(args[0], &o); err != nil {
				return nil, err
			}
			_, _ = recorder.Observe(o)
			continue
		case "KubernetesRead":
			if len(args) != 1 {
				return nil, errors.New("invalid KubernetesRead arguments")
			}
			var q program.KubernetesRequest
			if err := json.Unmarshal(args[0], &q); err != nil {
				return nil, err
			}
			_, _ = recorder.KubernetesRead(q)
			continue
		case "Object":
			if len(args) != 1 {
				return nil, errors.New("invalid Object arguments")
			}
			var ref sdk.ObjectRef
			if err := json.Unmarshal(args[0], &ref); err != nil {
				return nil, err
			}
			_, _ = recorder.Object(ref)
			continue
		case "QueryMetrics":
			if len(args) != 1 {
				return nil, errors.New("invalid QueryMetrics arguments")
			}
			var q metrics.Query
			if err := json.Unmarshal(args[0], &q); err != nil {
				return nil, err
			}
			_ = recorder.QueryMetrics(q)
			continue
		case "MetricResult", "TrafficWindow":
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid %s arguments", call.Method)
			}
			var name string
			if err := json.Unmarshal(args[0], &name); err != nil {
				return nil, err
			}
			if call.Method == "MetricResult" {
				_, _ = recorder.MetricResult(name)
			} else {
				_, _ = recorder.TrafficWindow(name)
			}
			continue
		case "ServiceReachability":
			if len(args) != 0 {
				return nil, errors.New("invalid ServiceReachability arguments")
			}
			_, _ = recorder.ServiceReachability()
			continue
		case "AlertsResult":
			if len(args) != 0 {
				return nil, errors.New("invalid AlertsResult arguments")
			}
			_, _ = recorder.AlertsResult()
			continue
		}
		switch call.Method {
		case "ScopedDeployments", "ScopedServices", "ScopedEndpoints", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration":
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid scoped recording arguments")
			}
			var namespace string
			if err := json.Unmarshal(args[0], &namespace); err != nil {
				return nil, err
			}
			switch call.Method {
			case "ScopedDeployments":
				_, _ = recorder.ScopedDeployments(namespace)
			case "ScopedServices":
				_, _ = recorder.ScopedServices(namespace)
			case "ScopedEndpoints":
				_, _ = recorder.ScopedEndpoints(namespace)
			case "MutatingWebhookConfiguration":
				_, _ = recorder.MutatingWebhookConfiguration(namespace)
			case "ValidatingWebhookConfiguration":
				_, _ = recorder.ValidatingWebhookConfiguration(namespace)
			}
			continue
		case "MutatingWebhookConfigurations", "ValidatingWebhookConfigurations":
			if len(args) != 0 {
				return nil, fmt.Errorf("invalid scoped webhook recording arguments")
			}
			if call.Method == "MutatingWebhookConfigurations" {
				_, _ = recorder.MutatingWebhookConfigurations()
			} else {
				_, _ = recorder.ValidatingWebhookConfigurations()
			}
			continue
		}
		if len(args) == 0 {
			return nil, fmt.Errorf("invalid %s recording arguments", call.Method)
		}
		var ref sdk.ObjectRef
		if err := json.Unmarshal(args[0], &ref); err != nil {
			return nil, err
		}
		switch call.Method {
		case "DiffAgainstSource":
			if len(args) != 1 {
				return nil, errors.New("invalid DiffAgainstSource arguments")
			}
			_, _ = recorder.DiffAgainstSource(ref)
		case "DiffAgainstBaseline":
			if len(args) != 1 {
				return nil, errors.New("invalid DiffAgainstBaseline arguments")
			}
			_, _ = recorder.DiffAgainstBaseline(ref)
		case "ProbeTCP":
			if len(args) != 2 {
				return nil, errors.New("invalid ProbeTCP arguments")
			}
			var port int32
			if err := json.Unmarshal(args[1], &port); err != nil {
				return nil, err
			}
			_, _ = recorder.ProbeTCP(ref, port)
		case "ProbeHTTP":
			if len(args) != 2 {
				return nil, errors.New("invalid ProbeHTTP arguments")
			}
			var request program.HTTPProbe
			if err := json.Unmarshal(args[1], &request); err != nil {
				return nil, err
			}
			_, _ = recorder.ProbeHTTP(ref, request)
		case "LogsWithOptions":
			if len(args) != 2 {
				return nil, errors.New("invalid LogsWithOptions arguments")
			}
			var options corev1.PodLogOptions
			if err := json.Unmarshal(args[1], &options); err != nil {
				return nil, err
			}
			_, _ = recorder.LogsWithOptions(ref, options)
		case "PodUsage", "NodeUsage":
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid %s arguments", call.Method)
			}
			if call.Method == "PodUsage" {
				_, _ = recorder.PodUsage(ref)
			} else {
				_, _ = recorder.NodeUsage(ref)
			}
		case "Logs":
			if len(args) != 3 {
				return nil, errors.New("invalid Logs arguments")
			}
			var container string
			var since time.Duration
			if err := json.Unmarshal(args[1], &container); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(args[2], &since); err != nil {
				return nil, err
			}
			_, _ = recorder.Logs(ref, container, since)
		default:
			return nil, fmt.Errorf("unknown recorded primitive %q", call.Method)
		}
	}
	return recorder.Calls, nil
}

func (v *RecordingReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedDeployments(namespace)
	v.record("ScopedDeployments", []any{namespace}, result, err)
	return result, err
}

func (v *RecordingReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedServices(namespace)
	v.record("ScopedServices", []any{namespace}, result, err)
	return result, err
}

func (v *RecordingReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	result, err := sdk.ScopedSource(v.ReadView).ScopedEndpoints(namespace)
	v.record("ScopedEndpoints", []any{namespace}, result, err)
	return result, err
}

func (v *RecordingReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfigurations()
	for i := range result {
		result[i] = sdk.ProjectMutatingWebhook(result[i])
	}
	v.record("MutatingWebhookConfigurations", []any{}, result, err)
	return result, err
}

func (v *RecordingReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfigurations()
	for i := range result {
		result[i] = sdk.ProjectValidatingWebhook(result[i])
	}
	v.record("ValidatingWebhookConfigurations", []any{}, result, err)
	return result, err
}

func (v *RecordingReadView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.ReadView).ObservationStatuses()
}
func (v *RecordingReadView) ObservationGeneration() string {
	return sdk.ScopedSource(v.ReadView).ObservationGeneration()
}

func (v *ReplayReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	return v.Snapshot.ScopedSnapshot.ScopedDeployments(namespace)
}

func (v *ReplayReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	return v.Snapshot.ScopedSnapshot.ScopedServices(namespace)
}

func (v *ReplayReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	return v.Snapshot.ScopedSnapshot.ScopedEndpoints(namespace)
}

func (v *ReplayReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	return v.Snapshot.ScopedSnapshot.MutatingWebhookConfigurations()
}

func (v *ReplayReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	return v.Snapshot.ScopedSnapshot.ValidatingWebhookConfigurations()
}

func (v *RecordingReadView) ObservedAt() time.Time {
	if clock, ok := v.ReadView.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v *RecordingReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	r, ok := v.ReadView.ServiceReachability()
	v.record("ServiceReachability", []any{}, reachabilityInput{r, ok}, nil)
	return r, ok
}

func (v *RecordingReadView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfiguration(name)
	if result != nil {
		projected := sdk.ProjectMutatingWebhook(*result)
		result = &projected
	}
	v.record("MutatingWebhookConfiguration", []any{name}, result, err)
	return result, err
}

func (v *RecordingReadView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfiguration(name)
	if result != nil {
		projected := sdk.ProjectValidatingWebhook(*result)
		result = &projected
	}
	v.record("ValidatingWebhookConfiguration", []any{name}, result, err)
	return result, err
}

func (v *RecordingReadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := v.ReadView.Object(ref)
	if err == nil {
		raw, marshalErr := json.Marshal(object)
		if marshalErr != nil {
			return nil, marshalErr
		}
		raw, marshalErr = ProjectCapturedObject(raw)
		if marshalErr != nil {
			return nil, marshalErr
		}
		projected := new(unstructured.Unstructured)
		if marshalErr = json.Unmarshal(raw, projected); marshalErr != nil {
			return nil, marshalErr
		}
		v.record("Object", []any{ref}, projected, nil)
	} else {
		v.record("Object", []any{ref}, nil, err)
	}
	return object, err
}

func (v *RecordingReadView) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if source, ok := v.ReadView.(sdk.ObjectCaptureSource); ok {
		return source.CaptureObjects(refs)
	}
	return sdk.TypedObjects(v.ReadView), nil
}
