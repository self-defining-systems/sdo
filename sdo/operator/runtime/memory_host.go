package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
	"sdo.dev/operator/sdk/vantage"
)

// MemoryHostRPC is implemented by the host manager, which leases the active
// child during each call. The controller only receives findings and plans.
type MemoryHostRPC interface {
	Evaluate(context.Context, *pb.EvaluateRequest) (*pb.EvaluateResponse, error)
	memoryhost.ProgramRPC
}

// MirroredMemoryRPC leases the accepted host, publishing a barrier for detector views.
type MirroredMemoryRPC interface {
	Capture(context.Context, sdk.DetectionContext) (memoryhost.Host, uint64, func(), error)
}
type RemoteMemory struct {
	Detectors []sdk.Detector
	Programs  program.ProgramRegistry
	// TelemetrySources is the accepted revision's telemetry sources file as
	// the host compiled it in; empty when the revision binds none.
	TelemetrySources []byte
	// Telemetry is the task-facing telemetry context for this revision, set by
	// the entry point that knows the environment; nil leaves the previous one.
	Telemetry     *pb.TelemetryContext
	rpc           MirroredMemoryRPC
	memoryCommit  string
	registrations []memoryhost.Registration
}

func NewRemoteMemory(ready *pb.ReadyResponse, rpc MirroredMemoryRPC) (*RemoteMemory, error) {
	if ready == nil || rpc == nil {
		return nil, errors.New("remote memory requires identity and mirrored transport")
	}
	r := &RemoteMemory{rpc: rpc, memoryCommit: ready.GetMemoryCommit(), TelemetrySources: ready.GetTelemetrySources()}
	seen := map[string]bool{}
	for _, wire := range ready.Detectors {
		spec, err := memoryhost.DecodeRegistration(wire)
		if err != nil {
			return nil, err
		}
		if seen[spec.ID] {
			return nil, fmt.Errorf("duplicate remote detector %q", spec.ID)
		}
		seen[spec.ID] = true
		r.registrations = append(r.registrations, spec)
		remote := &remoteDetector{Registration: spec, remote: r}
		if spec.Windowed {
			r.Detectors = append(r.Detectors, &windowedRemoteDetector{remoteDetector: remote})
		} else {
			r.Detectors = append(r.Detectors, remote)
		}
	}
	mitigators := map[string]bool{}
	for _, id := range ready.MitigatorIds {
		mitigators[id] = true
	}
	var ds []program.Diagnoser
	var ms []program.Mitigator
	for _, wire := range ready.Programs {
		if wire == nil {
			return nil, errors.New("nil remote program registration")
		}
		spec := program.ProgramSpec{ID: wire.Id}
		for _, p := range wire.Params {
			if p == nil {
				return nil, errors.New("nil program parameter")
			}
			param := program.ParamSpec{Name: p.Name, Kind: program.Kind(p.Kind), Required: p.Required, ValueKind: program.ValueKind(p.ValueKind)}
			if p.Default != nil {
				param.Default = p.Default.AsInterface()
			}
			spec.Params = append(spec.Params, param)
		}
		p := &remoteProgram{spec: spec, remote: r}
		ds = append(ds, p)
		if mitigators[spec.ID] {
			ms = append(ms, p)
			delete(mitigators, spec.ID)
		}
	}
	if len(mitigators) > 0 {
		return nil, errors.New("remote mitigator has no diagnoser")
	}
	var err error
	registry, err := program.NewRegistry(ds, ms)
	r.Programs = &remoteProgramRegistry{ProgramRegistry: registry}
	return r, err
}

// ReadState asks the host to run one app-state reader. The failure of the call
// or of the read is returned as an error: the observer records it as the
// reader's read error, never as state.
//
// view is the controller's current cluster snapshot. It is published to the
// host's mirror first (the step every detector evaluation takes), so a reader
// never runs against a mirror that no evaluation has refreshed, such as the
// one right after a memory revision's activation.
func (r *RemoteMemory) ReadState(ctx context.Context, view sdk.DetectionContext, id string, reads map[string]podread.Result) (carried.Values, error) {
	if view != nil {
		_, _, release, err := r.rpc.Capture(ctx, view)
		if err != nil {
			return carried.Values{}, fmt.Errorf("publish the cluster snapshot to the memory host: %w", err)
		}
		release()
	}
	rpc, ok := r.rpc.(interface {
		ReadState(context.Context, *pb.ReadStateRequest) (*pb.ReadStateResponse, error)
	})
	if !ok {
		return carried.Values{}, errors.New("memory host state reads unavailable")
	}
	request := &pb.ReadStateRequest{ReaderId: id, ObservedAt: timestamppb.New(lifecycleio.Now()), PodReads: map[string]*structpb.Struct{}}
	for queryID, result := range reads {
		wire, err := wireStruct(result)
		if err != nil {
			return carried.Values{}, fmt.Errorf("encode pod read %q: %w", queryID, err)
		}
		request.PodReads[queryID] = wire
	}
	response, err := rpc.ReadState(ctx, request)
	if err != nil {
		return carried.Values{}, fmt.Errorf("memory host read state: %w", err)
	}
	if response.GetError() != "" {
		return carried.Values{}, errors.New(response.GetError())
	}
	var values carried.Values
	for key, value := range response.GetValues() {
		values.Set(key, value)
	}
	if err := values.Err(); err != nil {
		return carried.Values{}, err
	}
	return values, nil
}

// appStateObservationBudget bounds one reader's result on the wire: 16 readers
// at the worst-case key and value sizes would exceed the host's 16 MiB message
// limit and fail every evaluation.
const appStateObservationBudget = 512 << 10

// boundAppState drops the oldest healthy history, then the oldest ring
// snapshots (keeping the newest), until the result fits budget bytes.
func boundAppState(r carried.Result, budget int) carried.Result {
	size := func() int { raw, _ := json.Marshal(r); return len(raw) }
	r.HealthyHistory = slices.Clone(r.HealthyHistory)
	r.Recent = slices.Clone(r.Recent)
	for size() > budget && len(r.HealthyHistory) > 0 {
		r.HealthyHistory = r.HealthyHistory[1:]
	}
	for size() > budget && len(r.Recent) > 1 {
		r.Recent = r.Recent[1:]
	}
	return r
}

// appStateReaderClue is the reader detector's whole output: an info finding
// while the newest read failed.
func appStateReaderClue(spec sdk.DetectorSpec, view sdk.DetectionContext) []sdk.Finding {
	result, ok := carried.ResultFrom(view, spec.ID)
	if !ok {
		return nil
	}
	latest, ok := result.Latest()
	if !ok || latest.Error == "" {
		return nil
	}
	return []sdk.Finding{{
		DetectorID: spec.ID, RuleID: "app-state.read-failed", Status: sdk.FindingActive, Severity: sdk.SeverityInfo,
		Summary:         "app-state reader " + spec.ID + " could not read the application's state",
		Evidence:        latest.Error,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: view.Namespace()},
		Fingerprint:     "app-state-read-failed/" + spec.ID,
	}}
}

type remoteProgramRegistry struct{ program.ProgramRegistry }

type remoteDetector struct {
	memoryhost.Registration
	remote *RemoteMemory
}

func (d *remoteDetector) Spec() sdk.DetectorSpec    { return d.DetectorSpec }
func (d *remoteDetector) AppStateReader() bool      { return d.Registration.AppStateReader }
func (d *remoteDetector) AppStateNoisy() []string   { return d.Registration.AppStateNoisy }
func (d *remoteDetector) AppStateReaders() []string { return d.Registration.AppStateReaders }
func (d *remoteDetector) AppStateRead(ctx context.Context, view sdk.DetectionContext, reads map[string]podread.Result) (carried.Values, error) {
	return d.remote.ReadState(ctx, view, d.ID, reads)
}
func (d *remoteDetector) MetricQueries() []metrics.Query        { return d.Metrics }
func (d *remoteDetector) LogQueries() []logs.Query              { return d.Logs }
func (d *remoteDetector) PodReads() []podread.Query             { return d.Registration.PodReads }
func (d *remoteDetector) VantageProbes() []vantage.Probe        { return d.Registration.Vantage }
func (d *remoteDetector) ConsumesUsage() bool                   { return d.UsesUsage }
func (d *remoteDetector) UsageRequirements() usage.Requirements { return d.Usage }
func (d *remoteDetector) ConsumesServiceReachability() bool     { return d.Reachability }
func (d *remoteDetector) ConsumesAlerts() bool                  { return d.Alerts }
func (d *remoteDetector) ConsumesPodAdmission() bool            { return d.PodAdmission }
func (d *remoteDetector) ScenarioSLOWorkload() string           { return d.ScenarioSLO }
func (d *remoteDetector) TrafficWorkloads() []string            { return d.Traffic }
func (d *remoteDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.Registration.AppStateReader {
		// A reader runs in the background on its own interval; its detector
		// only reports a failed read, as an info clue. It never errors, so a
		// broken reader cannot block health or baseline promotion.
		return appStateReaderClue(d.DetectorSpec, view), nil
	}
	prepared, release, err := d.prepareDetection(ctx, view)
	if err != nil {
		return nil, err
	}
	defer release()
	return prepared.Detect(ctx, view)
}

// prepareDetection freezes controller-owned inputs before the authored
// detector's execution budget begins. Source normalization may perform many
// API calls; it is not execution of the detector in the memory host.
func (d *remoteDetector) prepareDetection(ctx context.Context, view sdk.DetectionContext) (sdk.Detector, func(), error) {
	// Diffs may call the API server, so they are frozen before the host lease and mirror barrier are taken.
	reads, err := watchedDiffReads(view, d.DetectorSpec.Watches)
	if err != nil {
		return nil, nil, err
	}
	rpc, sequence, release, err := d.remote.scopedCapture(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	observation, err := d.remote.observation(view)
	if err != nil {
		release()
		return nil, nil, err
	}
	return &preparedRemoteDetector{remoteDetector: d, rpc: rpc, request: &pb.EvaluateRequest{DetectorIds: []string{d.ID}, MirrorSequence: sequence, Observation: observation, Reads: reads}}, release, nil
}

// windowedRemoteDetector keeps sdk.WindowedDetector across the host boundary:
// a windowed detector's repair is visible to it only after its window turns
// over, which the deterministic stage waits for. The window follows from the
// declared inputs, the same way the compiled detectors derive it.
type windowedRemoteDetector struct{ *remoteDetector }

func (d *windowedRemoteDetector) EvaluationWindow(view sdk.DetectionContext) time.Duration {
	var window time.Duration
	if d.Reachability {
		window = servicehealth.ReachabilityEvaluationWindow(view)
	}
	return max(window, d.MetricWindow, trafficConsumerWindow(d.Traffic, view))
}

type preparedRemoteDetector struct {
	*remoteDetector
	rpc     MemoryHostRPC
	request *pb.EvaluateRequest
}

func (d *preparedRemoteDetector) Detect(ctx context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	response, err := d.rpc.Evaluate(ctx, d.request)
	if err != nil {
		return nil, fmt.Errorf("memory host evaluate: %w", err)
	}
	if response == nil || len(response.Results) != 1 || response.Results[0].GetDetectorId() != d.ID {
		return nil, errors.New("invalid memory host detector response")
	}
	result := response.Results[0]
	if result.Error != "" {
		return nil, sdk.ErrorWithClass(result.Error, result.ErrorClass)
	}
	return findingsState(result.Findings), nil
}

// watchedDiffReads freezes the source and baseline diffs of the objects a detector watches, because the
// memory host answers DiffAgainstSource from controller-provided reads and never calls back.
func watchedDiffReads(view sdk.DetectionContext, watches []sdk.WatchKind) ([]*pb.MemoryReadReceipt, error) {
	if len(watches) == 0 {
		return nil, nil
	}
	refs, err := snapshotMemoryRefs(view)
	if err != nil {
		return nil, err
	}
	watchesRef := func(ref sdk.ObjectRef) bool {
		for _, watch := range watches {
			if ref.Kind == watch.Kind && (watch.APIVersion == "" || ref.APIVersion == watch.APIVersion) &&
				(watch.Namespace == "" || ref.Namespace == watch.Namespace) {
				return true
			}
		}
		return false
	}
	var watched []sdk.ObjectRef
	frozen := map[sdk.ObjectRef]bool{}
	for _, ref := range refs {
		if watchesRef(ref) {
			watched = append(watched, ref)
			frozen[ref] = true
		}
	}
	reads, err := frozenMemoryReads(view, watched)
	if err != nil {
		return nil, err
	}
	// A watched object that is declared but gone is in no snapshot list: freeze
	// its diff too, or the host answers it as unrecorded.
	live := map[sdk.ObjectRef]bool{}
	for _, ref := range refs {
		live[ref] = true
	}
	for ref := range live {
		frozen[ref] = true
	}
	absence, err := absenceInputs(view, frozen, watchesRef)
	if err != nil {
		return nil, err
	}
	return append(reads, absence...), nil
}

func wireStruct(value any) (*structpb.Struct, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, errors.New("memory observation requires a JSON object")
	}
	return structpb.NewStruct(fields)
}
func (r *RemoteMemory) observation(view sdk.DetectionContext) (*pb.MemoryObservation, error) {
	if cycle := memoryCycle(view); cycle != nil {
		p := &cycle.memory
		p.mu.Lock()
		if p.observations == nil {
			p.observations = map[*RemoteMemory]*preparedObservation{}
		}
		observation := p.observations[r]
		if observation == nil {
			observation = &preparedObservation{}
			p.observations[r] = observation
		}
		p.mu.Unlock()
		observation.once.Do(func() { observation.value, observation.err = r.buildObservation(view) })
		return observation.value, observation.err
	}
	return r.buildObservation(view)
}

// observationSource is the controller's own cycle inputs beneath any
// per-evaluation wrapper.
func observationSource(view sdk.DetectionContext) inputs.Carrier {
	sourceView := view
	for {
		wrapper, ok := sourceView.(interface{ UnderlyingReadView() program.ReadView })
		if !ok {
			break
		}
		sourceView = wrapper.UnderlyingReadView()
	}
	return inputs.New(sourceView)
}

func (r *RemoteMemory) buildObservation(view sdk.DetectionContext) (*pb.MemoryObservation, error) {
	source := observationSource(view)
	out := &pb.MemoryObservation{Traffic: map[string]*structpb.Struct{}, Metrics: map[string]*structpb.Struct{}, Logs: map[string]*structpb.Struct{}, PodReads: map[string]*structpb.Struct{}, Vantage: map[string]*structpb.Struct{}, AppState: map[string]*structpb.Struct{}}
	if at := source.ObservedAt(); !at.IsZero() {
		out.ObservedAt = timestamppb.New(at)
		if err := out.ObservedAt.CheckValid(); err != nil {
			return nil, err
		}
	}
	if hold := source.SelfHealHold(); hold > 0 {
		out.SelfHealHold = durationpb.New(hold)
	}
	if startups := source.ObservedStartups(); len(startups) > 0 {
		out.ObservedStartup = make(map[string]*durationpb.Duration, len(startups))
		for workload, allowance := range startups {
			out.ObservedStartup[workload] = durationpb.New(allowance)
		}
	}
	out.HeldKeys = source.HeldAlertKeys()
	for _, kind := range sdk.CompleteKinds(source) {
		out.CompleteKinds = append(out.CompleteKinds, kind.GroupVersion().String()+"/"+kind.Kind)
	}
	var err error
	if len(source.ObservationStatuses()) > 0 {
		scoped, _, e := sdktest.CaptureScoped(source)
		if e != nil {
			return nil, e
		}
		if out.Scoped, err = wireStruct(scoped); err != nil {
			return nil, err
		}
	}
	if v, ok := source.PodAdmission(); ok {
		if out.PodAdmission, err = wireStruct(v); err != nil {
			return nil, err
		}
	}
	if v, ok := source.ServiceReachability(); ok {
		out.Reachability, err = wireStruct(v)
		if err != nil {
			return nil, err
		}
	}
	if v, ok := source.UsageResult(); ok {
		out.Usage, err = wireStruct(v)
		if err != nil {
			return nil, err
		}
	}
	if v, ok := source.AlertsResult(); ok {
		out.Alerts, err = wireStruct(v)
		if err != nil {
			return nil, err
		}
	}
	if receipts, ok := source.TrafficWindows()[traffic.HTTPReceiptWindowID]; ok {
		if out.Traffic[traffic.HTTPReceiptWindowID], err = wireStruct(receipts); err != nil {
			return nil, err
		}
	}
	for _, spec := range r.registrations {
		for _, id := range spec.AppStateReaders {
			if v, ok := source.AppStateResult(id); ok {
				if out.AppState[id], err = wireStruct(boundAppState(v, appStateObservationBudget)); err != nil {
					return nil, err
				}
			}
		}
		for _, id := range spec.Traffic {
			if v, ok := source.TrafficWindow(id); ok {
				out.Traffic[id], err = wireStruct(v)
				if err != nil {
					return nil, err
				}
			}
		}
		for _, q := range spec.Metrics {
			if v, ok := source.MetricResult(q.ID); ok {
				out.Metrics[q.ID], err = wireStruct(v)
				if err != nil {
					return nil, err
				}
			}
		}
		for _, q := range spec.Logs {
			if v, ok := source.LogResult(q.ID); ok {
				out.Logs[q.ID], err = wireStruct(v)
				if err != nil {
					return nil, err
				}
			}
		}
		for _, q := range spec.PodReads {
			if v, ok := source.PodReadResult(q.ID); ok {
				out.PodReads[q.ID], err = wireStruct(v)
				if err != nil {
					return nil, err
				}
			}
		}
		for _, probe := range spec.Vantage {
			if v, ok := source.VantageResult(probe.ID); ok {
				out.Vantage[probe.ID], err = wireStruct(v)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

type remoteProgram struct {
	spec   program.ProgramSpec
	remote *RemoteMemory
}

func (p *remoteProgram) Spec() program.ProgramSpec { return p.spec }

func (p *remoteProgram) execute(ctx context.Context, view program.ReadView, b program.Bindings, operation pb.MemoryProgramInvocation_Operation) (*pb.RunProgramResponse, error) {
	if err := program.ValidateBindings(p.spec, b); err != nil {
		return nil, err
	}
	rpc, _, release, err := p.remote.capture(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer release()
	invocation := &pb.MemoryProgramInvocation{ProgramId: p.spec.ID, Bindings: programBindingsContract(b), Operation: operation}
	response, err := rpc.ExecuteProgram(ctx, invocation, func(call programread.Message) programread.Message {
		return readForRemoteProgram(view, call)
	})
	if err != nil {
		return nil, err
	}
	if response.GetPlanRefused() && response.GetError() != "" {
		return nil, &NonExecutablePlanError{Cause: errors.New(response.GetError())}
	}
	if response.GetError() == sdk.ErrReadReplyLimit.Error() {
		return nil, sdk.ErrReadReplyLimit
	}
	if response.GetError() != "" {
		return nil, errors.New(response.GetError())
	}
	return response, nil
}

func (p *remoteProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	response, err := p.execute(ctx, view, b, pb.MemoryProgramInvocation_OPERATION_DIAGNOSE)
	if err != nil {
		return program.Verdict{}, fmt.Errorf("memory host diagnose: %w", err)
	}
	if response.GetVerdict() == nil {
		return program.Verdict{}, errors.New("missing host verdict")
	}
	verdict := programRunState(&pb.ProgramRun{Verdict: response.GetVerdict()}).Verdict
	return verdict, verdict.Validate()
}

func (p *remoteProgram) Mitigate(ctx context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	response, err := p.execute(ctx, view, b, pb.MemoryProgramInvocation_OPERATION_MITIGATE)
	if err != nil {
		return program.Plan{}, fmt.Errorf("memory host mitigate: %w", err)
	}
	result := response.GetPlan()
	if result == nil {
		return program.Plan{}, errors.New("missing host plan")
	}
	plan := program.Plan{Rationale: result.Rationale}
	for _, a := range result.Actions {
		plan.Actions = append(plan.Actions, planActionState(a))
	}
	if err := plan.Validate(); err != nil {
		return program.Plan{}, &NonExecutablePlanError{Cause: err}
	}
	return plan, nil
}

// capture retains the active host lease and any requested mirror barrier through the RPC.
func (r *RemoteMemory) capture(ctx context.Context, view sdk.DetectionContext) (MemoryHostRPC, uint64, func(), error) {
	if cycle := memoryCycle(view); cycle != nil {
		view = cycle
	}
	host, seq, release, err := r.rpc.Capture(ctx, view)
	if err != nil {
		return nil, 0, nil, err
	}
	identity, ok := host.(interface{ MemoryCommit() string })
	if !ok || identity.MemoryCommit() != r.memoryCommit {
		release()
		return nil, 0, nil, errors.New("memory adapter belongs to a retired accepted revision")
	}
	rpc, ok := host.(MemoryHostRPC)
	if !ok {
		release()
		return nil, 0, nil, errors.New("host lacks program RPCs")
	}
	return rpc, seq, release, nil
}
