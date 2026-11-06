package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/usage"
)

// MemoryHostRPC is implemented by the host manager, which leases the active
// child during each call. The controller only receives findings and plans.
type MemoryHostRPC interface {
	Evaluate(context.Context, *pb.EvaluateRequest) (*pb.EvaluateResponse, error)
	Diagnose(context.Context, *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error)
	Mitigate(context.Context, *pb.MitigateRequest) (*pb.MitigateResponse, error)
}

// MirroredMemoryRPC holds the exact frozen-view barrier for each remote call.
type MirroredMemoryRPC interface {
	MemoryHostRPC
	Capture(context.Context, sdk.DetectionContext) (memoryhost.Host, uint64, func(), error)
}
type RemoteMemory struct {
	Detectors     []sdk.Detector
	Programs      program.ProgramRegistry
	rpc           MirroredMemoryRPC
	memoryCommit  string
	registrations []memoryhost.Registration
}

func NewRemoteMemory(ready *pb.ReadyResponse, rpc MirroredMemoryRPC) (*RemoteMemory, error) {
	if ready == nil || rpc == nil {
		return nil, errors.New("remote memory requires identity and mirrored transport")
	}
	r := &RemoteMemory{rpc: rpc, memoryCommit: ready.GetMemoryCommit()}
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

type remoteProgramRegistry struct{ program.ProgramRegistry }

type remoteDetector struct {
	memoryhost.Registration
	remote *RemoteMemory
}

func (d *remoteDetector) Spec() sdk.DetectorSpec                { return d.DetectorSpec }
func (d *remoteDetector) MetricQueries() []metrics.Query        { return d.Metrics }
func (d *remoteDetector) LogQueries() []logs.Query              { return d.Logs }
func (d *remoteDetector) ConsumesUsage() bool                   { return d.UsesUsage }
func (d *remoteDetector) UsageRequirements() usage.Requirements { return d.Usage }
func (d *remoteDetector) ConsumesServiceReachability() bool     { return d.Reachability }
func (d *remoteDetector) ConsumesAlerts() bool                  { return d.Alerts }
func (d *remoteDetector) ConsumesPodAdmission() bool            { return d.PodAdmission }
func (d *remoteDetector) TrafficWorkloads() []string            { return d.Traffic }
func (d *remoteDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
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
	return max(window, trafficConsumerWindow(d.Traffic, view))
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
		return nil, errors.New(result.Error)
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
	var watched []sdk.ObjectRef
	for _, ref := range refs {
		for _, watch := range watches {
			if ref.Kind == watch.Kind && (watch.APIVersion == "" || ref.APIVersion == watch.APIVersion) &&
				(watch.Namespace == "" || ref.Namespace == watch.Namespace) {
				watched = append(watched, ref)
				break
			}
		}
	}
	return frozenMemoryReads(view, watched)
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
func (r *RemoteMemory) buildObservation(view sdk.DetectionContext) (*pb.MemoryObservation, error) {
	sourceView := view
	for {
		wrapper, ok := sourceView.(interface{ UnderlyingReadView() program.ReadView })
		if !ok {
			break
		}
		sourceView = wrapper.UnderlyingReadView()
	}
	source := inputs.New(sourceView)
	out := &pb.MemoryObservation{Traffic: map[string]*structpb.Struct{}, Metrics: map[string]*structpb.Struct{}, Logs: map[string]*structpb.Struct{}}
	if at := source.ObservedAt(); !at.IsZero() {
		out.ObservedAt = timestamppb.New(at)
		if err := out.ObservedAt.CheckValid(); err != nil {
			return nil, err
		}
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
	for _, spec := range r.registrations {
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
	}
	return out, nil
}

type remoteProgram struct {
	spec   program.ProgramSpec
	remote *RemoteMemory
}

func (p *remoteProgram) Spec() program.ProgramSpec { return p.spec }

// reads freezes the source and baseline diffs of every object the program may inspect. It runs before the
// host lease and mirror barrier are taken, because a diff may call the API server.
func (p *remoteProgram) reads(view program.ReadView, b program.Bindings) ([]*pb.MemoryReadReceipt, error) {
	if err := program.ValidateBindings(p.spec, b); err != nil {
		return nil, err
	}
	refs, err := snapshotMemoryRefs(view)
	if err != nil {
		return nil, err
	}
	seen := map[sdk.ObjectRef]bool{}
	for _, ref := range refs {
		seen[ref] = true
	}
	names := make([]string, 0, len(b.Objects))
	for name := range b.Objects {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if ref := b.Objects[name]; !seen[ref] {
			refs = append(refs, ref)
			seen[ref] = true
		}
	}
	return frozenMemoryReads(view, refs)
}
func (p *remoteProgram) input(view program.ReadView, b program.Bindings, sequence uint64, reads []*pb.MemoryReadReceipt) (*pb.MemoryProgramInput, error) {
	observation, err := p.remote.observation(view)
	if err != nil {
		return nil, err
	}
	return &pb.MemoryProgramInput{ProgramId: p.spec.ID, MirrorSequence: sequence, Observation: observation, Reads: reads, Bindings: programBindingsContract(b)}, nil
}
func recordHostReads(view program.ReadView, reads []*pb.MemoryReadReceipt) error {
	calls, err := memoryhost.DecodeReceipts(reads)
	if err != nil {
		return err
	}
	if len(calls) == 0 {
		return nil
	}
	sink, ok := view.(interface{ RecordExternalCalls([]sdktest.PrimitiveCall) })
	if !ok {
		if wrapper, wrapped := view.(interface{ UnderlyingReadView() program.ReadView }); wrapped {
			return recordHostReads(wrapper.UnderlyingReadView(), reads)
		}
		return errors.New("remote program read receipts have no evidence sink")
	}
	sink.RecordExternalCalls(calls)
	return nil
}
func (p *remoteProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	reads, err := p.reads(view, b)
	if err != nil {
		return program.Verdict{}, err
	}
	rpc, sequence, release, err := p.remote.scopedCapture(ctx, view)
	if err != nil {
		return program.Verdict{}, err
	}
	defer release()
	input, err := p.input(view, b, sequence, reads)
	if err != nil {
		return program.Verdict{}, err
	}
	response, err := rpc.Diagnose(ctx, &pb.DiagnoseRequest{Input: input})
	if err != nil {
		return program.Verdict{}, fmt.Errorf("memory host diagnose: %w", err)
	}
	if response == nil {
		return program.Verdict{}, errors.New("empty host diagnosis")
	}
	if err = recordHostReads(view, response.Reads); err != nil {
		return program.Verdict{}, err
	}
	if response.Error != "" {
		return program.Verdict{}, errors.New(response.Error)
	}
	if response.Verdict == nil {
		return program.Verdict{}, errors.New("missing host verdict")
	}
	verdict := programRunState(&pb.ProgramRun{Verdict: response.Verdict}).Verdict
	return verdict, verdict.Validate()
}
func (p *remoteProgram) Mitigate(ctx context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	reads, err := p.reads(view, b)
	if err != nil {
		return program.Plan{}, err
	}
	rpc, sequence, release, err := p.remote.scopedCapture(ctx, view)
	if err != nil {
		return program.Plan{}, err
	}
	defer release()
	input, err := p.input(view, b, sequence, reads)
	if err != nil {
		return program.Plan{}, err
	}
	response, err := rpc.Mitigate(ctx, &pb.MitigateRequest{Input: input})
	if err != nil {
		return program.Plan{}, fmt.Errorf("memory host mitigate: %w", err)
	}
	if response == nil {
		return program.Plan{}, errors.New("empty host mitigation")
	}
	if err = recordHostReads(view, response.Reads); err != nil {
		return program.Plan{}, err
	}
	if response.Error != "" {
		return program.Plan{}, errors.New(response.Error)
	}
	if response.Plan == nil {
		return program.Plan{}, errors.New("missing host plan")
	}
	plan := program.Plan{Rationale: response.Plan.Rationale}
	for _, a := range response.Plan.Actions {
		plan.Actions = append(plan.Actions, planActionState(a))
	}
	return plan, plan.Validate()
}

// capture retains the active host lease and exact mirror barrier through the RPC.
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
