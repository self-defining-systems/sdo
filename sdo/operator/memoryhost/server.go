package memoryhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
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

// Server owns registrations and a controller-fed mirror. Each evaluation gets
// a copy. Program read factories may provide a read-only Kubernetes capability;
// the host never receives a controller callback or mutation capability.
type Server struct {
	pb.UnimplementedMemoryHostServiceServer
	traffic interface {
		Traffic(context.Context, *pb.TrafficRequest) (*pb.TrafficResponse, error)
	}
	mu          sync.RWMutex
	identity    *pb.ReadyResponse
	programs    program.ProgramRegistry
	readFactory func(context.Context, sdktest.Snapshot) (program.ReadView, error)
	detectors   map[string]sdk.Detector
	objects     map[string]*pb.MemoryObject
	history     []*mirrorVersion
	namespace   string
	generic     ObjectSource
	protected   string

	runMu   sync.Mutex
	running map[string]bool
}

// mirrorHistory is how many applied mirror sequences the host keeps readable.
// An evaluation is built against the sequence its caller froze; later frames
// must not invalidate it, so a lease never has to block the mirror's progress.
const mirrorHistory = 16

// mirrorVersion is one immutable applied mirror state. Its map is never
// modified after it is recorded.
type mirrorVersion struct {
	once      sync.Once
	snapshot  sdktest.Snapshot
	err       error
	sequence  uint64
	complete  bool
	namespace string
	objects   map[string]*pb.MemoryObject
}

func NewServer(commit, digest string, detectors []sdk.Detector) (*Server, error) {
	if commit == "" || digest == "" {
		return nil, errors.New("memory commit and binary digest are required")
	}
	s := &Server{identity: &pb.ReadyResponse{MemoryCommit: commit, BinaryDigest: digest}, detectors: map[string]sdk.Detector{}, objects: map[string]*pb.MemoryObject{}}
	for _, d := range detectors {
		if d == nil {
			return nil, errors.New("nil detector")
		}
		spec := d.Spec()
		if spec.ID == "" || s.detectors[spec.ID] != nil {
			return nil, fmt.Errorf("invalid or duplicate detector %q", spec.ID)
		}
		raw, err := json.Marshal(registration(d))
		if err != nil {
			return nil, err
		}
		var fields map[string]any
		if err = json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		registration, err := structpb.NewStruct(fields)
		if err != nil {
			return nil, err
		}
		s.detectors[spec.ID] = d
		s.identity.Detectors = append(s.identity.Detectors, &pb.MemoryDetectorRegistration{Id: spec.ID, Specification: registration})
	}
	return s, nil
}
func (s *Server) Ready(context.Context, *pb.ReadyRequest) (*pb.ReadyResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.identity).(*pb.ReadyResponse), nil
}
func objectKey(r *pb.ObjectRef) string {
	return r.GetApiVersion() + "/" + r.GetKind() + "/" + r.GetNamespace() + "/" + r.GetName()
}
func (s *Server) Apply(frame *pb.MirrorRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if frame == nil || frame.Sequence != s.identity.MirrorSequence+1 {
		return errors.New("noncontiguous mirror sequence")
	}
	if s.identity.MirrorSequence == 0 && !frame.ResetMirror {
		return errors.New("initial mirror frame must reset")
	}
	if frame.ResetMirror && frame.Namespace == "" {
		return errors.New("mirror namespace required")
	}
	namespace := s.namespace
	if frame.ResetMirror {
		namespace = frame.Namespace
	}
	if frame.Namespace != "" && frame.Namespace != namespace {
		return errors.New("mirror namespace mismatch")
	}
	for _, ref := range frame.Deletes {
		if ref == nil || ref.Name == "" || snapshotFields[ref.Kind] == "" || (ref.Namespace != namespace && !(ref.Kind == "Node" && ref.Namespace == "")) {
			return errors.New("invalid mirror deletion")
		}
	}
	for _, item := range frame.Upserts {
		if item == nil {
			return errors.New("nil mirror object")
		}
		if item.GetRef().GetKind() == "" || item.GetRef().GetName() == "" || item.Object == nil {
			return errors.New("mirror object requires ref and body")
		}
		if item.Ref.Namespace != namespace && !(item.Ref.Kind == "Node" && item.Ref.Namespace == "") {
			return errors.New("mirror object outside namespace")
		}
		if snapshotFields[item.Ref.Kind] == "" {
			return fmt.Errorf("unsupported mirror kind %s", item.Ref.Kind)
		}
		if item.Ref.Kind == "Secret" {
			for key := range item.Object.Fields {
				if !secretFields[key] {
					return errors.New("Secret mirror may contain metadata only")
				}
			}
		}
	}
	// Copy on write: a recorded version is read without the lock by evaluations.
	objects := make(map[string]*pb.MemoryObject, len(s.objects)+len(frame.Upserts))
	if !frame.ResetMirror {
		for key, item := range s.objects {
			objects[key] = item
		}
	}
	for _, item := range frame.Upserts {
		objects[objectKey(item.Ref)] = proto.Clone(item).(*pb.MemoryObject)
	}
	for _, ref := range frame.Deletes {
		delete(objects, objectKey(ref))
	}
	s.objects = objects
	s.namespace = namespace
	s.identity.MirrorSequence = frame.Sequence
	s.identity.MirrorComplete = frame.Complete
	s.history = append(s.history, &mirrorVersion{sequence: frame.Sequence, complete: frame.Complete, namespace: namespace, objects: objects})
	if len(s.history) > mirrorHistory {
		s.history = append([]*mirrorVersion(nil), s.history[len(s.history)-mirrorHistory:]...)
	}
	return nil
}
func (s *Server) Mirror(stream grpc.ClientStreamingServer[pb.MirrorRequest, pb.MirrorResponse]) error {
	var sequence uint64
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.MirrorResponse{Sequence: sequence})
		}
		if err != nil {
			return err
		}
		if err = s.Apply(frame); err != nil {
			return err
		}
		sequence = frame.Sequence
	}
}

// secretFields are the only fields a mirrored Secret may carry: the JSON
// fields of the value-free sdk.SecretMetadata projection, derived from the
// type so a field added there is admitted here and nothing else is.
var secretFields = func() map[string]bool {
	out := map[string]bool{}
	t := reflect.TypeFor[sdk.SecretMetadata]()
	for i := range t.NumField() {
		if name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); name != "" && name != "-" {
			out[name] = true
		}
	}
	return out
}()

var snapshotFields = map[string]string{"ConfigMap": "configMaps", "Service": "services", "Pod": "pods", "Deployment": "deployments", "StatefulSet": "statefulSets", "DaemonSet": "daemonSets", "Job": "jobs", "CronJob": "cronJobs", "PersistentVolumeClaim": "persistentVolumeClaims", "ServiceAccount": "serviceAccounts", "HorizontalPodAutoscaler": "horizontalPodAutoscalers", "PodDisruptionBudget": "podDisruptionBudgets", "Ingress": "ingresses", "Node": "nodes", "Secret": "secrets", "ReplicaSet": "replicaSets", "Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies", "Event": "events"}

// snapshot builds the view of exactly the requested sequence. A complete
// sequence among the last mirrorHistory is served even after newer frames
// arrived; nothing else is.
func (s *Server) snapshot(sequence uint64) (sdktest.Snapshot, error) {
	s.mu.RLock()
	var version *mirrorVersion
	for i := range s.history {
		if s.history[i].sequence == sequence {
			version = s.history[i]
		}
	}
	s.mu.RUnlock()
	if version == nil || !version.complete {
		return sdktest.Snapshot{}, errors.New("memory mirror incomplete or stale")
	}
	version.once.Do(func() { version.snapshot, version.err = decodeMirror(version) })
	if version.err != nil {
		return sdktest.Snapshot{}, version.err
	}
	return cloneMirrorSnapshot(version.snapshot), nil
}

func decodeMirror(version *mirrorVersion) (sdktest.Snapshot, error) {
	fields := map[string]any{"namespace": version.namespace}
	keys := make([]string, 0, len(version.objects))
	for key := range version.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := version.objects[key]
		field := snapshotFields[item.Ref.Kind]
		items, _ := fields[field].([]any)
		fields[field] = append(items, item.Object.AsMap())
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return sdktest.Snapshot{}, err
	}
	var snapshot sdktest.Snapshot
	err = json.Unmarshal(raw, &snapshot)
	return snapshot, err
}

func (s *Server) Evaluate(ctx context.Context, request *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	snapshot, err := s.snapshot(request.GetMirrorSequence())
	if err != nil {
		return nil, err
	}
	if err = observe(&snapshot, request.GetObservation()); err != nil {
		return nil, err
	}
	reads, err := DecodeReceipts(request.GetReads())
	if err != nil {
		return nil, err
	}
	snapshot.Readings = append(snapshot.Readings[:len(snapshot.Readings):len(snapshot.Readings)], reads...)
	response := &pb.EvaluateResponse{}
	ctx, cancel := withinTransportBudget(ctx)
	defer cancel()
	for _, id := range request.DetectorIds {
		result := &pb.MemoryDetectorResult{DetectorId: id}
		d := s.detectors[id]
		if d == nil {
			result.Error = "detector is not registered"
		} else {
			result.Findings, result.Error = s.runDetector(ctx, d, s.detectionView(snapshot))
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

// transportMargin is how long before the caller's deadline agent code is ended,
// so the host can still answer. The package reads no clock: it only moves the
// deadline the caller sent.
const transportMargin = 250 * time.Millisecond

// withinTransportBudget ends agent code a little before the caller's transport
// deadline, so a hang is a detector error and never a failure of the host.
func withinTransportBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline.Add(-transportMargin))
}

// runDetector runs one detector, abandoning it when it outlives ctx. A detector
// that ignores its context keeps its goroutine, so it is never started twice
// concurrently: the leak is bounded by the number of detectors.
func (s *Server) runDetector(ctx context.Context, d sdk.Detector, view sdk.DetectionContext) ([]*pb.Finding, string) {
	id := d.Spec().ID
	s.runMu.Lock()
	if s.running[id] {
		s.runMu.Unlock()
		return nil, "detector is still running a previous evaluation"
	}
	if s.running == nil {
		s.running = map[string]bool{}
	}
	s.running[id] = true
	s.runMu.Unlock()
	type outcome struct {
		findings []*pb.Finding
		failure  string
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			s.runMu.Lock()
			delete(s.running, id)
			s.runMu.Unlock()
		}()
		findings, failure := detect(ctx, d, view)
		done <- outcome{findings, failure}
	}()
	select {
	case got := <-done:
		return got.findings, got.failure
	case <-ctx.Done():
		return nil, fmt.Sprintf("detector evaluation abandoned: %v", ctx.Err())
	}
}
func detect(ctx context.Context, d sdk.Detector, snapshot sdk.DetectionContext) (findings []*pb.Finding, failure string) {
	defer func() {
		if value := recover(); value != nil {
			findings = nil
			failure = fmt.Sprintf("detector panic: %v", value)
		}
	}()
	got, err := d.Detect(ctx, snapshot)
	if err != nil {
		return nil, err.Error()
	}
	for _, f := range got {
		id := f.DetectorID
		if id == "" {
			id = d.Spec().ID
		}
		out := &pb.Finding{DetectorId: id, RuleId: f.RuleID, Status: string(f.Status), Severity: string(f.Severity), Summary: f.Summary, Evidence: f.Evidence, PrimaryResource: ref(f.PrimaryResource), PlaybookIds: f.Playbooks, Fingerprint: f.Fingerprint, ParameterBindings: map[string]*pb.ObjectRef{}}
		for _, r := range f.RelatedResources {
			out.RelatedResources = append(out.RelatedResources, ref(r))
		}
		for k, v := range f.ParameterBindings {
			out.ParameterBindings[k] = ref(v)
		}
		if f.Metadata != nil {
			out.Metadata, err = encodeStruct(f.Metadata)
			if err != nil {
				return nil, err.Error()
			}
		}
		findings = append(findings, out)
	}
	return findings, ""
}
func ref(r sdk.ObjectRef) *pb.ObjectRef {
	return &pb.ObjectRef{ApiVersion: r.APIVersion, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
}
func decode[T any](value *structpb.Struct) (*T, error) {
	if value == nil {
		return nil, nil
	}
	if err := validStruct(value); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value.AsMap())
	if err != nil {
		return nil, err
	}
	var result T
	err = json.Unmarshal(data, &result)
	return &result, err
}
func observe(snapshot *sdktest.Snapshot, o *pb.MemoryObservation) error {
	if o == nil {
		return nil
	}
	if o.ObservedAt != nil {
		if err := o.ObservedAt.CheckValid(); err != nil {
			return err
		}
		at := o.ObservedAt.AsTime()
		snapshot.ObservedAtTime = &at
	}
	if o.SelfHealHold != nil {
		if err := o.SelfHealHold.CheckValid(); err != nil {
			return err
		}
		hold := o.SelfHealHold.AsDuration()
		snapshot.SelfHealHoldTime = &hold
	}
	// Use the SDK's existing capture decoder: wire and replay share input semantics.
	var err error
	snapshot.Reachability, err = decode[servicehealth.ReachabilityWindow](o.Reachability)
	if err != nil {
		return err
	}
	snapshot.Usage, err = decode[usage.Result](o.Usage)
	if err != nil {
		return err
	}
	snapshot.Alerts, err = decode[metrics.AlertsResult](o.Alerts)
	if err != nil {
		return err
	}
	scoped, err := decode[sdk.ScopedSnapshot](o.Scoped)
	if err != nil {
		return err
	}
	if scoped != nil {
		snapshot.ScopedSnapshot = *scoped
		if err = snapshot.ScopedSnapshot.ValidateObservation(); err != nil {
			return err
		}
	}
	snapshot.Admission, err = decode[sdk.PodAdmissionResult](o.PodAdmission)
	if err != nil {
		return err
	}
	snapshot.Traffic = map[string]traffic.Window{}
	for k, v := range o.Traffic {
		got, e := decode[traffic.Window](v)
		if e != nil {
			return e
		}
		if got != nil {
			snapshot.Traffic[k] = *got
		}
	}
	snapshot.Metrics = map[string]metrics.Result{}
	for k, v := range o.Metrics {
		got, e := decode[metrics.Result](v)
		if e != nil {
			return e
		}
		if got != nil {
			snapshot.Metrics[k] = *got
		}
	}
	snapshot.Logs = map[string]logs.Result{}
	for k, v := range o.Logs {
		got, e := decode[logs.Result](v)
		if e != nil {
			return e
		}
		if got != nil {
			snapshot.Logs[k] = *got
		}
	}
	snapshot.PodReads = map[string]podread.Result{}
	for k, v := range o.PodReads {
		got, e := decode[podread.Result](v)
		if e != nil {
			return e
		}
		if got != nil {
			snapshot.PodReads[k] = *got
		}
	}
	snapshot.Vantage = map[string]vantage.Result{}
	for k, v := range o.Vantage {
		got, e := decode[vantage.Result](v)
		if e != nil {
			return e
		}
		if got != nil {
			snapshot.Vantage[k] = *got
		}
	}
	return nil
}

// WithTraffic installs the immutable revision's host-owned traffic executor.
func (s *Server) WithTraffic(executor interface {
	Traffic(context.Context, *pb.TrafficRequest) (*pb.TrafficResponse, error)
}) {
	s.traffic = executor
}
func (s *Server) Traffic(ctx context.Context, request *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	if s.traffic == nil {
		return nil, errors.New("memory host traffic unavailable")
	}
	return s.traffic.Traffic(ctx, request)
}
