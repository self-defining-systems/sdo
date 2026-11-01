package memoryhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
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
	namespace   string
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
				if key != "name" && key != "namespace" && key != "type" && key != "dataKeys" && key != "labels" {
					return errors.New("Secret mirror may contain metadata only")
				}
			}
		}
	}
	if frame.ResetMirror {
		s.objects = map[string]*pb.MemoryObject{}
		s.namespace = namespace
		s.identity.MirrorComplete = false
	}
	for _, item := range frame.Upserts {
		s.objects[objectKey(item.Ref)] = proto.Clone(item).(*pb.MemoryObject)
	}
	for _, ref := range frame.Deletes {
		delete(s.objects, objectKey(ref))
	}
	s.identity.MirrorSequence = frame.Sequence
	s.identity.MirrorComplete = frame.Complete
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

var snapshotFields = map[string]string{"ConfigMap": "configMaps", "Service": "services", "Pod": "pods", "Deployment": "deployments", "StatefulSet": "statefulSets", "DaemonSet": "daemonSets", "Job": "jobs", "CronJob": "cronJobs", "PersistentVolumeClaim": "persistentVolumeClaims", "ServiceAccount": "serviceAccounts", "HorizontalPodAutoscaler": "horizontalPodAutoscalers", "PodDisruptionBudget": "podDisruptionBudgets", "Ingress": "ingresses", "Node": "nodes", "Secret": "secrets", "ReplicaSet": "replicaSets", "Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies", "Event": "events"}

func (s *Server) snapshot(sequence uint64) (sdktest.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.identity.MirrorComplete || sequence != s.identity.MirrorSequence {
		return sdktest.Snapshot{}, errors.New("memory mirror incomplete or stale")
	}
	fields := map[string]any{"namespace": s.namespace}
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := s.objects[key]
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
	snapshot, err := s.requestSnapshot(request.GetMirrorSequence(), request.GetObservation())
	if err != nil {
		return nil, err
	}
	if err = observe(&snapshot, request.GetObservation()); err != nil {
		return nil, err
	}
	response := &pb.EvaluateResponse{}
	for _, id := range request.DetectorIds {
		result := &pb.MemoryDetectorResult{DetectorId: id}
		d := s.detectors[id]
		if d == nil {
			result.Error = "detector is not registered"
		} else {
			result.Findings, result.Error = detect(ctx, d, snapshot)
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
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
	data, err := value.MarshalJSON()
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
	return nil
}

// requestSnapshot accepts only the completed mirror selected by the request.
func (s *Server) requestSnapshot(sequence uint64, _ *pb.MemoryObservation) (sdktest.Snapshot, error) {
	return s.snapshot(sequence)
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
