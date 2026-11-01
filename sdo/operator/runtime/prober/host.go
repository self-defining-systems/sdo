package prober

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/servicehealth"
)

// Host owns the traffic engine and scheduler for one immutable memory revision.
// The lifetime is independent of any individual RPC. Stop and Close join every
// workload before returning; a closed host cannot be restarted.
type Host struct {
	engine *Prober
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	closed bool
}

func NewHost(engine *Prober) *Host { return &Host{engine: engine} }
func (h *Host) stop() {
	if h.cancel != nil {
		h.cancel()
		<-h.done
		h.cancel = nil
	}
}
func (h *Host) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	h.stop()
}
func trafficStruct(v any) (*structpb.Struct, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := &structpb.Struct{}
	err = out.UnmarshalJSON(raw)
	return out, err
}
func (h *Host) Traffic(ctx context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.closed || h.engine == nil {
		h.mu.Unlock()
		return nil, errors.New("traffic host is closed or unavailable")
	}
	if r.GetOperation() == pb.TrafficRequest_OPERATION_START || r.GetOperation() == pb.TrafficRequest_OPERATION_STOP {
		defer h.mu.Unlock()
	} else {
		h.mu.Unlock()
	}
	out := &pb.TrafficResponse{}
	switch r.GetOperation() {
	case pb.TrafficRequest_OPERATION_START:
		if h.cancel == nil {
			running, cancel := context.WithCancel(context.Background())
			h.cancel = cancel
			h.done = make(chan struct{})
			go func() { defer close(h.done); h.engine.Run(running) }()
		}
	case pb.TrafficRequest_OPERATION_STOP:
		h.stop()
	case pb.TrafficRequest_OPERATION_RESET:
		h.engine.Reset()
	case pb.TrafficRequest_OPERATION_WINDOWS:
		out.Windows = map[string]*structpb.Struct{}
		for name, window := range h.engine.Windows() {
			wire, err := trafficStruct(window)
			if err != nil {
				return nil, err
			}
			out.Windows[name] = wire
		}
	case pb.TrafficRequest_OPERATION_BURST:
		result, err := h.engine.Burst(ctx, BurstRequest{Workload: r.Workload, Scenarios: r.Scenarios})
		if err != nil {
			return nil, err
		}
		out.Burst, err = trafficStruct(result)
		if err != nil {
			return nil, err
		}
	case pb.TrafficRequest_OPERATION_DIALS:
		request := DialRequest{Timeout: time.Duration(r.TimeoutNs)}
		for _, target := range r.DialTargets {
			request.Targets = append(request.Targets, servicehealth.ReachabilityTarget{Service: target.GetService(), Port: target.GetPort(), PortName: target.GetPortName()})
		}
		result, err := h.engine.DialTargets(ctx, request)
		if err != nil {
			return nil, err
		}
		out.Dials, err = trafficStruct(result)
		if err != nil {
			return nil, err
		}
	case pb.TrafficRequest_OPERATION_PROBE:
		target := r.GetTarget()
		request := PrimitiveRequest{To: sdk.ObjectRef{APIVersion: target.GetApiVersion(), Kind: target.GetKind(), Namespace: target.GetNamespace(), Name: target.GetName()}, Port: r.Port, PodIP: r.PodIp, Timeout: time.Duration(r.TimeoutNs)}
		if r.HttpMethod != "" || r.HttpPath != "" || len(r.HttpHeaders) > 0 {
			request.HTTP = &program.HTTPProbe{Method: r.HttpMethod, Path: r.HttpPath, Headers: r.HttpHeaders}
		}
		result, err := h.engine.Primitive(ctx, request)
		if err != nil {
			return nil, err
		}
		out.Success, out.Detail = result.Success, result.Detail
	default:
		return nil, errors.New("unknown traffic operation")
	}
	return out, nil
}
