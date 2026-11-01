package runtime

import (
	"context"
	"encoding/json"
	"errors"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

// MemoryTraffic reads observations from the active immutable host revision.
type MemoryTraffic struct{ RPC memoryhost.TrafficRPC }

func (c MemoryTraffic) control(ctx context.Context, op pb.TrafficRequest_Operation) error {
	_, err := c.RPC.Traffic(ctx, &pb.TrafficRequest{Operation: op})
	return err
}
func (c MemoryTraffic) Start(ctx context.Context) error {
	return c.control(ctx, pb.TrafficRequest_OPERATION_START)
}
func (c MemoryTraffic) Stop(ctx context.Context) error {
	return c.control(ctx, pb.TrafficRequest_OPERATION_STOP)
}
func (c MemoryTraffic) Reset(ctx context.Context) error {
	return c.control(ctx, pb.TrafficRequest_OPERATION_RESET)
}
func (c MemoryTraffic) Windows(ctx context.Context) (map[string]traffic.Window, error) {
	response, err := c.RPC.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_WINDOWS})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("missing traffic response")
	}
	out := map[string]traffic.Window{}
	for name, wire := range response.Windows {
		if wire == nil {
			return nil, errors.New("missing traffic window")
		}
		raw, err := wire.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var window traffic.Window
		if err := json.Unmarshal(raw, &window); err != nil {
			return nil, err
		}
		out[name] = window
	}
	return out, nil
}
func (c MemoryTraffic) Burst(ctx context.Context, r prober.BurstRequest) (prober.BurstResult, error) {
	response, err := c.RPC.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_BURST, Workload: r.Workload, Scenarios: r.Scenarios})
	if err != nil {
		return prober.BurstResult{}, err
	}
	if response.GetBurst() == nil {
		return prober.BurstResult{}, errors.New("missing burst result")
	}
	raw, err := response.Burst.MarshalJSON()
	if err != nil {
		return prober.BurstResult{}, err
	}
	var result prober.BurstResult
	err = json.Unmarshal(raw, &result)
	return result, err
}
func (c MemoryTraffic) Probe(ctx context.Context, ref sdk.ObjectRef, port int32, http *program.HTTPProbe) (program.ProbeResult, error) {
	r := &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_PROBE, Target: objectRefContract(ref), Port: port}
	if http != nil {
		r.HttpMethod = http.Method
		if r.HttpMethod == "" {
			r.HttpMethod = "GET"
		}
		r.HttpPath = http.Path
		r.HttpHeaders = http.Headers
	}
	return c.probe(ctx, r)
}
func (c MemoryTraffic) ProbePod(ctx context.Context, ref sdk.ObjectRef, ip string, port int32) (program.ProbeResult, error) {
	return c.probe(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_PROBE, Target: objectRefContract(ref), PodIp: ip, Port: port})
}
func (c MemoryTraffic) probe(ctx context.Context, r *pb.TrafficRequest) (program.ProbeResult, error) {
	response, err := c.RPC.Traffic(ctx, r)
	if err != nil {
		return program.ProbeResult{}, err
	}
	if response == nil {
		return program.ProbeResult{}, errors.New("missing primitive result")
	}
	return program.ProbeResult{Success: response.Success, Detail: response.Detail}, nil
}

func (c MemoryTraffic) DialTargets(ctx context.Context, r prober.DialRequest) (prober.DialResponse, error) {
	request := &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_DIALS, TimeoutNs: int64(r.Timeout)}
	for _, target := range r.Targets {
		request.DialTargets = append(request.DialTargets, &pb.TrafficDialTarget{Service: target.Service, Port: target.Port, PortName: target.PortName})
	}
	response, err := c.RPC.Traffic(ctx, request)
	if err != nil {
		return prober.DialResponse{}, err
	}
	if response.GetDials() == nil {
		return prober.DialResponse{}, errors.New("missing dial results")
	}
	raw, err := response.Dials.MarshalJSON()
	if err != nil {
		return prober.DialResponse{}, err
	}
	var result prober.DialResponse
	err = json.Unmarshal(raw, &result)
	return result, err
}
