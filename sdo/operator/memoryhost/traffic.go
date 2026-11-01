package memoryhost

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"time"
)

// TrafficRPC never exposes a cluster mutation capability.
type TrafficRPC interface {
	Traffic(context.Context, *pb.TrafficRequest) (*pb.TrafficResponse, error)
}

func (m *Manager) Traffic(ctx context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	m.swap.Lock()
	defer m.swap.Unlock()
	m.mu.RLock()
	h := m.active
	rpc, ok := h.(TrafficRPC)
	if !ok {
		m.mu.RUnlock()
		go m.retryRecovery()
		return nil, errors.New("memory host traffic unavailable")
	}
	switch r.GetOperation() {
	case pb.TrafficRequest_OPERATION_START:
		m.trafficRunning = true
	case pb.TrafficRequest_OPERATION_STOP:
		m.trafficRunning = false
	}
	timeout := m.timeout
	// Accepted verification workloads may run for sixty seconds. Keep their
	// transport budget separate from short detector RPCs, while honoring caller cancellation.
	if r.GetOperation() == pb.TrafficRequest_OPERATION_BURST {
		timeout = max(timeout, 90*time.Second)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	response, err := rpc.Traffic(ctx, r)
	cancel()
	m.mu.RUnlock()
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		m.failed(h)
	}
	return response, err
}

// resumeTraffic is called while the active-host lease is exclusively held.
// Prepared hosts remain idle until activation; recovery restores the desired
// scheduler state without retaining samples from a different process/revision.
func (m *Manager) resumeTraffic(ctx context.Context, h Host) error {
	if !m.trafficRunning {
		return nil
	}
	rpc, ok := h.(TrafficRPC)
	if !ok {
		return errors.New("candidate lacks traffic RPC")
	}
	_, err := rpc.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_START})
	return err
}
