// Package memoryhost owns the accepted-memory execution boundary. It exposes
// the same protocol for local children and future externally launched hosts.
package memoryhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Host is a live connection plus its launcher-owned lifetime. Implementations
// must honor context cancellation and Close must terminate and reap the child.
type Host interface {
	Ready(context.Context) (*pb.ReadyResponse, error)
	Evaluate(context.Context, *pb.EvaluateRequest) (*pb.EvaluateResponse, error)
	Close() error
}

// Manager keeps the old host serving while a candidate is prepared. Calls hold
// a read lease so retirement cannot kill an evaluation already in flight.
type Manager struct {
	mirror         mirrorState
	swap           sync.Mutex
	mu             sync.RWMutex
	active         Host
	revision       string
	timeout        time.Duration
	closed         bool
	generation     uint64
	recovering     bool
	trafficRunning bool
	recovery       func(context.Context) (Host, string, string, uint64, error)
}

func NewManager(timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Manager{timeout: timeout}
}
func (m *Manager) Swap(ctx context.Context, h Host, commit, digest string, sequence uint64) (err error) {
	if h == nil {
		return errors.New("memory host is required")
	}
	m.swap.Lock()
	defer m.swap.Unlock()
	accepted := false
	defer func() {
		if !accepted {
			_ = h.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	identity, err := h.Ready(ctx)
	if err != nil {
		return fmt.Errorf("memory host ready: %w", err)
	}
	if commit == "" || digest == "" || identity.GetMemoryCommit() != commit || identity.GetBinaryDigest() != digest || !identity.GetMirrorComplete() || identity.GetMirrorSequence() != sequence {
		return errors.New("memory host identity or mirror does not match accepted revision")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("memory host manager is closed")
	}
	if err := m.resumeTraffic(ctx, h); err != nil {
		m.mu.Unlock()
		return err
	}
	old := m.active
	m.active = h
	m.revision = commit
	m.generation++
	accepted = true
	m.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}
func (m *Manager) Evaluate(ctx context.Context, request *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	m.mu.RLock()
	h := m.active
	if h == nil {
		m.mu.RUnlock()
		return unavailable(request, errors.New("memory host is unavailable")), nil
	}
	callCtx, cancel := context.WithTimeout(ctx, m.timeout)
	response, err := h.Evaluate(callCtx, request)
	cancel()
	m.mu.RUnlock()
	if err == nil {
		return response, nil
	}
	m.failed(h)
	return unavailable(request, err), nil
}
func (m *Manager) failed(h Host) {
	// An old connection's late failure must never evict a newer accepted host.
	m.mu.Lock()
	failed := m.active == h
	if failed {
		m.active = nil
		_ = h.Close()
	}
	recovery := m.recovery
	m.mu.Unlock()
	if failed && recovery != nil {
		go m.retryRecovery()
	}
}
func unavailable(request *pb.EvaluateRequest, err error) *pb.EvaluateResponse {
	response := &pb.EvaluateResponse{}
	for _, id := range request.GetDetectorIds() {
		response.Results = append(response.Results, &pb.MemoryDetectorResult{DetectorId: id, Error: fmt.Sprintf("memory host unavailable: %v", err)})
	}
	return response
}
func (m *Manager) Close() error {
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	defer func() {
		for host := range m.mirror.hosts {
			if host != m.active {
				_ = host.Close()
			}
		}
		m.mirror.hosts = nil
	}()
	m.swap.Lock()
	defer m.swap.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.active == nil {
		return nil
	}
	h := m.active
	m.active = nil
	return h.Close()
}

// SetRecovery supplies a launcher that replays the latest mirror into a fresh
// child. A concurrent accepted-memory swap always supersedes crash recovery.
func (m *Manager) SetRecovery(prepare func(context.Context) (Host, string, string, uint64, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recovery = prepare
}
func (m *Manager) recover(generation uint64, prepare func(context.Context) (Host, string, string, uint64, error)) {
	defer func() { m.mu.Lock(); m.recovering = false; m.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	h, commit, digest, sequence, err := prepare(ctx)
	if err != nil {
		if h != nil {
			m.mirror.mu.Lock()
			delete(m.mirror.hosts, h)
			m.mirror.mu.Unlock()
			_ = h.Close()
		}
		return
	}
	if h == nil {
		return
	}
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	if seq, ok := m.mirror.hosts[h]; ok {
		sequence = seq
	}
	m.swap.Lock()
	defer m.swap.Unlock()
	identity, err := h.Ready(ctx)
	if err != nil || identity.GetMemoryCommit() != commit || identity.GetBinaryDigest() != digest || !identity.GetMirrorComplete() || identity.GetMirrorSequence() != sequence {
		delete(m.mirror.hosts, h)
		_ = h.Close()
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.generation != generation || m.active != nil {
		delete(m.mirror.hosts, h)
		_ = h.Close()
		return
	}
	if err := m.resumeTraffic(ctx, h); err != nil {
		delete(m.mirror.hosts, h)
		_ = h.Close()
		return
	}
	m.active = h
	m.revision = commit
	m.generation++
}

// Diagnose and Mitigate lease the same accepted child as detector evaluation.
// Transport failure is an error, never clearance or an empty authorized plan.
func (m *Manager) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.active.(interface {
		Diagnose(context.Context, *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error)
	})
	if !ok {
		return nil, errors.New("memory host diagnosis unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return h.Diagnose(ctx, r)
}
func (m *Manager) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.active.(interface {
		Mitigate(context.Context, *pb.MitigateRequest) (*pb.MitigateResponse, error)
	})
	if !ok {
		return nil, errors.New("memory host mitigation unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return h.Mitigate(ctx, r)
}

// retryRecovery retries a failed restart on the next demand, without spinning
// or introducing a wall-clock retry loop in the controller.
func (m *Manager) retryRecovery() {
	m.mu.Lock()
	if m.closed || m.active != nil || m.recovering || m.recovery == nil {
		m.mu.Unlock()
		return
	}
	m.recovering = true
	generation, prepare := m.generation, m.recovery
	m.mu.Unlock()
	m.recover(generation, prepare)
}
