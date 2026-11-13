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

// Manager keeps the old host serving while a candidate is prepared. A call
// pins the host it uses, so retirement waits for it, but no lock is held while
// a call runs: a slow or hung evaluation never delays another evaluation, a
// swap, a failure or a recovery. mu guards fields only; the order of the three
// locks is mirror.mu, then swap, then mu.
type Manager struct {
	mirror         mirrorState
	swap           sync.Mutex
	mu             sync.Mutex
	active         Host
	uses           map[Host]*hostUse
	revision       string
	timeout        time.Duration
	closed         bool
	generation     uint64
	recovering     bool
	trafficRunning bool
	recovery       func(context.Context) (Host, string, string, uint64, error)
}

// hostUse counts the calls in flight on a host. A retired host closes when the
// last one ends.
type hostUse struct {
	calls   int
	retired bool
}

// pin leases the active host for one call; the caller must unpin a non-nil host.
func (m *Manager) pin() Host {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		m.pinLocked(m.active)
	}
	return m.active
}
func (m *Manager) pinLocked(h Host) {
	use := m.uses[h]
	if use == nil {
		use = &hostUse{}
		if m.uses == nil {
			m.uses = map[Host]*hostUse{}
		}
		m.uses[h] = use
	}
	use.calls++
}
func (m *Manager) unpin(h Host) {
	m.mu.Lock()
	closeNow := false
	if use := m.uses[h]; use != nil {
		use.calls--
		if use.calls == 0 {
			delete(m.uses, h)
			closeNow = use.retired
		}
	}
	m.mu.Unlock()
	if closeNow {
		_ = h.Close()
	}
}

// retireLocked reports whether a host that stopped being active can close now.
func (m *Manager) retireLocked(h Host) bool {
	use := m.uses[h]
	if use == nil || use.calls == 0 {
		return true
	}
	use.retired = true
	return false
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
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return errors.New("memory host manager is closed")
	}
	// Swap, recovery and traffic changes serialize on m.swap, so the desired
	// traffic state is stable here and the RPC runs outside m.mu.
	if err := m.resumeTraffic(ctx, h); err != nil {
		return err
	}
	m.mu.Lock()
	old := m.active
	m.active = h
	m.revision = commit
	m.generation++
	accepted = true
	closeOld := old != nil && m.retireLocked(old)
	m.mu.Unlock()
	if closeOld {
		_ = old.Close()
	}
	return nil
}
func (m *Manager) Evaluate(ctx context.Context, request *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	h := m.pin()
	if h == nil {
		return unavailable(request, errors.New("memory host is unavailable")), nil
	}
	callCtx, cancel := context.WithTimeout(ctx, m.timeout)
	response, err := h.Evaluate(callCtx, request)
	cancel()
	m.unpin(h)
	if err == nil {
		return response, nil
	}
	// Caller cancellation bounds this evaluation, not the lifetime of the serving host.
	if ctx.Err() == nil {
		m.failed(h)
	}
	return unavailable(request, err), nil
}

// failed evicts a host whose transport failed and starts its replacement.
// Calls still running on it end with the child.
func (m *Manager) failed(h Host) {
	// An old connection's late failure must never evict a newer accepted host.
	m.mu.Lock()
	failed := m.active == h
	if failed {
		m.active = nil
		delete(m.uses, h)
	}
	recovery := m.recovery
	m.mu.Unlock()
	if failed {
		_ = h.Close()
	}
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
	m.swap.Lock()
	defer m.swap.Unlock()
	m.mu.Lock()
	m.closed = true
	active := m.active
	m.active = nil
	// Candidates and retired hosts still serving a call close with the manager.
	others := map[Host]bool{}
	for host := range m.mirror.hosts {
		others[host] = true
	}
	for host := range m.uses {
		others[host] = true
	}
	delete(others, active)
	m.mirror.hosts = nil
	m.uses = nil
	m.mu.Unlock()
	for host := range others {
		_ = host.Close()
	}
	if active == nil {
		return nil
	}
	return active.Close()
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
	reject := func() {
		delete(m.mirror.hosts, h)
		_ = h.Close()
	}
	identity, err := h.Ready(ctx)
	if err != nil || identity.GetMemoryCommit() != commit || identity.GetBinaryDigest() != digest || !identity.GetMirrorComplete() || identity.GetMirrorSequence() != sequence {
		reject()
		return
	}
	// Swaps and closing hold m.swap, so only failures can change m.active now.
	m.mu.Lock()
	superseded := m.closed || m.generation != generation || m.active != nil
	m.mu.Unlock()
	if superseded {
		reject()
		return
	}
	if err := m.resumeTraffic(ctx, h); err != nil {
		reject()
		return
	}
	m.mu.Lock()
	m.active = h
	m.revision = commit
	m.generation++
	m.mu.Unlock()
}

// Diagnose and Mitigate lease the same accepted child as detector evaluation.
// Transport failure is an error, never clearance or an empty authorized plan.
func (m *Manager) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	h := m.pin()
	if h != nil {
		defer m.unpin(h)
	}
	host, ok := h.(interface {
		Diagnose(context.Context, *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error)
	})
	if !ok {
		return nil, errors.New("memory host diagnosis unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return host.Diagnose(ctx, r)
}
func (m *Manager) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	h := m.pin()
	if h != nil {
		defer m.unpin(h)
	}
	host, ok := h.(interface {
		Mitigate(context.Context, *pb.MitigateRequest) (*pb.MitigateResponse, error)
	})
	if !ok {
		return nil, errors.New("memory host mitigation unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return host.Mitigate(ctx, r)
}

// ReadState leases the accepted child like Diagnose. A transport failure is an
// error; the caller records it as a failed read, never as state.
func (m *Manager) ReadState(ctx context.Context, r *pb.ReadStateRequest) (*pb.ReadStateResponse, error) {
	h := m.pin()
	if h == nil {
		return nil, errors.New("memory host is unavailable")
	}
	defer m.unpin(h)
	host, ok := h.(interface {
		ReadState(context.Context, *pb.ReadStateRequest) (*pb.ReadStateResponse, error)
	})
	if !ok {
		return nil, errors.New("memory host state reads unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return host.ReadState(ctx, r)
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
