package memoryhost

import (
	"context"
	"errors"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type mirrorHost interface {
	Host
	MirrorFrame(context.Context, *pb.MirrorRequest) error
}
type mirrorState struct {
	mu        sync.Mutex
	namespace string
	objects   map[string]*pb.MemoryObject
	hosts     map[Host]uint64
	order     []mirrorHost
}

// Publish fans a frozen informer projection out as contiguous complete deltas.
// Capture returns the sequence its view was published at; hosts keep recent
// sequences readable, so an evaluation always runs against exactly its own
// frozen view while the mirror moves on, and no lock outlives Capture.
func (m *Manager) Publish(ctx context.Context, view sdk.DetectionContext) error {
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	return m.publish(ctx, view)
}
func (m *Manager) publish(ctx context.Context, view sdk.DetectionContext) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	objects, err := mirrorObjects(view)
	if err != nil {
		return err
	}
	if m.mirror.namespace != "" && view.Namespace() != m.mirror.namespace {
		return errors.New("mirror namespace changed")
	}
	frame := &pb.MirrorRequest{Namespace: view.Namespace(), Complete: true}
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := objects[key]
		if !proto.Equal(m.mirror.objects[key], item) {
			frame.Upserts = append(frame.Upserts, item)
		}
	}
	keys = keys[:0]
	for key := range m.mirror.objects {
		if objects[key] == nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		frame.Deletes = append(frame.Deletes, m.mirror.objects[key].Ref)
	}
	if len(frame.Upserts) == 0 && len(frame.Deletes) == 0 && m.mirror.namespace != "" {
		return nil
	}
	// A candidate failure removes only that candidate. An active failure leaves
	// the call unavailable; no detector may interpret it as an empty result.
	var activeErr error
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()
	order := m.mirror.order[:0]
	for _, host := range m.mirror.order {
		sequence, present := m.mirror.hosts[host]
		if !present {
			continue
		}
		order = append(order, host)
		delta := proto.Clone(frame).(*pb.MirrorRequest)
		delta.Sequence = sequence + 1
		if err := host.MirrorFrame(ctx, delta); err != nil {
			delete(m.mirror.hosts, host)
			if host == active {
				go m.failed(host)
				activeErr = err
			} else {
				_ = host.Close()
			}
		} else {
			m.mirror.hosts[host] = delta.Sequence
		}
	}
	m.mirror.order = order
	m.mirror.namespace = view.Namespace()
	m.mirror.objects = objects
	return activeErr
}

// Prepare enrolls a candidate before activation; future captures update both
// active and candidate hosts. Compilation happens outside this barrier.
func (m *Manager) Prepare(ctx context.Context, h Host) (err error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	defer func() {
		if err != nil && h != nil {
			_ = h.Close()
		}
	}()
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return errors.New("memory host manager is closed")
	}
	host, ok := h.(mirrorHost)
	if !ok {
		return errors.New("host lacks mirror transport")
	}
	if m.mirror.namespace == "" {
		return errors.New("initial mirror not published")
	}
	if _, exists := m.mirror.hosts[h]; exists {
		return errors.New("candidate already prepared")
	}
	frame := &pb.MirrorRequest{Sequence: 1, ResetMirror: true, Complete: true, Namespace: m.mirror.namespace}
	keys := make([]string, 0, len(m.mirror.objects))
	for key := range m.mirror.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		frame.Upserts = append(frame.Upserts, m.mirror.objects[key])
	}
	if err = host.MirrorFrame(ctx, frame); err != nil {
		return err
	}
	if m.mirror.hosts == nil {
		m.mirror.hosts = map[Host]uint64{}
	}
	m.mirror.hosts[h] = 1
	order := m.mirror.order[:0]
	for _, host := range m.mirror.order {
		if _, ok := m.mirror.hosts[host]; ok {
			order = append(order, host)
		}
	}
	m.mirror.order = append(order, host)
	return nil
}
func (m *Manager) Activate(ctx context.Context, h Host, commit, digest string) error {
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	m.mu.Lock()
	already := m.active == h
	old := m.active
	m.mu.Unlock()
	if already {
		return errors.New("host is already active")
	}
	sequence, ok := m.mirror.hosts[h]
	if !ok {
		_ = h.Close()
		return errors.New("candidate mirror unavailable")
	}
	if err := m.Swap(ctx, h, commit, digest, sequence); err != nil {
		delete(m.mirror.hosts, h)
		return err
	}
	if old != h {
		delete(m.mirror.hosts, old)
	}
	return nil
}

// Capture leases the active host at the sequence the view was published at. The
// barriers it takes (the mirror and the manager state) end before it returns:
// the lease only pins the host against retirement, so other captures, publishes,
// failures and recoveries never wait for the call made under it.
func (m *Manager) Capture(ctx context.Context, view sdk.DetectionContext) (Host, uint64, func(), error) {
	m.mirror.mu.Lock()
	if err := m.publish(ctx, view); err != nil {
		m.mirror.mu.Unlock()
		return nil, 0, nil, err
	}
	m.mu.Lock()
	h := m.active
	sequence, ok := m.mirror.hosts[h]
	if h == nil || !ok {
		m.mu.Unlock()
		m.mirror.mu.Unlock()
		go m.retryRecovery()
		return nil, 0, nil, errors.New("memory host unavailable")
	}
	m.pinLocked(h)
	revision := m.revision
	m.mu.Unlock()
	m.mirror.mu.Unlock()
	captured := &capturedHost{Host: h, manager: m, revision: revision}
	var once sync.Once
	return captured, sequence, func() {
		once.Do(func() {
			m.unpin(h)
			if captured.transportFailed() {
				m.mirror.mu.Lock()
				delete(m.mirror.hosts, h)
				m.mirror.mu.Unlock()
				m.failed(h)
			}
		})
	}, nil
}

// capturedHost records transport failures while its lease is held. Eviction
// happens on release, after all read locks have been returned.
type capturedHost struct {
	Host
	failureMu sync.Mutex
	manager   *Manager
	failure   error
	revision  string
}

func (h *capturedHost) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	caller := ctx
	ctx, cancel := context.WithTimeout(ctx, h.manager.timeout)
	defer cancel()
	response, err := h.Host.Evaluate(ctx, r)
	h.record(caller, err)
	return response, err
}

// record keeps a transport failure for eviction on release. A cancelled or expired
// caller bounds its own call, not the lifetime of the host that was serving it.
func (h *capturedHost) record(caller context.Context, err error) {
	if err != nil && caller.Err() != nil {
		return
	}
	if err != nil {
		h.failureMu.Lock()
		defer h.failureMu.Unlock()
		h.failure = err
	}
}
func (h *capturedHost) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	caller := ctx
	ctx, cancel := context.WithTimeout(ctx, h.manager.timeout)
	defer cancel()
	host, ok := h.Host.(interface {
		Diagnose(context.Context, *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error)
	})
	if !ok {
		return nil, errors.New("host lacks diagnosis")
	}
	response, err := host.Diagnose(ctx, r)
	h.record(caller, err)
	return response, err
}
func (h *capturedHost) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	caller := ctx
	ctx, cancel := context.WithTimeout(ctx, h.manager.timeout)
	defer cancel()
	host, ok := h.Host.(interface {
		Mitigate(context.Context, *pb.MitigateRequest) (*pb.MitigateResponse, error)
	})
	if !ok {
		return nil, errors.New("host lacks mitigation")
	}
	response, err := host.Mitigate(ctx, r)
	h.record(caller, err)
	return response, err
}

// Identity verifies the live activation at the current complete mirror barrier.
func (m *Manager) Identity(ctx context.Context) (*pb.ReadyResponse, error) {
	m.mirror.mu.Lock()
	defer m.mirror.mu.Unlock()
	m.mu.Lock()
	h := m.active
	sequence, ok := m.mirror.hosts[h]
	if h != nil && ok {
		m.pinLocked(h)
	}
	m.mu.Unlock()
	if h == nil || !ok {
		go m.retryRecovery()
		return nil, errors.New("memory host unavailable")
	}
	defer m.unpin(h)
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	identity, err := h.Ready(ctx)
	if err != nil {
		go m.failed(h)
		return nil, err
	}
	if !identity.GetMirrorComplete() || identity.GetMirrorSequence() != sequence {
		return nil, errors.New("active host mirror is incomplete")
	}
	return identity, nil
}

// MemoryCommit binds adapters and revision-scoped trust to the leased child.
func (h *capturedHost) MemoryCommit() string { return h.revision }

func (h *capturedHost) transportFailed() bool {
	h.failureMu.Lock()
	defer h.failureMu.Unlock()
	return h.failure != nil
}
