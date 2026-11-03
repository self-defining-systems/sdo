package runtime

import (
	"context"
	"errors"
	"sync"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Only an explicit cycle identity permits reuse. Equal timestamps or equal
// object lists do not establish that two live observations are the same.
func memoryCycle(view sdk.DetectionContext) *CycleReadView {
	for {
		switch v := view.(type) {
		case *CycleReadView:
			return v
		case interface{ Unchecked() program.ReadView }:
			view = v.Unchecked()
		case *sdktest.RecordingReadView:
			view = v.ReadView
		case interface{ UnderlyingReadView() program.ReadView }:
			view = v.UnderlyingReadView()
		default:
			return nil
		}
	}
}

type memoryPreparation struct {
	mu           sync.Mutex
	refsOnce     sync.Once
	refs         []sdk.ObjectRef
	refsErr      error
	diffs        map[sdk.ObjectRef][]*pb.MemoryReadReceipt
	observations map[*RemoteMemory]*preparedObservation
}
type preparedObservation struct {
	once  sync.Once
	value *pb.MemoryObservation
	err   error
}

func snapshotMemoryRefs(view sdk.DetectionContext) ([]sdk.ObjectRef, error) {
	cycle := memoryCycle(view)
	if cycle == nil {
		return memoryhost.SnapshotRefs(view)
	}
	p := &cycle.memory
	p.refsOnce.Do(func() { p.refs, p.refsErr = memoryhost.SnapshotRefs(view) })
	return append([]sdk.ObjectRef(nil), p.refs...), p.refsErr
}
func frozenMemoryReads(view sdk.DetectionContext, refs []sdk.ObjectRef) ([]*pb.MemoryReadReceipt, error) {
	cycle := memoryCycle(view)
	if cycle == nil {
		return memoryhost.EncodeReceipts(memoryDiffInputs(view, refs))
	}
	p := &cycle.memory
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.diffs == nil {
		p.diffs = map[sdk.ObjectRef][]*pb.MemoryReadReceipt{}
	}
	var out []*pb.MemoryReadReceipt
	for _, ref := range refs {
		reads, ok := p.diffs[ref]
		if !ok {
			var err error
			reads, err = memoryhost.EncodeReceipts(memoryDiffInputs(view, []sdk.ObjectRef{ref}))
			if err != nil {
				return nil, err
			}
			p.diffs[ref] = reads
		}
		out = append(out, reads...)
	}
	return out, nil
}

type memoryScopeKey struct{}
type memoryLeaseKey struct {
	remote *RemoteMemory
	cycle  *CycleReadView
}
type memoryLease struct {
	mu       sync.Mutex
	ready    chan struct{}
	rpc      MemoryHostRPC
	sequence uint64
	release  func()
	err      error
	users    int
	closed   bool
}
type memoryScope struct {
	mu     sync.Mutex
	closed bool
	leases map[memoryLeaseKey]*memoryLease
}

// A scope pins one host/sequence per fresh snapshot. Preparation is joined
// through a channel; no mutex spans capture or RPC I/O. Closing prevents new
// users and the last in-flight call releases a closed lease.
func withMemoryScope(ctx context.Context) (context.Context, func()) {
	scope := &memoryScope{leases: map[memoryLeaseKey]*memoryLease{}}
	return context.WithValue(ctx, memoryScopeKey{}, scope), func() {
		scope.mu.Lock()
		scope.closed = true
		leases := make([]*memoryLease, 0, len(scope.leases))
		for _, lease := range scope.leases {
			leases = append(leases, lease)
		}
		scope.mu.Unlock()
		for _, lease := range leases {
			lease.done(true)
		}
	}
}
func (l *memoryLease) done(closeScope bool) {
	l.mu.Lock()
	if closeScope {
		l.closed = true
	} else {
		l.users--
	}
	var release func()
	if l.closed && l.users == 0 {
		release = l.release
		l.release = nil
	}
	l.mu.Unlock()
	if release != nil {
		release()
	}
}
func (r *RemoteMemory) scopedCapture(ctx context.Context, view sdk.DetectionContext) (MemoryHostRPC, uint64, func(), error) {
	scope, _ := ctx.Value(memoryScopeKey{}).(*memoryScope)
	cycle := memoryCycle(view)
	if scope == nil || cycle == nil {
		return r.capture(ctx, view)
	}
	scope.mu.Lock()
	if scope.closed {
		scope.mu.Unlock()
		return nil, 0, nil, errors.New("memory snapshot scope is closed")
	}
	key := memoryLeaseKey{r, cycle}
	lease := scope.leases[key]
	initialize := lease == nil
	if initialize {
		lease = &memoryLease{ready: make(chan struct{}), users: 1}
		scope.leases[key] = lease
	}
	scope.mu.Unlock()
	if initialize {
		rpc, seq, release, err := r.capture(ctx, view)
		lease.mu.Lock()
		lease.rpc, lease.sequence, lease.release, lease.err = rpc, seq, release, err
		close(lease.ready)
		lease.mu.Unlock()
		lease.done(false)
	}
	select {
	case <-lease.ready:
	case <-ctx.Done():
		return nil, 0, nil, ctx.Err()
	}
	lease.mu.Lock()
	if lease.closed {
		lease.mu.Unlock()
		return nil, 0, nil, errors.New("memory snapshot lease is closed")
	}
	if lease.err != nil {
		lease.mu.Unlock()
		return nil, 0, nil, lease.err
	}
	lease.users++
	rpc, seq := lease.rpc, lease.sequence
	lease.mu.Unlock()
	var once sync.Once
	return rpc, seq, func() { once.Do(func() { lease.done(false) }) }, nil
}
