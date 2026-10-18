package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var ErrLeadershipInactive = errors.New("controller leadership is not active")

type LeaseElector struct {
	client        kubernetes.Interface
	namespace     string
	name          string
	identity      string
	leaseDuration time.Duration
	now           func() time.Time
	leader        atomic.Bool
	validUntil    atomic.Int64
	generation    atomic.Uint64
	mu            sync.Mutex
	session       context.Context
	cancelSession context.CancelFunc
	expiryTimer   *time.Timer
}

func NewLeaseElector(
	client kubernetes.Interface,
	namespace string,
	name string,
	identity string,
	leaseDuration time.Duration,
) *LeaseElector {
	return &LeaseElector{
		client: client, namespace: namespace, name: name, identity: identity,
		leaseDuration: leaseDuration, now: time.Now,
	}
}

func (e *LeaseElector) TryAcquireOrRenew(ctx context.Context) (leader bool, err error) {
	validUntil := time.Time{}
	defer func() {
		if leader && err == nil {
			e.activate(validUntil)
			return
		}
		// A transport timeout does not prove the Lease was lost. Keep the
		// current guarded session alive until its already-recorded expiry so a
		// renewal loop can retry without cancelling in-flight durable effects.
		if err == nil {
			e.deactivate()
		}
	}()
	if e.client == nil || e.namespace == "" || e.name == "" || e.identity == "" || e.leaseDuration <= 0 {
		return false, fmt.Errorf("valid Lease client, namespace, name, identity, and duration are required")
	}
	leases := e.client.CoordinationV1().Leases(e.namespace)
	now := metav1.NewMicroTime(e.now().UTC())
	durationSeconds := int32(e.leaseDuration / time.Second)
	if durationSeconds < 1 {
		durationSeconds = 1
	}
	lease, err := leases.Get(ctx, e.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, createErr := leases.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: e.name, Namespace: e.namespace},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &e.identity, LeaseDurationSeconds: &durationSeconds,
				AcquireTime: &now, RenewTime: &now,
			},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(createErr) {
			return false, nil
		}
		if createErr != nil {
			return false, fmt.Errorf("create leader Lease: %w", createErr)
		}
		validUntil = now.Add(time.Duration(durationSeconds) * time.Second)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get leader Lease: %w", err)
	}
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	expired := leaseExpired(lease, e.now())
	if holder != e.identity && holder != "" && !expired {
		return false, nil
	}
	updated := lease.DeepCopy()
	updated.Spec.HolderIdentity = &e.identity
	updated.Spec.LeaseDurationSeconds = &durationSeconds
	updated.Spec.RenewTime = &now
	if holder != e.identity {
		updated.Spec.AcquireTime = &now
	}
	if _, err := leases.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("renew leader Lease: %w", err)
	}
	validUntil = now.Add(time.Duration(durationSeconds) * time.Second)
	return true, nil
}

func (e *LeaseElector) Release(ctx context.Context) error {
	defer e.deactivate()
	leases := e.client.CoordinationV1().Leases(e.namespace)
	lease, err := leases.Get(ctx, e.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get leader Lease for release: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != e.identity {
		return nil
	}
	empty := ""
	lease.Spec.HolderIdentity = &empty
	if _, err := leases.Update(ctx, lease, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
		return fmt.Errorf("release leader Lease: %w", err)
	}
	return nil
}

func (e *LeaseElector) IsLeader() bool {
	if !e.leader.Load() {
		return false
	}
	generation := e.generation.Load()
	validUntil := time.Unix(0, e.validUntil.Load())
	if !e.now().Before(validUntil) {
		e.deactivateGeneration(generation)
		return false
	}
	return e.leader.Load() && e.generation.Load() == generation
}

func (e *LeaseElector) GuardContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	if !e.IsLeader() {
		return nil, nil, ErrLeadershipInactive
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.leader.Load() || e.session == nil || e.session.Err() != nil {
		return nil, nil, ErrLeadershipInactive
	}
	guarded, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(e.session, cancel)
	return guarded, func() {
		stop()
		cancel()
	}, nil
}

func (e *LeaseElector) activate(validUntil time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.leader.Load() || e.session == nil || e.session.Err() != nil {
		e.session, e.cancelSession = context.WithCancel(context.Background())
	}
	e.leader.Store(true)
	e.validUntil.Store(validUntil.UnixNano())
	generation := e.generation.Add(1)
	if e.expiryTimer != nil {
		e.expiryTimer.Stop()
	}
	delay := validUntil.Sub(e.now())
	if delay < 0 {
		delay = 0
	}
	e.expiryTimer = time.AfterFunc(delay, func() { e.expire(generation) })
}

func (e *LeaseElector) expire(generation uint64) {
	e.deactivateGeneration(generation)
}

func (e *LeaseElector) deactivate() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.deactivateLocked()
}

func (e *LeaseElector) deactivateGeneration(generation uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.generation.Load() != generation {
		return
	}
	e.deactivateLocked()
}

func (e *LeaseElector) deactivateLocked() {
	e.generation.Add(1)
	e.leader.Store(false)
	e.validUntil.Store(0)
	if e.expiryTimer != nil {
		e.expiryTimer.Stop()
		e.expiryTimer = nil
	}
	if e.cancelSession != nil {
		e.cancelSession()
	}
	e.session = nil
	e.cancelSession = nil
}

func leaseExpired(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil {
		return true
	}
	expires := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return !now.Before(expires)
}
