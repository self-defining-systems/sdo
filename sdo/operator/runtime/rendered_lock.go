package runtime

import (
	"context"
	"errors"
	"sync"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
)

// RenderedLockReconciler publishes the render of accepted source as the
// controller-owned lock. The lock records the manifests exactly as source
// declares them; no diff, reconcile or closure reads it, so a missing or stale
// lock never blocks them.
type RenderedLockReconciler struct {
	Source GitPlanSource
	Client kubernetes.Interface
	Broker *broker.Broker
	// Published remembers the revisions already rendered; nil renders every call.
	Published *PublishedLocks
}

// PublishedLocks is the set of accepted revisions whose lock is current.
type PublishedLocks struct {
	mu   sync.Mutex
	done map[string]bool
}

func (p *PublishedLocks) has(revision string) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[revision]
}

func (p *PublishedLocks) add(revision string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done == nil {
		p.done = map[string]bool{}
	}
	p.done[revision] = true
}

func (r RenderedLockReconciler) Reconcile(ctx context.Context, revision, namespace string) (string, error) {
	if r.Broker == nil {
		return revision, errors.New("rendered lock requires the controller broker")
	}
	head, err := r.Broker.Head(ctx)
	if err != nil {
		return revision, err
	}
	revision = head
	if r.Published.has(revision) {
		return revision, nil
	}
	snapshot, err := r.Source.BuildLock(ctx, revision, namespace)
	if err != nil {
		return revision, err
	}
	commit, err := r.Broker.RecordRenderedLock(ctx, revision, snapshot)
	if err != nil {
		// Publication may have succeeded with a lost reply, or another broker
		// transaction advanced HEAD. Recover only the broker-verified head.
		head, headErr := r.Broker.Head(ctx)
		if headErr != nil {
			return revision, errors.Join(err, headErr)
		}
		if head != "" {
			revision = head
		}
		return revision, err
	}
	if commit == "" {
		return revision, errors.New("rendered lock publication returned an empty accepted revision")
	}
	r.Published.add(commit)
	return commit, nil
}
