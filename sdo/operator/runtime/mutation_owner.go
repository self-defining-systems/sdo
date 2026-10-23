package runtime

import (
	"context"
	"errors"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sync"
	"time"
)

// mutationOwner holds a single gateway across the whole backend call, including
// intent/completion persistence. Revocation blocks queued calls before waiting
// for the active one. An expired drain can never turn into write permission.
// It proves only calls routed through this owner, not an unknown old process.
type mutationOwner struct {
	mu      sync.Mutex
	gate    chan struct{}
	revoked bool
	journal *selectionJournal
}

func newMutationOwner(journal *selectionJournal) *mutationOwner {
	o := &mutationOwner{gate: make(chan struct{}, 1), journal: journal}
	o.gate <- struct{}{}
	return o
}
func (o *mutationOwner) run(ctx context.Context, epoch uint64, call func(context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.gate:
	}
	defer func() { o.gate <- struct{}{} }()
	o.mu.Lock()
	revoked := o.revoked
	o.mu.Unlock()
	if revoked {
		return errSelectionFenced
	}
	if err := o.journal.authorizeMutation(ctx, epoch); err != nil {
		return err
	}
	deadline := o.journal.snapshot().OriginalDeadline.AsTime()
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return call(bounded)
}
func (o *mutationOwner) revokeAndAdopt(ctx context.Context, source string) error {
	o.mu.Lock()
	if o.revoked {
		o.mu.Unlock()
		return errSelectionFenced
	}
	o.revoked = true
	o.mu.Unlock()
	select {
	case <-o.gate:
		defer func() { o.gate <- struct{}{} }()
		if err := ctx.Err(); err != nil {
			return o.freezeAfterFailedDrain(ctx, source, err)
		}
		if err := o.journal.adopt(ctx, source, true); err != nil {
			return err
		}
		o.mu.Lock()
		o.revoked = false
		o.mu.Unlock()
		return nil
	case <-ctx.Done():
		return o.freezeAfterFailedDrain(ctx, source, ctx.Err())
	}
}
func (o *mutationOwner) freezeAfterFailedDrain(ctx context.Context, source string, cause error) error {
	// Revocation must survive cancellation of the request which asked to drain.
	durable, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return errors.Join(cause, o.journal.adopt(durable, source, false))
}

// The owner holds the whole operation while this edge rechecks every outgoing
// API write. A multi-action apply or automatic rollback must not keep issuing
// calls after another process has adopted/frozen its epoch.
type selectionFencedPlanClient struct {
	PlanObjectClient
	journal *selectionJournal
	epoch   uint64
}

func (c selectionFencedPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.journal.authorizeMutation(ctx, c.epoch); err != nil {
		return err
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, patch)
}
