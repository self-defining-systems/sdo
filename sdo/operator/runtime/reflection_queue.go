package runtime

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// ReflectionQueue is implemented by brokers that queue a failed attempt's
// reflection behind its closure acknowledgment. The broker ledger is durable,
// so a restarted controller replays every queued reflection in FIFO order.
type ReflectionQueue interface {
	QueuedReflections() ([]string, error)
	ProcessQueuedReflection(context.Context, string) error
	AbandonQueuedReflection(context.Context, string, string) error
}

type reflectionCompletion struct {
	incidentID string
	err        error
}

type reflectionQueueState struct {
	queued   []string
	running  string
	rescan   bool
	retryAt  time.Time
	attempts map[string]int
}

// driveQueuedReflections runs at most one queued reflection at a time, oldest
// first. It rescans the ledger only at start, after an acknowledgment and after
// a reflection finishes. Exhausted retries record the reflection as abandoned
// so a deterministic failure cannot hold open work forever.
func (c *Controller) driveQueuedReflections(ctx context.Context, now time.Time) {
	queue, ok := c.broker.(ReflectionQueue)
	if !ok {
		return
	}
	c.mu.Lock()
	state := &c.reflections
	if state.running != "" || (!state.rescan && len(state.queued) == 0) || now.Before(state.retryAt) {
		c.mu.Unlock()
		return
	}
	rescan := state.rescan
	c.mu.Unlock()
	if rescan {
		ids, err := queue.QueuedReflections()
		if err != nil {
			c.reportBrokerError("list queued reflections", err)
			return
		}
		c.mu.Lock()
		state.queued, state.rescan = ids, false
		c.mu.Unlock()
	}
	c.mu.Lock()
	if len(state.queued) == 0 {
		c.mu.Unlock()
		return
	}
	id := state.queued[0]
	if state.attempts == nil {
		state.attempts = map[string]int{}
	}
	attempts := state.attempts[id]
	limit := max(1, c.config.ClosureRetry.MaxAttempts)
	c.mu.Unlock()
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return
	}
	c.mu.Lock()
	state.running = id
	c.mu.Unlock()
	c.goBackground(func() {
		defer cancel()
		var err error
		if attempts >= limit {
			err = queue.AbandonQueuedReflection(actionCtx, id, fmt.Sprintf("queued reflection failed %d times", attempts))
		} else {
			err = queue.ProcessQueuedReflection(actionCtx, id)
		}
		c.reflectionResults <- reflectionCompletion{incidentID: id, err: err}
	})
}

func (c *Controller) handleReflectionCompletion(completion reflectionCompletion) {
	c.mu.Lock()
	state := &c.reflections
	state.running, state.rescan = "", true
	if completion.err == nil {
		delete(state.attempts, completion.incidentID)
		state.retryAt = time.Time{}
		state.queued = slices.DeleteFunc(state.queued, func(id string) bool { return id == completion.incidentID })
		c.mu.Unlock()
		return
	}
	state.attempts[completion.incidentID]++
	delay := c.config.ClosureRetry.InitialBackoff << min(state.attempts[completion.incidentID]-1, 16)
	if c.config.ClosureRetry.MaxBackoff > 0 {
		delay = min(delay, c.config.ClosureRetry.MaxBackoff)
	}
	state.retryAt = c.clock().Add(delay)
	c.mu.Unlock()
	c.reportBrokerError("process queued reflection "+completion.incidentID, completion.err)
}

// QueuedReflectionIncidentIDs returns queued or running reflections, oldest first.
func (c *Controller) QueuedReflectionIncidentIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.reflections.queued...)
}

func (c *Controller) reflectionWorkOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reflections.running != "" || len(c.reflections.queued) > 0
}
