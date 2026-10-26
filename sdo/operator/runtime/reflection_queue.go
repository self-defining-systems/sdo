package runtime

import (
	"context"
	"errors"
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
	QueuedReflectionRolloutPending(string) (bool, error)
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
	lastErr  map[string]string
	// unknown means the last ledger rescan failed, so queued work may exist.
	unknown bool
	// rollout names an incident whose queued reflection accepted a detector
	// change: the controller must restart into it before the work closes.
	rollout string
}

// driveQueuedReflections runs at most one queued reflection at a time, oldest
// first. It rescans the ledger only at start, after an acknowledgment and after
// a reflection finishes. Exhausted retries record the reflection as abandoned
// so a deterministic failure cannot hold open work forever. A reflection that
// accepted a detector change returns ErrControllerRolloutRestartRequired, the
// same supervisor path a closure acknowledgment takes.
func (c *Controller) driveQueuedReflections(ctx context.Context, now time.Time) error {
	queue, ok := c.broker.(ReflectionQueue)
	if !ok {
		return nil
	}
	c.mu.Lock()
	state := &c.reflections
	if state.rollout != "" {
		c.mu.Unlock()
		return ErrControllerRolloutRestartRequired
	}
	if state.running != "" || (!state.rescan && len(state.queued) == 0) || now.Before(state.retryAt) {
		c.mu.Unlock()
		return nil
	}
	rescan := state.rescan
	c.mu.Unlock()
	if rescan {
		ids, err := queue.QueuedReflections()
		c.mu.Lock()
		if err != nil {
			// Keep the previous list and export the queue as unknown, so
			// admission never reads a failed rescan as idle.
			state.unknown = true
			state.retryAt = now.Add(c.config.ClosureRetry.InitialBackoff)
			c.mu.Unlock()
			c.reportBrokerError("list queued reflections", err)
			return nil
		}
		state.queued, state.rescan, state.unknown = ids, false, false
		c.mu.Unlock()
	}
	c.mu.Lock()
	if len(state.queued) == 0 {
		c.mu.Unlock()
		return nil
	}
	id := state.queued[0]
	if state.attempts == nil {
		state.attempts = map[string]int{}
	}
	attempts, lastErr := state.attempts[id], state.lastErr[id]
	limit := max(1, c.config.ClosureRetry.MaxAttempts)
	c.mu.Unlock()
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	state.running = id
	c.mu.Unlock()
	c.goBackground(func() {
		defer cancel()
		var err error
		if attempts >= limit {
			err = queue.AbandonQueuedReflection(actionCtx, id, fmt.Sprintf("queued reflection failed %d times: %s", attempts, lastErr))
		} else {
			err = queue.ProcessQueuedReflection(actionCtx, id)
		}
		c.reflectionResults <- reflectionCompletion{incidentID: id, err: err}
	})
	return nil
}

func (c *Controller) handleReflectionCompletion(completion reflectionCompletion) {
	c.mu.Lock()
	state := &c.reflections
	state.running, state.rescan = "", true
	if completion.err == nil {
		delete(state.attempts, completion.incidentID)
		delete(state.lastErr, completion.incidentID)
		state.retryAt = time.Time{}
		c.mu.Unlock()
		// An accepted detector change keeps the incident queued until a
		// restarted controller records its rollout.
		pending, err := c.broker.(ReflectionQueue).QueuedReflectionRolloutPending(completion.incidentID)
		c.mu.Lock()
		switch {
		case err != nil:
			state.unknown = true
		case pending:
			state.rollout = completion.incidentID
		default:
			state.queued = slices.DeleteFunc(state.queued, func(id string) bool { return id == completion.incidentID })
		}
		c.mu.Unlock()
		if err != nil {
			c.reportBrokerError("check queued reflection rollout "+completion.incidentID, err)
		}
		return
	}
	if state.lastErr == nil {
		state.lastErr = map[string]string{}
	}
	state.attempts[completion.incidentID]++
	state.lastErr[completion.incidentID] = completion.err.Error()
	// A typed terminal worker failure is never retried: the next pass records
	// the reflection as abandoned with this evidence.
	var terminal interface{ NonRetryable() bool }
	if errors.As(completion.err, &terminal) && terminal.NonRetryable() {
		state.attempts[completion.incidentID] = max(1, c.config.ClosureRetry.MaxAttempts)
		state.retryAt = time.Time{}
		c.mu.Unlock()
		c.reportBrokerError("process queued reflection "+completion.incidentID, completion.err)
		return
	}
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
	return c.reflections.running != "" || len(c.reflections.queued) > 0 || c.reflections.unknown
}
