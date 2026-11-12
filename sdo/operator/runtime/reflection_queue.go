package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// QueuedReflectionServingCommit is the accepted commit that serves a
	// queued incident's reflection: the accepted HEAD once it committed (""
	// when its reflection committed nothing).
	QueuedReflectionServingCommit(context.Context, string) (string, error)
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
	// deferred holds incidents whose source publication could not start until
	// a time: they are skipped, so one cannot block another's reflection.
	deferred map[string]time.Time
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
// accepted a detector change activates the accepted memory in the memory host,
// the same path a closure acknowledgment takes; the controller never restarts.
func (c *Controller) driveQueuedReflections(ctx context.Context, now time.Time) error {
	queue, ok := c.broker.(ReflectionQueue)
	if !ok {
		return nil
	}
	c.mu.Lock()
	state := &c.reflections
	if state.rollout != "" {
		incident := state.rollout
		c.mu.Unlock()
		return c.activateQueuedRollout(ctx, queue, incident)
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
	id := ""
	for _, candidate := range state.queued {
		if until, waiting := state.deferred[candidate]; !waiting || !now.Before(until) {
			id = candidate
			break
		}
	}
	if id == "" {
		c.mu.Unlock()
		return nil
	}
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
		delete(state.deferred, completion.incidentID)
		state.retryAt = time.Time{}
		c.mu.Unlock()
		// An accepted detector change keeps the incident queued until a
		// restarted controller records its rollout.
		var pending bool
		queue, queued := c.broker.(ReflectionQueue)
		err := errors.New("broker cannot report queued reflection rollouts")
		if queued {
			pending, err = queue.QueuedReflectionRolloutPending(completion.incidentID)
		}
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
	var deferral interface{ Deferred() bool }
	if errors.As(completion.err, &deferral) && deferral.Deferred() {
		// Not a failed attempt: the deploy needs the controller idle. Try again
		// after the controller's initial retry interval, without blocking others.
		if state.deferred == nil {
			state.deferred = map[string]time.Time{}
		}
		interval := max(c.config.ClosureRetry.InitialBackoff, time.Second)
		state.deferred[completion.incidentID] = c.clock().Add(interval)
		state.rescan = true
		c.mu.Unlock()
		slog.Info("source_publication_deferred", "incident_id", completion.incidentID, "reason", completion.err.Error())
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

// activateQueuedRollout makes the host serve the accepted memory and records the
// rollout handoff, after which the queued incident is no longer open work.
func (c *Controller) activateQueuedRollout(ctx context.Context, queue ReflectionQueue, incident string) error {
	if c.memoryRefresh == nil {
		return nil
	}
	// A queued reflection commits after its closure set the accepted commit to
	// the outcome, and other publications may follow it; the host must serve
	// the accepted HEAD (what the rollout handoff verifies), or the learned
	// memory is never served and the rollout stays pending forever.
	commit, err := queue.QueuedReflectionServingCommit(ctx, incident)
	if err != nil {
		c.reportBrokerError("queued reflection commit "+incident, err)
		return nil
	}
	if commit != "" {
		c.mu.Lock()
		c.config.SourceCommit = commit
		c.mu.Unlock()
	}
	ready, err := c.memoryRefresh(ctx)
	if err != nil {
		c.reportBrokerError("memory activation", err)
		return nil
	}
	if !ready {
		return nil
	}
	pending, err := queue.QueuedReflectionRolloutPending(incident)
	c.mu.Lock()
	defer c.mu.Unlock()
	state := &c.reflections
	if err != nil {
		state.unknown = true
		return nil
	}
	if !pending {
		state.queued = slices.DeleteFunc(state.queued, func(id string) bool { return id == incident })
		state.rollout = ""
		state.rescan = true
	}
	return nil
}
