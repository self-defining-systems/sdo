package runtime

import (
	"context"
	"testing"
)

type rolloutQueue struct {
	commit  string
	pending bool
}

func (q *rolloutQueue) QueuedReflections() ([]string, error)                          { return nil, nil }
func (q *rolloutQueue) ProcessQueuedReflection(context.Context, string) error         { return nil }
func (q *rolloutQueue) AbandonQueuedReflection(context.Context, string, string) error { return nil }
func (q *rolloutQueue) QueuedReflectionRolloutPending(string) (bool, error)           { return q.pending, nil }
func (q *rolloutQueue) QueuedReflectionServingCommit(context.Context, string) (string, error) {
	return q.commit, nil
}

// A reflection that commits after its closure must become the commit the host
// activates; otherwise the rollout handoff compares the repository HEAD (the
// reflection) with the outcome commit forever and the learned memory is never
// served.
func TestQueuedRolloutActivatesTheReflectionCommit(t *testing.T) {
	c := &Controller{}
	c.config.SourceCommit = "outcome"
	c.reflections.queued = []string{"inc"}
	c.reflections.rollout = "inc"
	queue := &rolloutQueue{commit: "reflection", pending: true}
	var activated []string
	c.memoryRefresh = func(context.Context) (bool, error) {
		activated = append(activated, c.CurrentAcceptedCommit())
		queue.pending = false // the handoff recorded the rollout
		return true, nil
	}
	if err := c.activateQueuedRollout(context.Background(), queue, "inc"); err != nil {
		t.Fatal(err)
	}
	if len(activated) != 1 || activated[0] != "reflection" {
		t.Fatalf("activated %v, want the reflection commit", activated)
	}
	if c.reflections.rollout != "" || len(c.reflections.queued) != 0 {
		t.Fatalf("rollout %q queued %v: the incident should leave the queue", c.reflections.rollout, c.reflections.queued)
	}
}
