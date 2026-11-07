//go:build integration

package rehearsal

import (
	"context"
	"sync"
	"time"

	"sdo.dev/operator/internal/rehearsal/rehearsalio"
)

// Clock is the injected clock of a rehearsal: it moves only when a waiter
// waits, so a ten-minute rollout deadline costs one step per poll, not ten
// minutes.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts at start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now is the current injected time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
}

// World is the cluster stand-in every SDO wait goes through: each Wait
// advances the clock and runs one kubelet step, so the cluster progresses
// exactly as far as SDO waited.
type World struct {
	Clock   *Clock
	Kubelet *Kubelet
	// Kubeconfig is the envtest API server's kubeconfig for kubectl.
	Kubeconfig string

	statsMu sync.Mutex
	// Stats is the harness's own cost: kubectl processes and kubelet steps.
	Stats Stats
}

// Stats counts the harness's host work.
type Stats struct {
	KubectlCalls int           `json:"kubectl_calls"`
	KubectlTime  time.Duration `json:"kubectl_time"`
	KubeletSteps int           `json:"kubelet_steps"`
	KubeletTime  time.Duration `json:"kubelet_time"`
}

func (w *World) count(kubectl bool, d time.Duration) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	if kubectl {
		w.Stats.KubectlCalls++
		w.Stats.KubectlTime += d
	} else {
		w.Stats.KubeletSteps++
		w.Stats.KubeletTime += d
	}
}

// Wait implements every injected wait (deployer poll, Job wait, advance
// re-evaluation).
func (w *World) Wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Clock.Advance(d)
	elapsed := rehearsalio.Stopwatch()
	w.Kubelet.Step(ctx)
	w.count(false, elapsed())
	return ctx.Err()
}

// Settle runs the kubelet for d of injected time in steps of step.
func (w *World) Settle(ctx context.Context, d, step time.Duration) {
	for waited := time.Duration(0); waited < d; waited += step {
		_ = w.Wait(ctx, step)
	}
}

// Kubectl is the deployer's command edge pointed at the envtest API server:
// the same binary and arguments as production, with only KUBECONFIG set.
func (w *World) Kubectl(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
	elapsed := rehearsalio.Stopwatch()
	defer func() { w.count(true, elapsed()) }()
	return rehearsalio.Run(ctx, w.Kubeconfig, name, args, input)
}
