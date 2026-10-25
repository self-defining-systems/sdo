package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

// The setup caller's cancellation boundary is driven by virtual elapsed time.
// It uses no wall timer and never waits 360 real seconds for a slow worker.
type simSetupBudget struct {
	context.Context
	clock   *simClock
	expires time.Time
	done    chan struct{}
	expired atomic.Bool
}

func (b *simSetupBudget) Done() <-chan struct{} { return b.done }
func (b *simSetupBudget) Err() error {
	if b.expired.Load() {
		return context.DeadlineExceeded
	}
	return nil
}
func (b *simSetupBudget) spend(d time.Duration) {
	b.clock.now = b.clock.now.Add(d)
	if !b.clock.now.Before(b.expires) {
		b.clock.now = b.expires
		if b.expired.CompareAndSwap(false, true) {
			close(b.done)
		}
	}
}

type simSlowRefreshWorker struct {
	base   *tracingRefreshWorker
	budget *simSetupBudget
	delays []time.Duration
	tasks  []*contracts.AgentTask
}

func (w *simSlowRefreshWorker) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	request := new(contracts.HealthJudgeRequest)
	if err := task.Request.UnmarshalTo(request); err != nil {
		return nil, err
	}
	index := len(w.tasks)
	if index >= len(w.delays) {
		return nil, fmt.Errorf("unexpected setup worker attempt %d", index+1)
	}
	if request.GetValidationAttempt() != uint32(index+1) || (index > 0 && len(request.GetPreviousDiagnostics()) == 0) {
		return nil, errors.New("slow setup correction lost attributable validation feedback")
	}
	w.tasks = append(w.tasks, task)
	w.budget.spend(w.delays[index])
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := w.base.Execute(ctx, task)
	if err != nil {
		return nil, err
	}
	if index == 0 {
		// A real syntax error forces broker rejection and the production correction
		// loop. No preselected success or synthetic validator verdict is supplied.
		path := filepath.Join(task.Workspace.CheckoutPath, ".sdo/diagnostics/detectors/health/health-objective/detector.go")
		if err := os.WriteFile(path, []byte("package objective\nfunc malformed("), 0644); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func runSimSlowSetupRefresh(t *testing.T, seed uint64) {
	t.Helper()
	exceed := seed%2 == 0
	t.Run(fmt.Sprintf("exceeds-cap=%t", exceed), func(t *testing.T) {
		root, proposals, _, worker, coordinator := tracingFixture(t)
		tracingSwap(t, root)
		before, err := memory.AcceptedHealthRevision(context.Background(), root)
		if err != nil || !before.NeedsRefresh {
			t.Fatalf("stale seed fixture: %+v %v", before, err)
		}
		repository, err := broker.New(root, proposals, simMemoryStructureValidator)
		if err != nil {
			t.Fatal(err)
		}
		clock := newSimClock()
		setupCap := 360 * time.Second
		budget := &simSetupBudget{Context: context.Background(), clock: clock, expires: clock.Now().Add(setupCap), done: make(chan struct{})}
		rng := newSimRNG(seed)
		delays := []time.Duration{time.Duration(150+rng.IntN(20)) * time.Second, time.Duration(150+rng.IntN(20)) * time.Second}
		if exceed {
			delays[0] += 90 * time.Second
		}
		slow := &simSlowRefreshWorker{base: worker, budget: budget, delays: delays}
		coordinator.Broker = repository
		coordinator.Workers = slow
		_, err = coordinator.RefreshHealth(budget, lifecycle.Config{Application: "tracing", Namespace: "demo"}, before)
		if len(slow.tasks) != 2 || slow.tasks[0].TaskId == slow.tasks[1].TaskId {
			t.Fatalf("slow refresh did not execute a distinct correction: tasks=%d err=%v", len(slow.tasks), err)
		}
		after, readErr := memory.AcceptedHealthRevision(context.Background(), root)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if exceed {
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "refresh health requirements") || !after.NeedsRefresh || after.Commit != before.Commit || !clock.Now().Equal(budget.expires) {
				t.Fatalf("slow setup failed silently or adopted stale health: revision=%+v err=%v", after, err)
			}
			// Cancellation retains exact retry/proposal identity for reattachment;
			// it must not erase the cause or silently launch another judge attempt.
			proposal, lookupErr := repository.ProposalInfo(slow.tasks[1].Workspace.ProposalId, broker.InitialPhase)
			if lookupErr != nil || proposal.Path != slow.tasks[1].Workspace.CheckoutPath {
				t.Fatalf("timed-out setup lost attributable proposal/task: %v", lookupErr)
			}
		} else if err != nil || after.NeedsRefresh || after.Commit == before.Commit || !clock.Now().Before(budget.expires) {
			t.Fatalf("slow bounded setup did not complete: revision=%+v err=%v", after, err)
		}
	})
}
