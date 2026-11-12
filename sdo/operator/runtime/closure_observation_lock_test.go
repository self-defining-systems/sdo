package runtime

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/sdk"
)

// parkedProofBroker is a broker whose committed-closure lookup waits on an
// external party, as the native broker's does while another incident's
// reflection owns the effect lock.
type parkedProofBroker struct {
	*riskBroker
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *parkedProofBroker) committedClosureReceipt(ctx context.Context, _ IncidentClosure, _ *ClosureReceipt) (ClosureReceipt, bool, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return ClosureReceipt{}, false, ctx.Err()
	}
	return ClosureReceipt{}, false, nil
}

// A closure effect that waits on the broker (behind another incident's
// reflection) must never hold the observation lock: detectors keep running
// while the closure waits.
func TestClosureEffectBrokerWaitDoesNotHoldObservationLock(t *testing.T) {
	ctx := context.Background()
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	dispatcher := &blockingDispatcher{stopAfter: opened}
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, newRestartedStore(opened), dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	var clockMu sync.Mutex
	now := opened
	controller.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	dispatcher.clock = controller.clock
	controller.gitRead = fakeArchitecture("")
	parked := &parkedProofBroker{riskBroker: &riskBroker{}, entered: make(chan struct{}), release: make(chan struct{})}
	controller.broker = parked
	healedAt := opened.Add(20 * time.Second)
	for step := 0; step < 1200; step++ {
		clockMu.Lock()
		at := now
		clockMu.Unlock()
		if !at.Before(healedAt) {
			world.healthy.Store(true)
		}
		if err := controller.Step(ctx, at, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := controller.PendingWorkspaceEffect(); ok {
			if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-controller.workspaceResults
			controller.workspaceResults <- completion
		} else if effect, ok := controller.PendingDispatchEffect(); ok {
			if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			for !dispatcher.hasStarted() {
				runtime.Gosched()
			}
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			timeout, stop := context.WithTimeout(ctx, 10*time.Second)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- controller.ExecuteClosureEffect(ctx, effect) }()
			select {
			case <-parked.entered:
			case err := <-done:
				t.Fatalf("closure effect finished without consulting the broker: %v", err)
			case <-timeout.Done():
				t.Fatal("closure effect never reached the broker")
			}
			if !controller.observationMu.TryLock() {
				close(parked.release)
				<-done
				t.Fatal("closure effect holds the observation lock while it waits on the broker")
			}
			controller.observationMu.Unlock()
			close(parked.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
			return
		}
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
	}
	t.Fatal("incident never reached closure")
}

// Reading a committed closure receipt is a lock-free read of the atomically
// written ledger: another incident's reflection owning the broker lock must
// not make it wait.
func TestCommittedClosureReceiptDoesNotWaitForBrokerLock(t *testing.T) {
	root := t.TempDir()
	native := NativeIncidentBroker{Repository: &broker.Broker{}, LedgerRoot: root}
	file, err := os.OpenFile(filepath.Join(root, "broker.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	timeout, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, _, err := native.committedClosureReceipt(context.Background(), IncidentClosure{}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-timeout.Done():
		t.Fatal("committedClosureReceipt waits for the broker lock held by another worker")
	}
}
