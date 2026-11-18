package runtime

import (
	"context"
	"syscall"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

// The controller process must treat SIGTERM as a request to stop, so its
// deferred Lease release runs; with no handler the process dies with the Lease
// still naming it and the replacement waits the Lease out.
func TestSIGTERMEndsTheControllerContext(t *testing.T) {
	ctx, stop := terminationContext(context.Background())
	defer stop()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM did not end the controller context")
	}
}

// A released Lease is taken at once by the next controller; one that is only
// abandoned makes it wait out the whole duration.
func TestReleasedLeaseIsAcquiredAtOnce(t *testing.T) {
	client := fake.NewSimpleClientset()
	old := NewLeaseElector(client, "ns", "lease", "old", 60*time.Second)
	if leader, err := old.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("old acquire: %v %v", leader, err)
	}
	next := NewLeaseElector(client, "ns", "lease", "next", 60*time.Second)
	if leader, _ := next.TryAcquireOrRenew(context.Background()); leader {
		t.Fatal("a second controller led while the first held the Lease")
	}
	if err := old.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old.IsLeader() {
		t.Fatal("the released controller still acts as leader")
	}
	if leader, err := next.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("next acquire after release: %v %v", leader, err)
	}
}
