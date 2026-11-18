package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// A signal stop is not a success: the pod must end Failed so the Job's pod
// failure policy and backoff treat it like any other lost pod, and the
// installer does not read it as a finished bounded run. A bounded run
// (--duration, --exit-after-closure) ends by itself and exits 0.
func TestExitCodeSeparatesASignalStopFromACompletedRun(t *testing.T) {
	signalled, stop := context.WithCancel(context.Background())
	stop()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want int
	}{
		{"completed", context.Background(), nil, 0},
		{"signal stop, clean return", signalled, nil, ExitTerminated},
		{"signal stop, cancellation error", signalled, context.Canceled, ExitTerminated},
		{"failure", context.Background(), errors.New("boom"), 1},
		{"failure during a signal stop", signalled, errors.New("boom"), 1},
	} {
		if got := runExitCode(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: exit code = %d, want %d", tc.name, got, tc.want)
		}
	}
	if ExitTerminated != 143 {
		t.Fatalf("ExitTerminated = %d, want 128+SIGTERM", ExitTerminated)
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

// A renewal that lands between Release's read and write makes the write
// conflict; the Lease must still be released, not left naming this holder.
func TestReleaseRetriesAnOptimisticLockConflict(t *testing.T) {
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "ns", "lease", "me", 60*time.Second)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire: %v %v", leader, err)
	}
	conflicts := 2
	client.PrependReactor("update", "leases", func(clienttesting.Action) (bool, kruntime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease", errors.New("the object has been modified"))
		}
		return false, nil, nil
	})
	if err := elector.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if holder := leaseHolder(t, client); holder != "" {
		t.Fatalf("holder after release = %q, want it cleared", holder)
	}
}

func TestReleaseReportsAConflictItCouldNotResolve(t *testing.T) {
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "ns", "lease", "me", 60*time.Second)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire: %v %v", leader, err)
	}
	client.PrependReactor("update", "leases", func(clienttesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease", errors.New("the object has been modified"))
	})
	if err := elector.Release(context.Background()); err == nil {
		t.Fatal("a release that never landed must not report success")
	}
}

// After Release the elector never renews or takes the Lease again, so a stray
// renewal cannot make a stopped controller the holder.
func TestReleasedElectorRefusesToRenew(t *testing.T) {
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "ns", "lease", "me", 60*time.Second)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire: %v %v", leader, err)
	}
	if err := elector.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if leader, err := elector.TryAcquireOrRenew(context.Background()); leader || !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("renewal after release = %v, %v; want refused with ErrLeaseReleased", leader, err)
	}
	if holder := leaseHolder(t, client); holder != "" {
		t.Fatalf("holder after a refused renewal = %q", holder)
	}
}

func leaseHolder(t *testing.T, client *fake.Clientset) string {
	t.Helper()
	lease, err := client.CoordinationV1().Leases("ns").Get(context.Background(), "lease", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}
