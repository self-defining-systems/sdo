//go:build integration

package runtime

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/sdk"
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

// The whole stop path: a controller that leads, then has its context
// cancelled, returns with the Lease holder cleared, after its work stopped.
func TestCancelledControllerReleasesItsLeaseOnReturn(t *testing.T) {
	const leaseNamespace = "sdo-system"
	repo := t.TempDir()
	writePlanFile(t, repo, syntheticPlan)
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "seed"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel once the controller leads and has started: the lease names it.
	held := make(chan struct{})
	go func() {
		defer cancel()
		for ctx.Err() == nil {
			lease, err := client.CoordinationV1().Leases(leaseNamespace).Get(context.Background(), controllerLeaseName, metav1.GetOptions{})
			if err == nil && lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
				close(held)
				time.Sleep(300 * time.Millisecond)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- RunWithOptions(ctx, nil, RuntimeOptions{
			Args: []string{"--namespace", "shop", "--control-namespace", leaseNamespace, "--app-root", repo, "--application", "shop",
				"--source-commit", "0123456789abcdef0123456789abcdef01234567", "--repository-mount-path", t.TempDir()},
			Stdout: &out, Stderr: &out, Client: client, Dispatcher: noDispatch{},
			AcceptedScope: func(context.Context, string) (sdk.ObservationScope, string, error) {
				scope, err := observation.Parse([]byte("---\nschema_version: 1\n---\nSynthetic test architecture.\n"))
				return scope, "0123456789abcdef0123456789abcdef01234567", err
			},
			PlanGit:      func(_ context.Context, _ string, args ...string) ([]byte, error) { return nil, nil },
			InformerSync: fastSyncPolicy(), InformerResync: 10 * time.Minute,
		})
	}()
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatalf("the controller never took the Lease: %s", out.String())
	}
	select {
	case err := <-done:
		_ = err // a cancelled run may report the cancellation
	case <-time.After(30 * time.Second):
		t.Fatalf("the cancelled controller did not return: %s", out.String())
	}
	lease, err := client.CoordinationV1().Leases(leaseNamespace).Get(context.Background(), controllerLeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatalf("Lease holder after the controller returned = %v, want cleared (output %s)", lease.Spec.HolderIdentity, out.String())
	}
}

type noDispatch struct{}

func (noDispatch) Dispatch(context.Context, IncidentRequest) (IncidentResult, error) {
	return IncidentResult{}, errors.New("no responder in this test")
}
