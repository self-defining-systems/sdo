package builder

import (
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/durationpb"
	"os/exec"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"time"
)

func TestHealthRefreshBoundsBuildAndPendingJudge(t *testing.T) {
	cfg := RunConfig{Controller: &contracts.ControllerConfig{Duration: durationpb.New(20 * time.Millisecond)}}
	err := runWithHealthRefresh(context.Background(), cfg, func(ctx context.Context, _ RunConfig) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("refresh/build has no installation deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded or hidden refresh failure: %v", err)
	}
}

func TestHealthRefreshExhaustedRelaunchBudgetIsNotHealthy(t *testing.T) {
	restart := exec.Command("sh", "-c", "exit 76").Run()
	err := runWithHealthRefresh(context.Background(), RunConfig{Controller: &contracts.ControllerConfig{Duration: durationpb.New(time.Millisecond)}}, func(ctx context.Context, _ RunConfig) error { <-ctx.Done(); return restart })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired unverified install reported healthy: %v", err)
	}
}

func TestHealthRefreshRestartsAreBoundedWithoutDuration(t *testing.T) {
	restart := exec.Command("sh", "-c", "exit 76").Run()
	runs := 0
	err := runWithHealthRefresh(context.Background(), RunConfig{Controller: &contracts.ControllerConfig{}}, func(context.Context, RunConfig) error { runs++; return restart })
	if err == nil || runs != maxHealthRefreshRestarts+1 {
		t.Fatalf("unbounded restart loop: runs=%d err=%v", runs, err)
	}
}
