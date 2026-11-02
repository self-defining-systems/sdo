//go:build integration

package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCycleReadProbeTimeoutIsRecorded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	v := NewCycleReadView(blockedProbe{program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), release})
	started := time.Now()
	if _, err := v.ProbeTCP(sdk.ObjectRef{}, 80); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe error=%v", err)
	}
	if time.Since(started) > 10*time.Second {
		t.Fatal("probe timeout not enforced")
	}
	captured, err := sdktest.CaptureSnapshot(v, "app")
	if err != nil {
		t.Fatal(err)
	}
	replay := program.ReadFrom(captured)
	if _, err = replay.ProbeTCP(sdk.ObjectRef{}, 80); err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("timeout lost during replay: %v", err)
	}
}

