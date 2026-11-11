package generichealth

import (
	"context"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

// crashSnapshot is one pod whose container exited with a Python traceback at
// crashedAt and whose next incarnation started right after it, Ready or not.
func crashSnapshot(now, crashedAt time.Time, ready bool) sdktest.Snapshot {
	started := crashedAt.Add(time.Second)
	match := map[string]logs.Match{"python-traceback": {Count: 1}}
	return sdktest.Snapshot{
		NamespaceName:  "ns",
		ObservedAtTime: &now,
		PodList: []core.Pod{{
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "svc-1", UID: "uid-1"},
			Spec:       core.PodSpec{Containers: []core.Container{{Name: "main"}}},
			Status: core.PodStatus{Phase: core.PodRunning, ContainerStatuses: []core.ContainerStatus{{
				Name: "main", Ready: ready, RestartCount: 1,
				State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: meta.NewTime(started)}},
			}}},
		}},
		Logs: map[string]logs.Result{"generic-crashes": {ObservedAt: now, Windows: []logs.Window{{
			Namespace: "ns", Pod: "svc-1", UID: "uid-1", Container: "main", RestartCount: 1,
			Cycles:       []logs.Cycle{{Start: started, End: now, Running: true}},
			Terminations: []logs.Termination{{RestartCount: 0, FinishedAt: crashedAt, ExitCode: 1, Tail: logs.Cycle{Start: crashedAt.Add(-time.Minute), End: crashedAt, Matches: match}}},
		}}}},
	}
}

func crashFindings(t *testing.T, s sdktest.Snapshot) int {
	t.Helper()
	findings, err := NewCrashes(sdk.DetectorSpec{ID: "runtime-crashes"}).Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return len(findings)
}

// A container that crashed once (a service that starts before its database)
// and has served Ready ever since has recovered: its retained termination is
// history, not current health. A recent crash, or one whose successor is not
// Ready, stays reported, so a container that crashes periodically is still
// found after each crash.
func TestCrashesReportsOnlyCrashesNotYetRecoveredFrom(t *testing.T) {
	now := time.Date(2026, 11, 11, 5, 30, 0, 0, time.UTC)
	cases := []struct {
		name    string
		crashed time.Duration
		ready   bool
		want    int
	}{
		{"startup crash, Ready for 20 minutes since", 20 * time.Minute, true, 0},
		{"crash a minute ago, Ready again", time.Minute, true, 1},
		{"old crash, successor not Ready", 20 * time.Minute, false, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := crashFindings(t, crashSnapshot(now, now.Add(-c.crashed), c.ready)); got != c.want {
				t.Fatalf("crash findings = %d, want %d", got, c.want)
			}
		})
	}
}
