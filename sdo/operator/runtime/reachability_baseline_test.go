package runtime

import (
	"sdo.dev/operator/sdk/inputs"
	"context"
	corev1 "k8s.io/api/core/v1"
	"net"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"syscall"
	"testing"
	"time"
)

func refusingObserver(t *testing.T, snapshots *mutableSnapshots) *ServiceReachabilityObserver {
	t.Helper()
	dialer, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(link traffic.Link) string { return link.To }, DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	}})
	if err != nil {
		t.Fatal(err)
	}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Prober: reachabilityDirect{dialer}, Snapshots: snapshots, Interval: time.Millisecond})
	t.Cleanup(observer.Stop)
	return observer
}

func stateBaseline(t *testing.T, state string) *servicehealth.ReachabilityBaseline {
	t.Helper()
	b, err := memory.ReadReachabilityBaselineState(state)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func reachabilityFindings(t *testing.T, ctx context.Context, provider SnapshotProvider) []sdk.Finding {
	t.Helper()
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability"}).Detect(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func healthyGoal() sdk.Detector {
	goal := controllerDetector("health-objective", time.Second, sdk.Finding{})
	goal.spec.Class = sdk.DetectorClassHealth
	return goal
}

func firingGoal() sdk.Detector {
	goal := controllerDetector("health-objective", time.Second, stateFinding("health-objective"))
	goal.spec.Class = sdk.DetectorClassHealth
	return goal
}

func withMode(mode string) *mutableSnapshots {
	s := reachabilityApp(9600)
	if mode != "" {
		s.PodList[0].Spec.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: mode}}
	}
	return &mutableSnapshots{snapshot: s}
}

func withReachability(s sdktest.Snapshot, w servicehealth.ReachabilityWindow) sdk.DetectionContext {
	return trafficContext{Carrier: inputs.New(s).WithReachability(reachabilityWindow{&w}), ScopedObservationSource: sdk.ScopedSource(s)}
}

