package runtime

import (
	"context"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"testing"
	"time"
)

// This isolates per-evaluation adapter, delta and bufconn costs. OS process
// startup, compilation and Kubernetes reads are deliberately excluded.
func BenchmarkMemoryHostEvaluation(b *testing.B) {
	ctx := context.Background()
	snapshot := sdktest.Snapshot{NamespaceName: "toy"}
	local := servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{ID: "endpoints"})
	b.Run("flag-off", func(b *testing.B) {
		for b.Loop() {
			if _, err := local.Detect(ctx, snapshot); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("mirrored-bufconn", func(b *testing.B) {
		server, err := memoryhost.NewServer("accepted", "digest", []sdk.Detector{local})
		if err != nil {
			b.Fatal(err)
		}
		rpc, stop := memoryTestRPC(b, server)
		host := &simActivationHost{testMemoryRPC: rpc, stop: stop}
		manager := memoryhost.NewManager(time.Second)
		b.Cleanup(func() { _ = manager.Close() })
		if err = manager.Publish(ctx, snapshot); err != nil {
			b.Fatal(err)
		}
		if err = manager.Prepare(ctx, host); err != nil {
			b.Fatal(err)
		}
		if err = manager.Activate(ctx, host, "accepted", "digest"); err != nil {
			b.Fatal(err)
		}
		ready, err := host.Ready(ctx)
		if err != nil {
			b.Fatal(err)
		}
		remote, err := NewRemoteMemory(ready, manager, func(sdk.DetectionContext) (uint64, error) { return 0, nil })
		if err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			if _, err := remote.Detectors[0].Detect(ctx, snapshot); err != nil {
				b.Fatal(err)
			}
		}
	})
}
