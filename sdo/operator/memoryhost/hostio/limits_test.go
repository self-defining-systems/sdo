package hostio

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

// Memory-code RPCs decode every read receipt of a request; unbounded fan-out of them
// is what exhausted the host's address space, so only a few may run at once.
func TestHeavyCallsAreBounded(t *testing.T) {
	interceptor := boundHeavyCalls(2)
	var running, peak atomic.Int32
	release := make(chan struct{})
	handler := func(ctx context.Context, request any) (any, error) {
		now := running.Add(1)
		for {
			seen := peak.Load()
			if now <= seen || peak.CompareAndSwap(seen, now) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/sdodev.contracts.v1alpha1.MemoryHostService/Diagnose"}, handler)
		}()
	}
	for running.Load() < 2 {
		runtime.Gosched()
	}
	close(release)
	wg.Wait()
	if peak.Load() > 2 {
		t.Fatalf("peak concurrent heavy calls %d exceeds the bound", peak.Load())
	}
}

func TestLightCallsAreNotQueuedBehindHeavyOnes(t *testing.T) {
	interceptor := boundHeavyCalls(1)
	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_, _ = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Diagnose"}, func(context.Context, any) (any, error) {
			close(started)
			<-block
			return nil, nil
		})
	}()
	<-started
	if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Ready"}, func(context.Context, any) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	close(block)
}

func TestAddressSpaceLimitLeavesHeadroomAboveTheSoftHeapLimit(t *testing.T) {
	const capBytes uint64 = 512 << 20
	if hard, soft := dataLimit(capBytes), capBytes*3/4; hard < 2*soft {
		t.Fatalf("RLIMIT_DATA %d counts mapped address space, not live heap; need at least twice the soft limit %d", hard, soft)
	}
}
