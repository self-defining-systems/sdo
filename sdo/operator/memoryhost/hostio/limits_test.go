package hostio

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type limitTestStream struct{ grpc.ServerStream }

func (limitTestStream) Context() context.Context { return context.Background() }

// Only a few playbooks may execute and decode inputs concurrently.
func TestHeavyCallsAreBounded(t *testing.T) {
	interceptor := boundProgramCalls(2)
	var running, peak atomic.Int32
	release := make(chan struct{})
	handler := func(any, grpc.ServerStream) error {
		now := running.Add(1)
		for {
			seen := peak.Load()
			if now <= seen || peak.CompareAndSwap(seen, now) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = interceptor(nil, limitTestStream{}, &grpc.StreamServerInfo{FullMethod: pb.MemoryHostService_RunProgram_FullMethodName}, handler)
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
	interceptor := boundProgramCalls(1)
	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = interceptor(nil, limitTestStream{}, &grpc.StreamServerInfo{FullMethod: pb.MemoryHostService_RunProgram_FullMethodName}, func(any, grpc.ServerStream) error {
			close(started)
			<-block
			return nil
		})
	}()
	<-started
	if err := interceptor(nil, limitTestStream{}, &grpc.StreamServerInfo{FullMethod: pb.MemoryHostService_Mirror_FullMethodName}, func(any, grpc.ServerStream) error { return nil }); err != nil {
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
