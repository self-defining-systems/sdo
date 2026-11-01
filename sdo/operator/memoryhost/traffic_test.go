package memoryhost

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"testing/synctest"
	"time"
)

type blockedTrafficHost struct {
	*fakeHost
	entered, release chan struct{}
}

func (h *blockedTrafficHost) Traffic(ctx context.Context, _ *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	close(h.entered)
	select {
	case <-h.release:
		return &pb.TrafficResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestTrafficBurstDoesNotBlockDetection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(time.Second)
		defer m.Close()
		h := &blockedTrafficHost{fakeHost: readyHost("active"), entered: make(chan struct{}), release: make(chan struct{})}
		if err := m.Swap(ctx, h, "active", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := m.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_BURST})
			done <- err
		}()
		<-h.entered
		// Detector calls require a read lease; a traffic RPC must permit one.
		if m.mu.TryRLock() {
			m.mu.RUnlock()
		} else {
			t.Error("burst blocks detector lease")
		}
		close(h.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

type lifecycleTrafficHost struct {
	*fakeHost
	running   bool
	failStart bool
	crashed   bool
}

func (h *lifecycleTrafficHost) Traffic(_ context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	if h.crashed {
		return nil, status.Error(codes.Unavailable, "crashed")
	}
	if r.Operation == pb.TrafficRequest_OPERATION_START {
		if h.failStart {
			return nil, errors.New("scheduler start failed")
		}
		h.running = true
	}
	return &pb.TrafficResponse{}, nil
}
func TestTrafficActivationFailureAndCrashRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(time.Second)
		defer m.Close()
		old := &lifecycleTrafficHost{fakeHost: readyHost("accepted")}
		if err := m.Swap(ctx, old, "accepted", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_START}); err != nil {
			t.Fatal(err)
		}
		bad := &lifecycleTrafficHost{fakeHost: readyHost("candidate"), failStart: true}
		if err := m.Swap(ctx, bad, "candidate", "sha256:abc", 1); err == nil {
			t.Fatal("activation accepted a failed scheduler")
		}
		if m.active != old || !old.running {
			t.Fatal("failed activation retired serving traffic")
		}
		recovered := &lifecycleTrafficHost{fakeHost: readyHost("accepted")}
		m.SetRecovery(func(context.Context) (Host, string, string, uint64, error) {
			return recovered, "accepted", "sha256:abc", 1, nil
		})
		old.crashed = true
		if _, err := m.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_WINDOWS}); err == nil {
			t.Fatal("crash returned traffic windows")
		}
		synctest.Wait()
		if _, err := m.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_WINDOWS}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if m.active != recovered || !recovered.running {
			t.Fatal("recovery lost accepted revision or running intent")
		}
	})
}

type timedTrafficHost struct {
	*fakeHost
	run func(context.Context) (*pb.TrafficResponse, error)
}

func (h *timedTrafficHost) Traffic(ctx context.Context, _ *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	return h.run(ctx)
}
func TestBoundedBurstOutlivesDetectorRPCTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(time.Second)
		defer m.Close()
		h := &timedTrafficHost{fakeHost: readyHost("active"), run: func(ctx context.Context) (*pb.TrafficResponse, error) {
			select {
			case <-time.After(45 * time.Second):
				return &pb.TrafficResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		if err := m.Swap(context.Background(), h, "active", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Traffic(context.Background(), &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_BURST}); err != nil {
			t.Fatal("accepted bounded workload inherited detector timeout", err)
		}
	})
}
