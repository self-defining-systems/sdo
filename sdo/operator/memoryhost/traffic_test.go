package memoryhost

import (
	"context"
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
