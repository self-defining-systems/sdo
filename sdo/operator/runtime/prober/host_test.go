package prober

import (
	"context"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestHostTrafficLifecycle(t *testing.T) {
	p, err := New(Config{Namespace: "toy"})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHost(p)
	defer h.Close()
	for _, op := range []pb.TrafficRequest_Operation{pb.TrafficRequest_OPERATION_START, pb.TrafficRequest_OPERATION_START, pb.TrafficRequest_OPERATION_WINDOWS, pb.TrafficRequest_OPERATION_RESET, pb.TrafficRequest_OPERATION_STOP, pb.TrafficRequest_OPERATION_STOP} {
		if _, err := h.Traffic(context.Background(), &pb.TrafficRequest{Operation: op}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.Traffic(context.Background(), &pb.TrafficRequest{}); err == nil {
		t.Fatal("unknown operation accepted")
	}
	if _, err := h.Traffic(context.Background(), &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_BURST}); err == nil {
		t.Fatal("missing workload accepted")
	}
	if _, err := h.Traffic(context.Background(), &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_PROBE, Target: &pb.ObjectRef{Kind: "Service", Namespace: "other", Name: "api"}, Port: 80}); err == nil {
		t.Fatal("cross-namespace probe accepted")
	}
	h.Close()
	if _, err := h.Traffic(context.Background(), &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_START}); err == nil {
		t.Fatal("closed scheduler restarted")
	}
}
