package runtime

import (
	"context"
	"errors"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type simActivationHost struct {
	testMemoryRPC
	stop func()
}

func (h *simActivationHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	return h.client.Ready(ctx, &pb.ReadyRequest{})
}

func (h *simActivationHost) Close() error { h.stop(); return nil }

func (h *simActivationHost) MirrorFrame(ctx context.Context, frame *pb.MirrorRequest) error {
	stream, err := h.client.Mirror(ctx)
	if err != nil {
		return err
	}
	if err = stream.Send(frame); err != nil {
		return err
	}
	ack, err := stream.CloseAndRecv()
	if err == nil && ack.Sequence != frame.Sequence {
		return errors.New("mirror acknowledgment mismatch")
	}
	return err
}
