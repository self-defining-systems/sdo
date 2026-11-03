package runtime

import (
	"context"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func (c testMemoryRPC) Traffic(ctx context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	return c.client.Traffic(ctx, r)
}
