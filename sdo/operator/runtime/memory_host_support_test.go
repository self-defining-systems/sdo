package runtime

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/memoryhost"
)

type testMemoryRPC struct{ client pb.MemoryHostServiceClient }

func (c testMemoryRPC) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	return c.client.Evaluate(ctx, r)
}

func (c testMemoryRPC) ExecuteProgram(ctx context.Context, invocation *pb.MemoryProgramInvocation, read func(programread.Message) programread.Message) (*pb.RunProgramResponse, error) {
	return memoryhost.CallProgram(ctx, c.client, invocation, read)
}

type memoryTesting interface {
	Helper()
	Cleanup(func())
	Fatal(...any)
}

func memoryTestRPC(t memoryTesting, s *memoryhost.Server) (testMemoryRPC, func()) {
	t.Helper()
	listener := bufconn.Listen(16 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(memoryhost.MaxMessageBytes), grpc.MaxSendMsgSize(memoryhost.MaxMessageBytes))
	pb.RegisterMemoryHostServiceServer(server, s)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stop := func() { server.Stop(); <-done }
	t.Cleanup(stop)
	conn, err := grpc.NewClient("passthrough:///memory-host", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(memoryhost.MaxMessageBytes), grpc.MaxCallSendMsgSize(memoryhost.MaxMessageBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return testMemoryRPC{client: pb.NewMemoryHostServiceClient(conn)}, stop
}
