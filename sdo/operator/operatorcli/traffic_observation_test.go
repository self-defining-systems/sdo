//go:build integration

package operatorcli

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type observedTrafficHost struct {
	pb.UnimplementedMemoryHostServiceServer
	commit   string
	complete bool
}

func (h *observedTrafficHost) Ready(context.Context, *pb.ReadyRequest) (*pb.ReadyResponse, error) {
	return &pb.ReadyResponse{MemoryCommit: h.commit, MirrorComplete: h.complete}, nil
}
func (h *observedTrafficHost) Traffic(_ context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	if r.Operation != pb.TrafficRequest_OPERATION_WINDOWS {
		panic("read observation mutated traffic")
	}
	w, _ := structpb.NewStruct(map[string]any{"scenarios": []any{map[string]any{"qualified": true}}})
	return &pb.TrafficResponse{Windows: map[string]*structpb.Struct{"health": w}}, nil
}
func TestTrafficObservationUsesLiveAcceptedHost(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		commit                         string
		complete, duplicate, wantError bool
	}{
		{name: "accepted", commit: "accepted", complete: true},
		{name: "stale RPC", commit: "stale", complete: true, wantError: true},
		{name: "incomplete mirror", commit: "accepted", wantError: true},
		{name: "ambiguous process", commit: "accepted", complete: true, duplicate: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			socketRoot, err := os.MkdirTemp("/tmp", "traffic-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
			socket := filepath.Join(socketRoot, "host.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			pb.RegisterMemoryHostServiceServer(server, &observedTrafficHost{commit: tc.commit, complete: tc.complete})
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			process := func(id string) {
				p := filepath.Join(root, id)
				if err := os.Mkdir(p, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(p, "cmdline"), []byte("/compiled/sdo-memory-host\x00--commit\x00accepted\x00--socket\x00"+socket+"\x00"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			process("11")
			if tc.duplicate {
				process("12")
			}
			response, err := observeTraffic(context.Background(), root, "accepted")
			if (err != nil) != tc.wantError {
				t.Fatalf("response=%v error=%v", response, err)
			}
			if err == nil && response.Windows["health"] == nil {
				t.Fatal("live window missing")
			}
		})
	}
}
