package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// RunTrafficObservation reads the accepted revision's running host. It neither
// starts traffic nor treats absent observations as healthy.
func RunTrafficObservation(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("observe-traffic", flag.ContinueOnError)
	app := flags.String("app", "", "accepted application checkout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(*app) || flags.NArg() != 0 {
		return errors.New("observe-traffic requires --app with an absolute checkout path")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	head := func() (string, error) { return processio.AcceptedHead(ctx, *app) }
	commit, err := head()
	if err != nil {
		return fmt.Errorf("read accepted traffic revision: %w", err)
	}
	response, err := observeTraffic(ctx, "/proc", commit)
	if err != nil {
		return err
	}
	after, err := head()
	if err != nil {
		return err
	}
	if after != commit {
		return errors.New("accepted revision changed during traffic observation")
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(response)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, string(encoded))
	return err
}

func observeTraffic(ctx context.Context, procRoot, commit string) (*pb.TrafficResponse, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, err
	}
	var sockets []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(procRoot, entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		args := strings.Split(string(data), "\x00")
		if len(args) == 0 || filepath.Base(args[0]) != "sdo-memory-host" {
			continue
		}
		value := func(flag string) string {
			for i := 1; i+1 < len(args); i++ {
				if args[i] == flag {
					return args[i+1]
				}
			}
			return ""
		}
		if value("--commit") == commit && value("--socket") != "" {
			sockets = append(sockets, value("--socket"))
		}
	}
	if len(sockets) != 1 {
		return nil, fmt.Errorf("traffic observation requires one running host for accepted revision %s; found %d", commit, len(sockets))
	}
	conn, err := grpc.NewClient("unix://"+sockets[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewMemoryHostServiceClient(conn)
	before, err := client.Ready(ctx, &pb.ReadyRequest{})
	if err != nil {
		return nil, err
	}
	if before.MemoryCommit != commit || !before.MirrorComplete {
		return nil, errors.New("traffic host does not serve a complete accepted revision")
	}
	response, err := client.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_WINDOWS})
	if err != nil {
		return nil, err
	}
	after, err := client.Ready(ctx, &pb.ReadyRequest{})
	if err != nil {
		return nil, err
	}
	if after.MemoryCommit != commit || !after.MirrorComplete || after.BinaryDigest != before.BinaryDigest {
		return nil, errors.New("traffic host changed during observation")
	}
	return response, nil
}
