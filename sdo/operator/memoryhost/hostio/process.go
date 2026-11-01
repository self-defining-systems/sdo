package hostio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"time"
)

type LaunchConfig struct {
	Namespace               string
	DisableSyntheticTraffic bool
	Binary                  string
	Commit                  string
	Digest                  string
	SocketDirectory         string
	MemoryBytes             uint64
	Output                  io.Writer
}

// Process is independent of the controller's context: candidate preparation
// cancellation must not kill a successfully published host. Close owns reaping.
type Process struct {
	client    pb.MemoryHostServiceClient
	conn      *grpc.ClientConn
	command   *exec.Cmd
	directory string
	done      chan struct{}
	once      sync.Once
}

func BinaryDigest(path string) (string, error) {
	input, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer input.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, input); err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", digest.Sum(nil)), nil
}
func Launch(config LaunchConfig) (*Process, error) {
	if config.Commit == "" || config.Binary == "" || config.MemoryBytes < 64<<20 {
		return nil, errors.New("host requires binary, commit and memory cap of at least 64MiB")
	}
	digest, err := BinaryDigest(config.Binary)
	if err != nil {
		return nil, err
	}
	if digest != config.Digest {
		return nil, errors.New("host binary digest mismatch")
	}
	directory, err := os.MkdirTemp(config.SocketDirectory, "sdo-memory-")
	if err != nil {
		return nil, err
	}
	socket := filepath.Join(directory, "host.sock")
	command := exec.Command(config.Binary, "--namespace", config.Namespace, "--synthetic-traffic="+strconv.FormatBool(!config.DisableSyntheticTraffic), "--socket", socket, "--commit", config.Commit, "--memory-bytes", strconv.FormatUint(config.MemoryBytes, 10))
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Env = append(os.Environ(), "GOMEMLIMIT="+strconv.FormatUint(config.MemoryBytes*3/4, 10))
	command.Stdout = config.Output
	command.Stderr = config.Output
	if err = command.Start(); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	p := &Process{command: command, directory: directory, done: make(chan struct{})}
	go func() { _ = command.Wait(); close(p.done) }()
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 10 * time.Millisecond, Multiplier: 1.5, Jitter: 0.2, MaxDelay: 100 * time.Millisecond}, MinConnectTimeout: time.Second}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.MaxCallSendMsgSize(16<<20)))
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	p.conn = conn
	p.client = pb.NewMemoryHostServiceClient(conn)
	return p, nil
}
func (p *Process) Client() pb.MemoryHostServiceClient { return p.client }
func (p *Process) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	// WaitForReady handles socket creation races without bespoke polling.
	return p.client.Ready(ctx, &pb.ReadyRequest{}, grpc.WaitForReady(true))
}
func (p *Process) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	return p.client.Evaluate(ctx, r)
}
func (p *Process) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	return p.client.Diagnose(ctx, r)
}
func (p *Process) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	return p.client.Mitigate(ctx, r)
}
func (p *Process) Close() error {
	p.once.Do(func() {
		if p.command != nil && p.command.Process != nil {
			_ = syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
		if p.conn != nil {
			_ = p.conn.Close()
		}
		_ = os.RemoveAll(p.directory)
	})
	return nil
}

// Serve is the generated host main's sole entry point. The cap is installed
// before accepting any memory-code request. Read factories may use the shared service account.
func Serve(ctx context.Context, socket string, capBytes uint64, server pb.MemoryHostServiceServer) error {
	if err := SetMemoryLimit(capBytes); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	pb.RegisterMemoryHostServiceServer(grpcServer, server)
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			grpcServer.Stop()
		case <-finished:
		}
	}()
	return grpcServer.Serve(listener)
}

// WaitExited is useful to launchers supervising crash recovery and to tests.
func (p *Process) WaitExited(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetMemoryLimit must precede memory constructors as well as RPC serving.
func SetMemoryLimit(capBytes uint64) error {
	if capBytes < 64<<20 || capBytes > 1<<60 {
		return errors.New("memory cap must be between 64MiB and 1EiB")
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_DATA, &syscall.Rlimit{Cur: capBytes, Max: capBytes}); err != nil {
		return fmt.Errorf("host memory cap: %w", err)
	}
	debug.SetMemoryLimit(int64(capBytes * 3 / 4))
	return nil
}

// MirrorFrame acknowledges a complete contiguous frame before a barrier is used.
func (p *Process) MirrorFrame(ctx context.Context, frame *pb.MirrorRequest) error {
	stream, err := p.client.Mirror(ctx)
	if err != nil {
		return err
	}
	if err = stream.Send(frame); err != nil {
		return err
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if ack.GetSequence() != frame.Sequence {
		return errors.New("mirror acknowledgment sequence mismatch")
	}
	return nil
}

func (p *Process) Traffic(ctx context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	return p.client.Traffic(ctx, r)
}
