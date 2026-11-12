//go:build integration

package prober

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	mathrand "math/rand/v2"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"sdo.dev/operator/sdk/traffic"
)

// sshServer is a synthetic SSH service that accepts one client key and runs
// a few fixed commands.
type sshServer struct {
	listener net.Listener
	logins   chan string
}

func startSSHServer(t *testing.T, client ssh.PublicKey) *sshServer {
	t.Helper()
	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if !bytes.Equal(key.Marshal(), client.Marshal()) {
			return nil, errors.New("unknown key")
		}
		return nil, nil
	}}
	config.AddHostKey(hostSigner)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &sshServer{listener: listener, logins: make(chan string, 16)}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.serve(conn, config)
		}
	}()
	return server
}

func (s *sshServer) serve(conn net.Conn, config *ssh.ServerConfig) {
	defer conn.Close()
	server, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return
	}
	s.logins <- server.User()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		channel, requests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go func() {
			defer channel.Close()
			for request := range requests {
				if request.Type != "exec" {
					_ = request.Reply(false, nil)
					continue
				}
				var payload struct{ Command string }
				_ = ssh.Unmarshal(request.Payload, &payload)
				_ = request.Reply(true, nil)
				exit := func(status uint32) {
					_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
				}
				switch payload.Command {
				case "echo-stdin":
					data, _ := io.ReadAll(channel)
					_, _ = channel.Write(data)
					exit(0)
				case "fail":
					_, _ = channel.Stderr().Write([]byte("access denied\n"))
					exit(3)
				case "big":
					_, _ = channel.Write(bytes.Repeat([]byte("x"), 4*traffic.MaxResponseBody))
					exit(0)
				case "hang":
					time.Sleep(5 * time.Second)
				case "no-exit":
				}
				return
			}
		}()
	}
}

func sshClientKey(t *testing.T) ssh.Signer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func sshExecer(signer ssh.Signer) *SSHExecer {
	return &SSHExecer{Signer: func(user string) (ssh.Signer, error) {
		if user != "sdoprobe-writer" {
			return nil, errors.New("no such probe user")
		}
		return signer, nil
	}}
}

func TestSSHExecerRunsACommandAsTheProbeUser(t *testing.T) {
	signer := sshClientKey(t)
	server := startSSHServer(t, signer.PublicKey())
	execer := sshExecer(signer)
	exec := traffic.Exec{Address: server.listener.Addr().String(), Login: "svc", ProbeUser: "sdoprobe-writer", Command: "echo-stdin", Stdin: "0000"}
	result, err := execer.Exec(context.Background(), exec)
	if err != nil || result.Exit != 0 || string(result.Stdout) != "0000" {
		t.Fatalf("result = %+v, %v", result, err)
	}
	if login := <-server.logins; login != "svc" {
		t.Fatalf("login = %q", login)
	}
	exec.Command = "fail"
	result, err = execer.Exec(context.Background(), exec)
	if err != nil || result.Exit != 3 || result.Stderr != "access denied\n" {
		t.Fatalf("result = %+v, %v", result, err)
	}
	exec.Command = "no-exit"
	if _, err := execer.Exec(context.Background(), exec); err == nil || !strings.Contains(err.Error(), "exit status") {
		t.Fatalf("a session without an exit status is an error: %v", err)
	}
}

// A large answer is bounded, and the rest is drained so the command still
// finishes with its exit status instead of stalling into a timeout.
func TestSSHExecerBoundsAndDrainsStdout(t *testing.T) {
	signer := sshClientKey(t)
	server := startSSHServer(t, signer.PublicKey())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := sshExecer(signer).Exec(ctx, traffic.Exec{Address: server.listener.Addr().String(), Login: "svc", ProbeUser: "sdoprobe-writer", Command: "big"})
	if err != nil || result.Exit != 0 || len(result.Stdout) != traffic.MaxResponseBody {
		t.Fatalf("exit %d, %d bytes, %v", result.Exit, len(result.Stdout), err)
	}
}

// Transport failures are classified as the engine expects: a refused dial is
// a dial error, a deadline wraps the context's error, a rejected key is an
// error that is not a dial failure.
func TestSSHExecerClassifiesTransportFailures(t *testing.T) {
	signer := sshClientKey(t)
	server := startSSHServer(t, signer.PublicKey())
	address := server.listener.Addr().String()

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := closed.Addr().String()
	closed.Close()
	_, err = sshExecer(signer).Exec(context.Background(), traffic.Exec{Address: refused, Login: "svc", ProbeUser: "sdoprobe-writer", Command: "echo-stdin"})
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		t.Fatalf("refused dial = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = sshExecer(signer).Exec(ctx, traffic.Exec{Address: address, Login: "svc", ProbeUser: "sdoprobe-writer", Command: "hang"})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatalf("deadline = %v after %s", err, time.Since(started))
	}

	_, err = sshExecer(sshClientKey(t)).Exec(context.Background(), traffic.Exec{Address: address, Login: "svc", ProbeUser: "sdoprobe-writer", Command: "echo-stdin"})
	if err == nil || (errors.As(err, &opErr) && opErr.Op == "dial") {
		t.Fatalf("a rejected key is an error, not a dial failure: %v", err)
	}
}

// The prober gives every engine its SSH transport when it has a signer.
func TestProberRunsSSHStepsWithItsSigner(t *testing.T) {
	signer := sshClientKey(t)
	server := startSSHServer(t, signer.PublicKey())
	_, port, _ := net.SplitHostPort(server.listener.Addr().String())
	scenario := traffic.Scenario{
		ID: "ssh-echo", Target: traffic.Target{Service: "shell", Port: 22, Scheme: "ssh", User: "svc"},
		SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdoprobe",
		Users: []traffic.ProbeUser{{Name: "sdoprobe-writer", As: "pw", SSHKey: "key"}},
		Steps: []traffic.Step{{Name: "echo", Endpoint: echoEndpoint{}}},
	}
	workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe", Purpose: traffic.PurposeHealthProbe,
		Scenarios: []traffic.WorkloadScenario{{ID: scenario.ID}}}
	prober, err := New(Config{
		Namespace: "app", Catalog: traffic.Catalog{scenario}, Workloads: []traffic.Workload{workload},
		BaseURL:   func(target traffic.Target) string { return target.Scheme + "://127.0.0.1:" + port },
		Passwords: fixedKeys{}, Signer: sshExecer(signer).Signer,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, sample := prober.probes["probe"].engine.Iterate(context.Background(), 0)
	if sample.Outcome != traffic.OutcomeOK || sample.Exit == nil || *sample.Exit != 0 {
		t.Fatalf("sample = %+v", sample)
	}
}

type echoEndpoint struct{}

func (echoEndpoint) Build(context.Context, *mathrand.Rand, traffic.State) (traffic.Request, error) {
	return traffic.Request{Method: traffic.MethodExec, Path: "echo-stdin", Body: "sdoprobe"}, nil
}

func (echoEndpoint) Check(response traffic.Response, _ traffic.State) traffic.Verdict {
	if response.Status != 0 || string(response.Body) != "sdoprobe" {
		return traffic.Fail("exit %d stdout %q", response.Status, response.Body)
	}
	return traffic.Pass()
}

type fixedKeys struct{}

func (fixedKeys) Password(context.Context, string) (string, error) { return "synthetic-password", nil }
func (fixedKeys) SSHPublicKey(context.Context, string) (string, error) {
	return "ssh-ed25519 AAAAsynthetic", nil
}
