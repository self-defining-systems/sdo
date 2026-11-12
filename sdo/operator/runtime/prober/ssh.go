package prober

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"golang.org/x/crypto/ssh"
	"sdo.dev/operator/sdk/traffic"
)

// SSHExecer is the prober's traffic.Execer: it runs one EXEC per fresh SSH
// connection, signed by the probe user's key. The private key stays inside
// the Signer the memory host derives; nothing here logs or returns it.
//
// It accepts any host key: the probe sends only synthetic data, and
// public-key authentication signs the session, so an impostor cannot replay
// it. Pinning a key per prober start would turn a legitimate host-key change
// (a replaced sshd Pod, replicas with different keys) into a failure lasting
// until the controller restarts.
type SSHExecer struct {
	// Signer returns the key that signs sessions as a probe user.
	Signer func(user string) (ssh.Signer, error)
	// DialContext opens connections; by default a net.Dialer bounded by
	// DialTimeout, so a refused or blocked dial is a dial failure.
	DialContext func(ctx context.Context, network string, address string) (net.Conn, error)
}

// Exec implements traffic.Execer.
func (x *SSHExecer) Exec(ctx context.Context, exec traffic.Exec) (traffic.ExecResult, error) {
	if x.Signer == nil {
		return traffic.ExecResult{}, errors.New("ssh: no probe-user signer")
	}
	signer, err := x.Signer(exec.ProbeUser)
	if err != nil {
		return traffic.ExecResult{}, fmt.Errorf("ssh key of probe user %s: %w", exec.ProbeUser, err)
	}
	dial := x.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: DialTimeout}).DialContext
	}
	conn, err := dial(ctx, "tcp", exec.Address)
	if err != nil {
		return traffic.ExecResult{}, err
	}
	defer conn.Close()
	// x/crypto/ssh takes no context: bound the connection by the request's
	// deadline and close it when the request is cancelled.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	clientConn, channels, requests, err := ssh.NewClientConn(conn, exec.Address, &ssh.ClientConfig{
		User: exec.Login, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		return traffic.ExecResult{}, sshError(ctx, "ssh handshake", err)
	}
	client := ssh.NewClient(clientConn, channels, requests)
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return traffic.ExecResult{}, sshError(ctx, "ssh session", err)
	}
	defer session.Close()
	stdout := &boundedBuffer{limit: traffic.MaxResponseBody}
	stderr := &boundedBuffer{limit: traffic.MaxExecStderr}
	session.Stdin, session.Stdout, session.Stderr = strings.NewReader(exec.Stdin), stdout, stderr
	err = session.Run(exec.Command)
	result := traffic.ExecResult{Stdout: stdout.data, Stderr: string(stderr.data)}
	var exitErr *ssh.ExitError
	switch {
	case err == nil:
		return result, nil
	case errors.As(err, &exitErr):
		result.Exit = exitErr.ExitStatus()
		return result, nil
	case ctx.Err() == nil && errors.As(err, new(*ssh.ExitMissingError)):
		return traffic.ExecResult{}, errors.New("ssh session ended without an exit status")
	}
	return traffic.ExecResult{}, sshError(ctx, "ssh command", err)
}

// sshError names what failed and, when the request ended, wraps the
// context's error so the engine classifies a timeout.
func sshError(ctx context.Context, what string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", what, ctx.Err())
	}
	return fmt.Errorf("%s: %w", what, err)
}

// boundedBuffer keeps the first limit bytes written and discards the rest
// without failing, so the remote's output is drained to the end.
type boundedBuffer struct {
	limit int
	data  []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - len(b.data); room > 0 {
		b.data = append(b.data, p[:min(room, len(p))]...)
	}
	return len(p), nil
}
