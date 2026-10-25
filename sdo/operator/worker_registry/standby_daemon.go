package worker_registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// AwaitStandbyTask is the outbound filesystem long poll. The inbox is mounted
// read-only; only the controller can publish the immutable AgentTask. The ready
// file is refreshed only while waiting, never after claiming an incident.
func AwaitStandbyTask(ctx context.Context, inbox, ready string) (*contracts.AgentTask, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(inbox, TaskDataKey))
		if err == nil {
			task := new(contracts.AgentTask)
			if err := wire.Unmarshal(data, task); err != nil {
				return nil, err
			}
			request := new(contracts.IncidentRequest)
			if err := task.GetRequest().UnmarshalTo(request); err != nil || request.GetIncidentId() == "" {
				return nil, errors.New("standby requires a typed incident task")
			}
			return task, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		heartbeat, err := os.Stat(filepath.Join(inbox, "owner-heartbeat"))
		if err != nil {
			return nil, err
		}
		if time.Since(heartbeat.ModTime()) > 30*time.Second {
			return nil, errors.New("standby controller heartbeat expired")
		}
		if err := os.WriteFile(ready, []byte("waiting"), 0600); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

type StandbyDaemonOptions struct {
	Inbox, Ready, Checkout string
	SessionDirectory       string
	Command                []string
	// ProxyAddress is the existing loopback api-proxy, never a claim listener.
	ProxyAddress string
}

// RunStandbyDaemon consumes exactly one task, invokes the registered command
// unchanged and exits with it. Provider homes stay in the durable canonical
// incident session, so ordinary reflection resumes the same provider session.
func RunStandbyDaemon(ctx context.Context, options StandbyDaemonOptions) error {
	if len(options.Command) == 0 {
		return errors.New("standby requires a registered agent command")
	}
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer readyRead.Close()
	claimRead, claimWrite, err := os.Pipe()
	if err != nil {
		readyWrite.Close()
		return err
	}
	defer claimWrite.Close()
	command := exec.CommandContext(ctx, options.Command[0], options.Command[1:]...)
	command.Dir = options.Checkout
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Env = append(os.Environ(), "SDO_STANDBY_READY_FD=3", "SDO_STANDBY_CLAIM_FD=4", "SDO_STANDBY_INBOX="+options.Inbox)
	command.ExtraFiles = []*os.File{readyWrite, claimRead}
	if err := command.Start(); err != nil {
		readyWrite.Close()
		claimRead.Close()
		return err
	}
	workerStarted := time.Now().UTC()
	readyWrite.Close()
	claimRead.Close()
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	imported := make(chan error, 1)
	go func() {
		var token [1]byte
		n, err := readyRead.Read(token[:])
		if err == nil && (n != 1 || token[0] != 'R') {
			err = errors.New("standby worker sent invalid import handshake")
		}
		imported <- err
	}()
	preloadTimeout := time.NewTimer(30 * time.Second)
	defer preloadTimeout.Stop()
	select {
	case <-preloadTimeout.C:
		return errors.New("standby worker import handshake timed out")
	case <-ctx.Done():
		return ctx.Err()
	case err := <-imported:
		if err != nil {
			return fmt.Errorf("preload registered worker: %w", err)
		}
	}
	workerImportedAt := time.Now().UTC()
	task, err := AwaitStandbyTask(ctx, options.Inbox, options.Ready)
	if err != nil {
		return err
	}

	if output, err := exec.CommandContext(ctx, "git", "-C", options.Checkout, "reset", "--hard", task.GetWorkspace().GetBaseCommit()).CombinedOutput(); err != nil {
		return fmt.Errorf("bind accepted commit: %w: %s", err, output)
	}
	if options.ProxyAddress != "" {
		deadline := time.Now().Add(2 * time.Second)
		for {
			connection, err := net.DialTimeout("tcp", options.ProxyAddress, 20*time.Millisecond)
			if err == nil {
				connection.Close()
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("standby incident API proxy unavailable: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	// The Python process is already imported but holds no incident data. Its
	// private pipe releases it only after the Go-owned bind and proxy readiness.
	if options.SessionDirectory != "" {
		_ = writeStandbyJSON(filepath.Join(options.SessionDirectory, "startup-daemon.json"), map[string]string{"registered_agent_process_started": workerStarted.Format(time.RFC3339Nano), "worker_imported_at": workerImportedAt.Format(time.RFC3339Nano), "worker_claim_released": time.Now().UTC().Format(time.RFC3339Nano)})
	}
	consumed, err := os.OpenFile(options.Ready+".consumed", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("standby already consumed: %w", err)
	}
	consumed.Close()
	if _, err := claimWrite.Write([]byte{'C'}); err != nil {
		return err
	}
	claimWrite.Close()
	err = command.Wait()
	waited = true
	return err
}
