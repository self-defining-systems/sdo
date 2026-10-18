package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type SubprocessDispatcher struct {
	Argv    []string
	Env     []string
	Timeout time.Duration
}

func (d SubprocessDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	if err := request.Validate(); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident request: %w", err)
	}
	if len(d.Argv) == 0 || strings.TrimSpace(d.Argv[0]) == "" {
		return IncidentResult{}, fmt.Errorf("dispatcher argv is required")
	}
	if d.Timeout <= 0 {
		return IncidentResult{}, fmt.Errorf("dispatcher timeout must be positive")
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()
	payload, err := json.Marshal(request)
	if err != nil {
		return IncidentResult{}, fmt.Errorf("encode incident request: %w", err)
	}
	command := exec.CommandContext(dispatchCtx, d.Argv[0], d.Argv[1:]...)
	command.Env = append(os.Environ(), d.Env...)
	command.Stdin = bytes.NewReader(payload)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		if dispatchCtx.Err() != nil {
			return IncidentResult{}, dispatchCtx.Err()
		}
		return IncidentResult{}, fmt.Errorf("dispatcher exited unsuccessfully: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	decoder := json.NewDecoder(&stdout)
	decoder.DisallowUnknownFields()
	var result IncidentResult
	if err := decoder.Decode(&result); err != nil {
		return IncidentResult{}, fmt.Errorf("decode incident result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return IncidentResult{}, fmt.Errorf("decode incident result: trailing output")
	}
	if err := result.ValidateFor(request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident result: %w", err)
	}
	return result, nil
}
