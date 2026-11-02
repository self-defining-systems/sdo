// Package checktransport is the worker's narrow, advisory memory-check RPC.
// Only the broker retains attestations; a forged response cannot publish files.
package checktransport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Check sends only a decision. Checkout, base, policy and toolchain are selected
// by the incident-bound broker, never by request-supplied paths or environment.
func Check(ctx context.Context, directory string, decision *contracts.ReflectionDecision) (*contracts.MemoryCheckReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if decision == nil {
		return nil, fmt.Errorf("reflection decision is required")
	}
	encoded, err := protojson.Marshal(decision)
	if err != nil {
		return nil, err
	}
	// This is a file/JSON transport, never HTML. Avoid HTML escaping, which
	// can expand an otherwise valid worker decision sixfold before framing.
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(struct {
		Command    string            `json:"command"`
		Parameters map[string]string `json:"parameters"`
	}{"memory-check", map[string]string{"decision": string(encoded)}})
	if err != nil {
		return nil, err
	}
	request := body.Bytes()
	name := "check-" + rand.Text() + ".json"
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	requestPath, responsePath := filepath.Join("requests", name), filepath.Join("responses", name)
	temporary := filepath.Join("requests", "."+name)
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer root.Remove(temporary)
	if _, err := file.Write(request); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := root.Rename(temporary, requestPath); err != nil {
		return nil, err
	}
	defer root.Remove(requestPath)
	defer root.Remove(responsePath)
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		data, err := root.ReadFile(responsePath)
		if err == nil {
			var response struct {
				Error    string          `json:"error"`
				TimedOut bool            `json:"memory_check_timed_out"`
				Report   json.RawMessage `json:"memory_check_report"`
			}
			if err := json.Unmarshal(data, &response); err != nil {
				return nil, err
			}
			if response.TimedOut {
				return nil, fmt.Errorf("broker memory check timed out: %s: %w", response.Error, context.DeadlineExceeded)
			}
			if response.Error != "" {
				return nil, fmt.Errorf("broker memory check: %s", response.Error)
			}
			report := new(contracts.MemoryCheckReport)
			if err := protojson.Unmarshal(response.Report, report); err != nil {
				return nil, err
			}
			return report, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
