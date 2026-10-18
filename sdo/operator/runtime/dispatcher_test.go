package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSubprocessDispatcherRoundTripsGoldenProtocol(t *testing.T) {
	request := goldenRequest(t)
	dispatcher := helperDispatcher("success", 10*time.Second)

	result, err := dispatcher.Dispatch(context.Background(), request)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result.IncidentID != request.IncidentID || result.Status != IncidentCompleted {
		t.Fatalf("unexpected result %#v", result)
	}
}

func TestSubprocessDispatcherRejectsFailureModes(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		timeout time.Duration
		match   string
	}{
		{name: "malformed output", mode: "malformed", timeout: 10 * time.Second, match: "decode incident result"},
		{name: "nonzero exit", mode: "nonzero", timeout: 10 * time.Second, match: "dispatcher exited unsuccessfully"},
		{name: "timeout", mode: "wait", timeout: 10 * time.Millisecond, match: "deadline exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := helperDispatcher(test.mode, test.timeout).Dispatch(context.Background(), goldenRequest(t))
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("expected %q error, got %v", test.match, err)
			}
		})
	}
}

func TestSubprocessDispatcherHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := helperDispatcher("wait", time.Second).Dispatch(ctx, goldenRequest(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestSubprocessDispatcherHelper(t *testing.T) {
	if os.Getenv("SDO_DISPATCHER_HELPER") != "1" {
		return
	}
	mode := os.Getenv("SDO_DISPATCHER_MODE")
	var request IncidentRequest
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	switch mode {
	case "success":
		result := completedResult(request.IncidentID)
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			os.Exit(2)
		}
	case "malformed":
		fmt.Fprint(os.Stdout, "not-json")
	case "nonzero":
		fmt.Fprint(os.Stderr, "responder failed")
		os.Exit(7)
	case "wait":
		time.Sleep(time.Second)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func helperDispatcher(mode string, timeout time.Duration) SubprocessDispatcher {
	return SubprocessDispatcher{
		Argv: []string{os.Args[0], "-test.run=TestSubprocessDispatcherHelper"}, Timeout: timeout,
		Env: []string{"SDO_DISPATCHER_HELPER=1", "SDO_DISPATCHER_MODE=" + mode},
	}
}

func goldenRequest(t *testing.T) IncidentRequest {
	t.Helper()
	content, err := os.ReadFile("../../../tests/fixtures/sdo/contracts/incident_request.json")
	if err != nil {
		t.Fatalf("read request fixture: %v", err)
	}
	var request IncidentRequest
	if err := json.Unmarshal(content, &request); err != nil {
		t.Fatalf("decode request fixture: %v", err)
	}
	return request
}
