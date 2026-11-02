package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/checktransport"
)

// The real client removes its request on cancellation. That must not terminate
// the incident's server and strand every subsequent check until its timeout.
func TestReflectionCheckSurvivesClientCancellation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "tools")
	started, release := make(chan struct{}), make(chan struct{})
	calls := 0
	server := SessionToolServer{Directory: directory, PollInterval: time.Millisecond, Handle: func(ctx context.Context, request ToolRequest) ToolResponse {
		calls++
		if calls == 1 {
			close(started)
			<-release
		}
		return ToolResponse{MemoryCheckReport: []byte(`{"actor":"responder"}`)}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	clientCtx, stopClient := context.WithCancel(ctx)
	checked := make(chan error, 1)
	decision := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic"}
	go func() { _, err := checktransport.Check(clientCtx, directory, decision); checked <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopClient()
	if err := <-checked; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled check: %v", err)
	}
	close(release)
	report, err := checktransport.Check(ctx, directory, decision)
	cancel()
	serverErr := <-done
	if err != nil || report.GetActor() != "responder" || serverErr != nil {
		t.Fatalf("next check=%v, error=%v, server=%v", report, err, serverErr)
	}
}
