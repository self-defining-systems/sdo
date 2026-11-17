package kubereads

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type stalledExec struct{}

func (stalledExec) Stream(ctx context.Context, _ sdk.ObjectRef, _ string, _ []string, _ string, _ io.Writer) error {
	<-ctx.Done()
	return errors.New("stream reset")
}

// An exec that outlives its read budget says so: a slow command is a different
// fix than a broken transport.
func TestRunCommandNamesAnExpiredReadBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-0"}
	_, err := RunCommand(ctx, stalledExec{}, "", program.Command{Pod: pod, Container: "store", Args: []string{"true"}})
	if err == nil || !strings.HasPrefix(err.Error(), "exec transport failed") || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("RunCommand error = %v; want the transport class and the context cause", err)
	}
}
