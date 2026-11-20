package processio

import (
	"context"
	"testing"
)

func TestTerminationStopMayBeCalledTwice(t *testing.T) {
	ctx, _, stop := Termination(context.Background())
	stop()
	stop()
	if ctx.Err() == nil {
		t.Fatal("stop must end the context")
	}
}
