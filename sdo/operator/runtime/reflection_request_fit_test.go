package runtime

import (
	"testing"

	"sdo.dev/operator/worker_registry"
)

func TestTransportLimitErrorIsNonRetryable(t *testing.T) {
	var err error = &worker_registry.RequestTooLargeError{}
	if terminal, ok := err.(interface{ NonRetryable() bool }); !ok || !terminal.NonRetryable() {
		t.Fatal("a request that does not fit must not be retried")
	}
}

