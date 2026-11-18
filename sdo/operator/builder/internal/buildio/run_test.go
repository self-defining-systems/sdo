package buildio

import (
	"testing"
	"time"
)

func TestStopGraceIsBoundedBelowThePodGracePeriod(t *testing.T) {
	if StopGrace <= 5*time.Second || StopGrace >= 30*time.Second {
		t.Fatalf("StopGrace = %v; it must exceed the Lease release (5s) and fit the pod grace period", StopGrace)
	}
}
