package settings_test

import (
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
)

// A from-scratch onboarding judge timed out at 900 s with no writes in every
// measured run; the default must leave headroom over that.
func TestDefaultLifecycleTimeoutExceedsMeasuredFirstOnboardingSession(t *testing.T) {
	if got := time.Duration(settings.Default().Agent.LifecycleTimeout); got < 3600*time.Second {
		t.Fatalf("agent.lifecycle_timeout default = %s, want at least 1h", got)
	}
}
