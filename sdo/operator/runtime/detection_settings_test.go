package runtime

import (
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// The detection section is the one source of the controller's stability window; NewController has no default.
func TestControllerTakesItsStabilityWindowFromTheDetectionSection(t *testing.T) {
	policy := settings.Default()
	policy.Detection.StabilityWindow = settings.Duration(40 * time.Second)
	if got := (ControllerConfig{}).WithSettings(policy).ClosureStabilityWindow; got != 40*time.Second {
		t.Fatalf("ClosureStabilityWindow = %s, want 40s", got)
	}
	config := testControllerConfig()
	config.ClosureStabilityWindow = 0
	if _, err := NewController(config, nil, staticProvider{}, &fixedResultDispatcher{}, time.Unix(0, 0)); err == nil {
		t.Fatal("a controller without a stability window must be refused, not defaulted")
	}
}

// settings.Default() cannot import the SDK, so the production hold is written in both; they must stay equal.
func TestProductionSelfHealHoldIsTheSDKDefault(t *testing.T) {
	if time.Duration(settings.Default().Detection.SelfHealHold) != sdk.DefaultSelfHealHold {
		t.Fatalf("settings default %s differs from sdk.DefaultSelfHealHold %s", settings.Default().Detection.SelfHealHold, sdk.DefaultSelfHealHold)
	}
}
