package runtime

import (
	"testing"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
)

// Request-metrics findings are shadow clues by default: they never open an
// incident until detection.request_metrics_shadow is turned off, and even then
// only the error rule opens, never a not-instrumented note.
func TestRequestMetricsFindingsOpenOnlyOutOfShadowMode(t *testing.T) {
	failing := sdk.Finding{DetectorID: memory.RequestMetricsDetectorID, RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive}
	note := sdk.Finding{DetectorID: memory.RequestMetricsDetectorID, RuleID: requestmetrics.RuleNotInstrumented, Status: sdk.FindingActive}
	if !settings.Default().Detection.RequestMetricsShadow {
		t.Fatal("shadow must default on")
	}
	c := openersController(t, &recordingDispatcher{})
	if c.config.RequestMetricsOpens || c.opens(failing) || c.opens(note) {
		t.Fatal("shadow mode opened an incident")
	}
	if got := (ControllerConfig{}).WithSettings(settings.Default()); got.RequestMetricsOpens {
		t.Fatal("default settings must leave the detector in shadow mode")
	}
	off := settings.Default()
	off.Detection.RequestMetricsShadow = false
	if got := (ControllerConfig{}).WithSettings(off); !got.RequestMetricsOpens {
		t.Fatal("turning shadow off must let the detector open")
	}
	c.config.RequestMetricsOpens = true
	if !c.opens(failing) || c.opens(note) {
		t.Fatalf("out of shadow: error opens %v, note opens %v", c.opens(failing), c.opens(note))
	}
}
