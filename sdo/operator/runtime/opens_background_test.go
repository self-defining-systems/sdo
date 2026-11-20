package runtime

import (
	"testing"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
)

// Failing background jobs are not failing user traffic: the finding stays a
// clue even though it is a request-metrics error.
func TestRequestMetricsErrorOfBackgroundWorkIsAClue(t *testing.T) {
	job := failingOperation("requestmetrics/errors/undeclared/worker/mailers")
	job.DetectorID = memory.RequestMetricsDetectorID
	job.Metadata = map[string]any{"background": true}
	c := openersController(t, &recordingDispatcher{})
	c.config = c.config.WithSettings(settings.Default())
	if c.opens(job) {
		t.Fatal("a failing background job opened an incident")
	}
}
