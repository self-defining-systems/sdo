package core

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"testing"
	"time"
)

type logValidationDetector struct {
	validationDetector
	queries []logs.Query
}

func (d logValidationDetector) LogQueries() []logs.Query { return d.queries }
func TestLogDeclarationsValidateAtRegistration(t *testing.T) {
	d := logValidationDetector{validationDetector: validationDetector{spec: sdk.DetectorSpec{ID: "logs", Interval: time.Second, Watches: []sdk.WatchKind{logs.Watch}}}, queries: []logs.Query{{ID: "q", Patterns: []logs.Pattern{{ID: "p", RE2: "error"}}}}}
	if err := ValidateDetectors([]sdk.Detector{d}); err != nil {
		t.Fatal(err)
	}
	d.queries[0].Patterns[0].RE2 = "["
	if err := ValidateDetectors([]sdk.Detector{d}); err == nil {
		t.Fatal("registration accepted invalid RE2")
	}
}
