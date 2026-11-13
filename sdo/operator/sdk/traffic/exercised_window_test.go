package traffic

import "testing"

func TestWindowCountsExercisedExistingRecordsSamples(t *testing.T) {
	window := Window{Scenarios: []ScenarioObservations{{Samples: []Sample{
		{ExistingRecords: ExistingRecordsOwnOnly}, {ExistingRecords: ExistingRecordsEmpty}, {ExistingRecords: ExistingRecordsExercised}, {},
	}}}}
	if got := window.ExistingRecordsExercised(); got != 1 {
		t.Fatalf("exercised = %d", got)
	}
}
