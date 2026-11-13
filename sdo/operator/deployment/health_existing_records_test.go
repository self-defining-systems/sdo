package deployment

import "testing"

func TestHealthProbeDecodesExistingRecordsExercised(t *testing.T) {
	got, err := decodeHealthVerdict([]byte(`{"status":"clear","existing_records_exercised":2}`), nil)
	if err != nil || got.ExistingRecordsExercised != 2 {
		t.Fatalf("got %+v, %v", got, err)
	}
}
