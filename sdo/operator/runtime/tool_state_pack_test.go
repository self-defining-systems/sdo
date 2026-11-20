package runtime

import (
	"bytes"
	"testing"

	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// A responder that repairs many resources keeps each application's pre-images and patches in the attempt
// journal. The journal must stay far below the ConfigMap budget however many applications it holds.
func TestPackedToolStateRoundTripsAndStaysWithinBudget(t *testing.T) {
	object := bytes.Repeat([]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"k":"v"}}},`), 12000)
	payload := append(append([]byte(`{"ApplicationEvidence":[`), object...), []byte(`{}]}`)...)
	if len(payload) < lifecycleio.MaxConfigMapBytes {
		t.Fatalf("fixture must exceed the budget to prove anything, got %d", len(payload))
	}
	packed, err := packToolState(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= lifecycleio.MaxConfigMapBytes/4 {
		t.Fatalf("packed journal is %d bytes", len(packed))
	}
	restored, err := unpackToolState(packed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatal("journal changed in the round trip")
	}
	if _, err := unpackToolState([]byte("not gzip")); err == nil {
		t.Fatal("a journal that is not packed must be refused")
	}
}
