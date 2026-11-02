package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

// Accepted objects read over REST must keep integers as int64: unstructured
// accessors such as GetGeneration silently return zero for float64 values,
// which would make every generation comparison look settled.
func TestDecodeAcceptedObjectKeepsIntegers(t *testing.T) {
	object, err := decodeAcceptedObject([]byte(`{"metadata":{"name":"search","generation":2},"spec":{"replicas":3},"status":{"observedGeneration":1}}`), sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if object.GetGeneration() != 2 {
		t.Fatalf("generation = %d, want 2", object.GetGeneration())
	}
	if acceptanceSettled(object) {
		t.Fatal("observedGeneration 1 behind generation 2 counted as settled")
	}
}
