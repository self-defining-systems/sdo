package apihistory

import (
	"testing"
	"time"
)

func TestMutationGateFencesOnlyAfterPendingWritesResolve(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write, err := gate.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if observation, _ := gate.TryObserve(); observation != nil {
		observation.Close()
		t.Fatal("closure acquired during write")
	}
	if err := write.Finish(false); err != nil {
		t.Fatal(err)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("observe: %v", err)
	}
	at := time.Unix(42, 0).UTC()
	if err := observation.Fence(at); err != nil {
		t.Fatal(err)
	}
	observation.Close()
	if write, err := gate.Begin(); err == nil {
		write.Finish(false)
		t.Fatal("post-closure mutation admitted")
	}
	if got, err := gate.Verified(); err != nil || got == nil || !got.VerifiedAt.Equal(at) {
		t.Fatalf("warning: %+v %v", got, err)
	}
}

func TestMutationGateUncertainReceiptBlocksClosure(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write, err := gate.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Finish(true); err != nil {
		t.Fatal(err)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("observe: %v", err)
	}
	defer observation.Close()
	if !observation.Pending {
		t.Fatal("uncertain write was treated as resolved")
	}
}
