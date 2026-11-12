package apihistory

import (
	"errors"
	"testing"
)

func openTestStore(t *testing.T, limits Limits) (*Store, string) {
	t.Helper()
	dir := t.TempDir() + "/history"
	if err := BeginGeneration(dir); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir, "incident", limits)
	if err != nil {
		t.Fatal(err)
	}
	return store, dir
}

func TestAdmitRefusesAClosedStore(t *testing.T) {
	store, _ := openTestStore(t, Limits{})
	if err := store.Admit(); err != nil {
		t.Fatalf("open store refused admission: %v", err)
	}
	store.Close()
	if err := store.Admit(); !errors.Is(err, ErrRecordingUnavailable) {
		t.Fatalf("closed store admitted a write: %v", err)
	}
}

func TestAdmitRefusesAfterTheWriteReservationIsSpent(t *testing.T) {
	store, _ := openTestStore(t, Limits{})
	defer store.Close()
	for i := 0; i < MaxAdmittedWrites; i++ {
		if err := store.Admit(); err != nil {
			t.Fatalf("admission %d refused: %v", i, err)
		}
	}
	if err := store.Admit(); !errors.Is(err, ErrRecordingUnavailable) {
		t.Fatalf("admitted beyond the reserved envelope budget: %v", err)
	}
}
