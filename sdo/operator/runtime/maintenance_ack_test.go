package runtime

import (
	"context"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

// The controller confirms a pause by recording the generation it stopped
// under, in a ConfigMap of its own, so the declaring side never races a write
// of the maintenance ConfigMap.
func TestMaintenanceAckerRecordsTheLatestGeneration(t *testing.T) {
	acker := MaintenanceAcker{Client: fake.NewSimpleClientset(), Namespace: "sdo-system"}
	ctx := context.Background()
	for _, generation := range []string{"upstream-advance-paused-1", "upstream-advance-paused-2"} {
		if err := acker.Acknowledge(ctx, generation); err != nil {
			t.Fatal(err)
		}
		got, err := acker.Acknowledged(ctx)
		if err != nil || got != generation {
			t.Fatalf("acknowledged %q, %v; want %q", got, err, generation)
		}
	}
	if got, err := (MaintenanceAcker{Client: fake.NewSimpleClientset(), Namespace: "sdo-system"}).Acknowledged(ctx); err != nil || got != "" {
		t.Fatalf("an unacknowledged install reports %q, %v", got, err)
	}
	if err := acker.Acknowledge(ctx, ""); err == nil {
		t.Fatal("an empty generation was acknowledged")
	}
}

// Draining waits for every pass that holds the observation lock, so an
// evaluation that began before the pause finishes (and may open its incident)
// before the pause is acknowledged.
func TestDrainObservationWaitsForAnInFlightPass(t *testing.T) {
	c := &Controller{}
	release := c.lockObservation("in_flight")
	finished := false
	drained := make(chan bool)
	go func() {
		c.DrainObservation()
		drained <- finished
	}()
	finished = true
	release()
	if !<-drained {
		t.Fatal("DrainObservation returned before the in-flight pass finished")
	}
}
