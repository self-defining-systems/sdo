package workloads

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "workload-health" {
		t.Fatal("unexpected detector ID")
	}
}
