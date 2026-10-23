package synthetic

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "traffic-health" {
		t.Fatal("unexpected detector ID")
	}
}
