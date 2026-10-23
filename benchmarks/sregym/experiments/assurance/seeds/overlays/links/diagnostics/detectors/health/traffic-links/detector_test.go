package synthetic

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "traffic-links" {
		t.Fatal("unexpected detector ID")
	}
}
