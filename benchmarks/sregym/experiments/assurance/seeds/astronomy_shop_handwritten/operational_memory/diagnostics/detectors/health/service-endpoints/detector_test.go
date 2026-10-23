package serviceendpoints

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "service-endpoints" {
		t.Fatal("unexpected detector ID")
	}
}
