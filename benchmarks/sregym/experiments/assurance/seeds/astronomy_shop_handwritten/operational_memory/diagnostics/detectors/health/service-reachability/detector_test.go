package servicereachability

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "service-reachability" {
		t.Fatal("unexpected detector ID")
	}
}
