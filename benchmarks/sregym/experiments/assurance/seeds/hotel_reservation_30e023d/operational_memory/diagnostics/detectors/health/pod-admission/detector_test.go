package podadmission

import "testing"

func TestRegistration(t *testing.T) {
	if New().Spec().ID != "pod-admission" {
		t.Fatal("unexpected detector ID")
	}
}
