package deployment

import (
	"regexp"

	"sdo.dev/operator/rendered"
)

// WorkloadObservation is the live state of one workload, read independently
// of anything a plan claims.
type WorkloadObservation struct {
	ContainerImages []rendered.DeployedImage
	ResourceID      string
	Revision        string
	Images          []string
	Ready           bool
}

var digestReference = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)

func sameNamedImages(expected, actual []rendered.DeployedImage) bool {
	if len(expected) != len(actual) {
		return false
	}
	counts := map[rendered.DeployedImage]int{}
	for _, image := range expected {
		image.Inputs = rendered.ImageInputs{} // observation verifies identities, not build declarations
		counts[image]++
	}
	for _, image := range actual {
		image.Inputs = rendered.ImageInputs{}
		counts[image]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
	}
	for _, value := range counts {
		if value != 0 {
			return false
		}
	}
	return true
}
