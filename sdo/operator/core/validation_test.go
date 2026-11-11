package core

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

func clientLinks(t *testing.T, id string, count int) sdk.Detector {
	t.Helper()
	links := make([]vantage.ClientLink, count)
	for i := range links {
		link := traffic.Link{From: "api", To: fmt.Sprintf("%s-%d", id, i), Port: 80}
		links[i] = vantage.ClientLink{ID: id + "." + link.ID(), From: podread.Target{Workload: "Deployment/api"}, Link: link}
	}
	detector, err := vantage.NewClientLinksDetector(sdk.DetectorSpec{ID: id, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}, links, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return detector
}

// The controller merges every detector's vantage probes and refuses more than
// vantage.MaxProbes, so memory whose detectors are each within the budget but
// together exceed it must be refused before it is accepted.
func TestValidateDetectorsBoundsVantageProbesAcrossDetectors(t *testing.T) {
	half := vantage.MaxProbes/2 + 1
	if err := ValidateDetectors([]sdk.Detector{clientLinks(t, "one", half)}); err != nil {
		t.Fatalf("one detector within the budget: %v", err)
	}
	err := ValidateDetectors([]sdk.Detector{clientLinks(t, "one", half), clientLinks(t, "two", half)})
	if err == nil || !strings.Contains(err.Error(), "vantage probes exceed") {
		t.Fatalf("err = %v, want the merged vantage budget refused", err)
	}
}
