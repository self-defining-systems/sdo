package deployment

import (
	"slices"
	"testing"
)

func TestAcceptedHealthProbeReceivesMetricsURL(t *testing.T) {
	probe := AcceptedDetectorProbe{MetricsURL: "http://metrics:9090"}
	args := probe.evaluationArgs("/accepted", "app")
	index := slices.Index(args, "--metrics-url")
	if index < 0 || args[index+1] != probe.MetricsURL {
		t.Fatal(args)
	}
	if slices.Contains((AcceptedDetectorProbe{}).evaluationArgs("/accepted", "app"), "--metrics-url") {
		t.Fatal("unconfigured metrics")
	}
}
