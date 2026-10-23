package operatorcli

import "testing"

func TestParseStopLaunch(t *testing.T) {
	config, err := ParseStopLaunch([]string{"stop", "--control-namespace", "demo-sdo", "--kubeconfig", "/lane/config", "--timeout", "5s", "--inspect"})
	if err != nil || config.ControlNamespace != "demo-sdo" || config.Kubeconfig != "/lane/config" || !config.Inspect {
		t.Fatalf("stop config: %+v %v", config, err)
	}
	if _, err := ParseStopLaunch([]string{"stop", "--timeout", "0s"}); err == nil {
		t.Fatal("unbounded stop accepted")
	}
	if _, err := ParseStopLaunch([]string{"stop", "unexpected"}); err == nil {
		t.Fatal("positional argument accepted")
	}
	config, err = ParseStopLaunch([]string{"stop"})
	if err != nil || config.ControlNamespace != "" {
		t.Fatal("cluster-wide stop unavailable")
	}
}
