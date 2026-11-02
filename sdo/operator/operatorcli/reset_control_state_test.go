package operatorcli

import "testing"

func TestParseResetControlStateLaunch(t *testing.T) {
	cfg, err := ParseResetControlStateLaunch([]string{"reset-control-state", "--control-namespace", "app-sdo", "--kubeconfig", "/lane/config", "--timeout", "5s"})
	if err != nil || cfg.ControlNamespace != "app-sdo" || cfg.Kubeconfig != "/lane/config" {
		t.Fatalf("config: %+v %v", cfg, err)
	}
	for _, args := range [][]string{{"reset-control-state"}, {"reset-control-state", "--control-namespace", "app-sdo", "--timeout", "0s"}, {"reset-control-state", "--control-namespace", "app-sdo", "extra"}} {
		if _, err := ParseResetControlStateLaunch(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
