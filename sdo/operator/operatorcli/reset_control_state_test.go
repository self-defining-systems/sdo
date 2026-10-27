package operatorcli

import "testing"

func TestParseResetControlStateLaunch(t *testing.T) {
	cfg, err := ParseResetControlStateLaunch([]string{"reset-control-state", "--control-namespace", "app-sdo", "--kubeconfig", "/lane/config", "--timeout", "5s"})
	if err != nil || cfg.ControlNamespace != "app-sdo" || cfg.Kubeconfig != "/lane/config" {
		t.Fatalf("config: %+v %v", cfg, err)
	}
	if cfg.WorkersOnly {
		t.Fatal("workers-only must be opt-in")
	}
	workers, err := ParseResetControlStateLaunch([]string{"reset-control-state", "--control-namespace", "app-sdo", "--workers-only"})
	if err != nil || !workers.WorkersOnly {
		t.Fatalf("workers-only: %+v %v", workers, err)
	}
	for _, args := range [][]string{{"reset-control-state"}, {"reset-control-state", "--control-namespace", "app-sdo", "--timeout", "0s"}, {"reset-control-state", "--control-namespace", "app-sdo", "extra"}} {
		if _, err := ParseResetControlStateLaunch(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
