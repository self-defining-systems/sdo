package operatorcli

import "testing"

func TestParseSyncExistingRequiresExplicitAcceptedRepository(t *testing.T) {
	config, err := ParseSyncExistingLaunch([]string{"sync-existing", "--repository", "/accepted/demo", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--kubeconfig", "/tmp/kubeconfig"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Repository != "/accepted/demo" || config.ControlNamespace != "sdo" || config.RepositoryPVC != "repo" || config.Kubeconfig != "/tmp/kubeconfig" {
		t.Fatalf("lost sync contract: %+v", config)
	}
	if _, err := ParseSyncExistingLaunch([]string{"sync-existing", "--repository", "relative", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test"}); err == nil {
		t.Fatal("relative target accepted")
	}
}

func TestParseMaintenanceRequiresExplicitGeneration(t *testing.T) {
	config, err := ParseMaintenanceLaunch([]string{"maintenance", "--control-namespace", "sdo", "--paused=true", "--generation", "run-2", "--kubeconfig", "/tmp/kubeconfig"})
	if err != nil {
		t.Fatal(err)
	}
	if !config.Paused || config.Generation != "run-2" || config.ControlNamespace != "sdo" {
		t.Fatalf("lost maintenance contract: %+v", config)
	}
	if _, err := ParseMaintenanceLaunch([]string{"maintenance", "--control-namespace", "sdo", "--paused=true"}); err == nil {
		t.Fatal("missing generation accepted")
	}
}

func TestParseMaintenanceObservationBarrier(t *testing.T) {
	config, err := ParseMaintenanceLaunch([]string{"maintenance", "--control-namespace", "sdo", "--paused", "--preserve-baseline", "--generation", "barrier"})
	if err != nil || !config.Paused || !config.PreserveBaseline { t.Fatalf("lost observation barrier: %+v %v", config, err) }
}
