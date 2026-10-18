package operatorcli

import "testing"

func TestParseIncidentEvidenceLaunch(t *testing.T) {
	cfg, err := ParseIncidentEvidenceLaunch([]string{"incident-evidence", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--incident-id", "incident-1", "--kubeconfig", "/tmp/kube"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IncidentID != "incident-1" || cfg.Sync.ControlNamespace != "sdo" || cfg.Sync.RepositoryPVC != "repo" {
		t.Fatalf("lost evidence identity: %+v", cfg)
	}
	if _, err := ParseIncidentEvidenceLaunch([]string{"incident-evidence", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test"}); err == nil {
		t.Fatal("missing incident accepted")
	}
}
