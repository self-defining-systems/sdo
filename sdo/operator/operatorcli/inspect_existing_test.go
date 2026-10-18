package operatorcli

import "testing"

func TestInspectExistingParsesTrustedIdentityWithoutLocalCheckout(t *testing.T) {
	config, err := ParseInspectExistingLaunch([]string{"inspect-existing", "--control-namespace", "demo-sdo", "--namespace", "demo", "--application", "app", "--repository-pvc", "repository", "--controller-image", "controller:test", "--kubeconfig", "/tmp/kubeconfig"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Inspect.ControlNamespace != "demo-sdo" || config.Inspect.Namespace != "demo" || config.Inspect.Application != "app" || config.Inspect.RepositoryPVC != "repository" || config.Inspect.ControllerImage != "controller:test" || config.Kubeconfig != "/tmp/kubeconfig" {
		t.Fatalf("lost inspect identity: %+v", config)
	}
	if _, err := ParseInspectExistingLaunch([]string{"inspect-existing", "--control-namespace", "demo-sdo", "--namespace", "demo", "--application", "app", "--repository-pvc", "repository", "--controller-image", "controller:test", "--kubeconfig", "relative"}); err == nil {
		t.Fatal("relative kubeconfig accepted")
	}
}
