package operatorcli

import (
	"strings"
	"testing"
)

// Without a provided plan the deployer agent builds the application's images and pushes them only to the trusted
// registry, so a launch that names none can never deploy: refuse it before any resource or model call, naming the
// flag, instead of letting the deployer task fail later as "source deployment request is incomplete".
func TestHostWithoutAPlanNeedsTheRegistryHost(t *testing.T) {
	var bare []string
	for i := 0; i < len(hostArgs()); i++ {
		if arg := hostArgs()[i]; arg == "--registry-host" {
			i++
		} else {
			bare = append(bare, arg)
		}
	}
	_, err := ParseHostLaunchEnv(bare, journeysRequired[:0])
	if err == nil || !strings.Contains(err.Error(), "--registry-host") || !strings.Contains(err.Error(), "--deployment-plan") {
		t.Fatalf("err = %v, want a refusal naming --registry-host and --deployment-plan", err)
	}
	if _, err := ParseHostLaunchEnv(append(bare, "--deployment-plan", providedPlanJSON), journeysRequired[:0]); err != nil {
		t.Fatalf("a provided plan needs no registry: %v", err)
	}
	launch, err := ParseHostLaunchEnv(append(bare, "--registry-host", "registry.local:5000"), journeysRequired[:0])
	if err != nil || launch.Install.RegistryHost != "registry.local:5000" {
		t.Fatalf("registry host = %q, %v", launch.Install.RegistryHost, err)
	}
}
