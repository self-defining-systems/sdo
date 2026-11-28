package operatorcli

import (
	"testing"
)

// The registry host is the trusted push target of images Go builds from the tree. An application that ships its
// own deployment with prebuilt images needs none, so the flag is optional; when given it reaches the install.
func TestHostRegistryHostIsOptional(t *testing.T) {
	var bare []string
	for i := 0; i < len(hostArgs()); i++ {
		if arg := hostArgs()[i]; arg == "--registry-host" {
			i++
		} else {
			bare = append(bare, arg)
		}
	}
	// An application whose images are all prebuilt needs no registry.
	if launch, err := ParseHostLaunchEnv(bare, journeysRequired[:0]); err != nil || launch.Install.RegistryHost != "" {
		t.Fatalf("no registry host: %q, %v", launch.Install.RegistryHost, err)
	}
	launch, err := ParseHostLaunchEnv(append(bare, "--registry-host", "registry.local:5000"), journeysRequired[:0])
	if err != nil || launch.Install.RegistryHost != "registry.local:5000" {
		t.Fatalf("registry host = %q, %v", launch.Install.RegistryHost, err)
	}
}
