package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestCodePathTypedLaunchRoundTrip(t *testing.T) {
	config := &contracts.ControllerConfig{CodePath: &contracts.CodePathConfig{Name: "shell", Overrides: &contracts.CodePathOverrides{Coverage: "none"}}}
	args := controllerConfigToArgs(config)
	found := false
	for i, arg := range args {
		if arg == "--code-path" && i+1 < len(args) && args[i+1] == "shell" {
			found = true
		}
	}
	if !found {
		t.Fatalf("shell selection dropped: %v", args)
	}
	resolved, err := codePathFromContract(config.GetCodePath())
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Selection.Name != "shell" || resolved.Selection.Coverage != "none" {
		t.Fatalf("shell selection changed: %#v", resolved.Selection)
	}
}
