package operatorcli

import "testing"

// The controller launch parses --watch-only into its typed config.
func TestControllerLaunchParsesWatchOnly(t *testing.T) {
	run, err := ParseControllerLaunch(controllerLaunchArgs("--frozen-memory", "--watch-only"))
	if err != nil {
		t.Fatal(err)
	}
	if !run.Controller.GetWatchOnly() || !run.Controller.GetFrozenMemory() {
		t.Fatalf("controller config watch_only=%v frozen_memory=%v", run.Controller.GetWatchOnly(), run.Controller.GetFrozenMemory())
	}
	existing, err := ParseInstallExistingLaunch(existingLaunchArgs("--frozen-memory", "--watch-only"))
	if err != nil {
		t.Fatal(err)
	}
	if !existing.Install.WatchOnly {
		t.Fatal("install-existing dropped --watch-only")
	}
}
