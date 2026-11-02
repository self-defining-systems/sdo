package broker

import (
	"testing"
)

// A waited git command can still leave detached auto-maintenance writing into
// the repository. Every temp-repository fixture must opt out before any commit.
func TestTempRepositoryDisablesDetachedMaintenance(t *testing.T) {
	root := repository(t)
	for key, want := range map[string]string{
		"gc.auto":                "0",
		"maintenance.auto":       "false",
		"gc.autoDetach":          "false",
		"maintenance.autoDetach": "false",
	} {
		if got := git(t, root, "config", "--get", key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}
