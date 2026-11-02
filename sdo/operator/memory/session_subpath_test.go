package memory

import (
	"os"
	"testing"
)

func TestIncidentSessionSubPathMatchesPreparedSessionWithoutStorage(t *testing.T) {
	root := t.TempDir()
	for _, prefix := range []string{"", "tenant/workspace"} {
		sub, err := IncidentSessionSubPath(prefix, "incident-A")
		if err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("path resolution touched session storage: %v %v", entries, err)
		}
		prepared, err := PrepareIncidentSession(root, prefix, "incident-A")
		if err != nil || prepared != sub {
			t.Fatalf("resolved=%q prepared=%q err=%v", sub, prepared, err)
		}
		if err := os.RemoveAll(root + "/.sdo-runtime"); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := IncidentSessionSubPath("tenant", "incident-A")
	b, _ := IncidentSessionSubPath("tenant", "incident-B")
	if a == b {
		t.Fatal("separate incidents share a session subpath")
	}
}

func TestIncidentSessionSubPathRejectsInvalidIdentityAndPrefix(t *testing.T) {
	for _, tc := range []struct{ prefix, id string }{{"", ""}, {"", " "}, {"../other", "incident"}, {"/absolute", "incident"}, {"tenant/../other", "incident"}, {"tenant\\other", "incident"}, {".", "incident"}} {
		if _, err := IncidentSessionSubPath(tc.prefix, tc.id); err == nil {
			t.Errorf("accepted invalid session ownership: %+v", tc)
		}
	}
}
