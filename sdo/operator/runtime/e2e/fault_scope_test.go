//go:build integration && controller_e2e

package e2e

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestMemoryHostFaultIsConfinedToOwnedSandbox(t *testing.T) {
	proc := t.TempDir()
	files := map[string]string{
		"100/cmdline":           "bwrap\x00",
		"100/task/100/children": "101 ",
		// Go can start the host from any OS thread, not just the main thread.
		"100/task/110/children": "102 ",
		"101/cmdline":           "/usr/local/bin/sdo-operator\x00controller\x00",
		"101/task/101/children": "103 104 ",
		"102/cmdline":           "/cache/old/sdo-memory-host\x00",
		"103/cmdline":           "/cache/current/sdo-memory-host\x00",
		"104/cmdline":           "/cache/current/sdo-memory-host-other\x00",
		// A second World (or a real cluster lane) is outside this Runtime.
		"200/cmdline":           "bwrap\x00",
		"200/task/200/children": "201 ",
		"201/cmdline":           "/cache/current/sdo-memory-host\x00",
	}
	for name, contents := range files {
		path := filepath.Join(proc, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got := descendantPIDs(proc, []int{100}, "sdo-memory-host")
	sort.Ints(got)
	if want := []int{102, 103}; !reflect.DeepEqual(got, want) {
		t.Fatalf("owned memory hosts = %v, want %v", got, want)
	}
	if got := descendantPIDs(proc, nil, "sdo-memory-host"); len(got) != 0 {
		t.Fatalf("no owned sandboxes must select no targets, got %v", got)
	}
}
