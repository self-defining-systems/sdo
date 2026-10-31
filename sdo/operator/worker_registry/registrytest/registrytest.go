// Package registrytest gives tests the built-in agent registrations, read from
// the same manifests the controller image ships, so a test never restates a
// task type or message URL the manifests already define.
package registrytest

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/worker_registry"
)

// Directory is the checked-in directory of built-in agent manifests.
func Directory() string {
	directory, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		candidate := filepath.Join(directory, "sdo", "agents", "registrations")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			panic("cannot locate built-in agent registrations from test working directory")
		}
		directory = parent
	}
}

// BuiltIn loads the built-in registrations.
func BuiltIn(t testing.TB) *worker_registry.Registry {
	t.Helper()
	registry, err := worker_registry.LoadDirectory(Directory())
	if err != nil {
		t.Fatalf("load built-in agent registrations: %v", err)
	}
	return registry
}

// TaskType is the task type the built-in registrations declare for request.
func TaskType(t testing.TB, agentID string, request proto.Message) string {
	t.Helper()
	taskType, err := BuiltIn(t).TaskType(agentID, request)
	if err != nil {
		t.Fatalf("built-in task type: %v", err)
	}
	return taskType
}
