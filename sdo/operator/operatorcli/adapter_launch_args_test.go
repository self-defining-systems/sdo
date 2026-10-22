package operatorcli

import (
	"encoding/json"
	"os"
	"testing"
)

// TestAdapterEmittedLaunchArgsParse feeds the exact argv the benchmark adapter
// emits (every option set; pinned by
// tests/unit/benchmarks/sregym/adapter/test_native_launch_args_contract.py) to
// the real flag parsers. A flag removed or renamed on only one side fails here.
func TestAdapterEmittedLaunchArgsParse(t *testing.T) {
	raw, err := os.ReadFile("../../../tests/fixtures/sdo/contracts/adapter_launch_args.json")
	if err != nil {
		t.Fatal(err)
	}
	var emitted map[string][]string
	if err := json.Unmarshal(raw, &emitted); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseInstallExistingLaunch(emitted["install-existing"]); err != nil {
		t.Fatalf("install-existing rejects the adapter's arguments: %v", err)
	}
	if _, err := ParseHostLaunch(emitted["host"]); err != nil {
		t.Fatalf("host rejects the adapter's arguments: %v", err)
	}
}
