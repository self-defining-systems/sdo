package broker

import (
	"reflect"
	"testing"
)

func TestPartitionPathsSplitsSDOFromScratch(t *testing.T) {
	got := partitionPaths([]string{"b.go", ".sdo/playbooks/x.md", ".sdoc/file", "a.go", ".sdo/deployment/plan.yaml"})
	if want := []string{".sdo/deployment/plan.yaml", ".sdo/playbooks/x.md"}; !reflect.DeepEqual(got.SDO, want) {
		t.Fatalf("SDO = %v", got.SDO)
	}
	if want := []string{".sdoc/file", "a.go", "b.go"}; !reflect.DeepEqual(got.Scratch, want) {
		t.Fatalf("Scratch = %v (.sdoc is an app path, not memory)", got.Scratch)
	}
}
