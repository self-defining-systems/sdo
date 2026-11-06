package broker

import (
	"reflect"
	"strings"
	"testing"
)

func TestPartitionPathsSplitsSDOFromScratch(t *testing.T) {
	got := partitionPaths([]string{"b.go", ".sdo/playbooks/x.md", ".sdoc/file", "a.go", ".sdo/deployment/values/v.yaml"})
	if want := []string{".sdo/deployment/values/v.yaml", ".sdo/playbooks/x.md"}; !reflect.DeepEqual(got.SDO, want) {
		t.Fatalf("SDO = %v", got.SDO)
	}
	if want := []string{".sdoc/file", "a.go", "b.go"}; !reflect.DeepEqual(got.Scratch, want) {
		t.Fatalf("Scratch = %v (.sdoc is an app path, not memory)", got.Scratch)
	}
}

func TestUncapturedMessageIsEmptyOnlyWhenConsistent(t *testing.T) {
	if uncapturedMessage(nil) != "" {
		t.Fatal("no uncaptured files must give no feedback")
	}
	var many []string
	for i := 0; i < 25; i++ {
		many = append(many, "f"+string(rune('a'+i))+".go")
	}
	msg := uncapturedMessage(many)
	for _, want := range []string{UncapturedRule, "25 app file", "and 5 more", "patch create"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q lacks %q", msg, want)
		}
	}
}
