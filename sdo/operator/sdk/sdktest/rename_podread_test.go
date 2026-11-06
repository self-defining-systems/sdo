package sdktest

import (
	"testing"

	"sdo.dev/operator/sdk/podread"
)

// A finding on a reading names its Pod, so rename replay renames the Pod and
// container and keeps the recorded output.
func TestRenameKeepsPodReadsResolvable(t *testing.T) {
	m := newRenameMapping()
	source := map[string]podread.Result{"q": {Readings: []podread.Reading{{Pod: "store-0", Container: "store", Stdout: "store-0 value"}}}}
	got := renamePodReads(source, m)["q"].Readings[0]
	if got.Pod != m.value("store-0") || got.Pod == "store-0" || got.Container != m.value("store") || got.Stdout != "store-0 value" {
		t.Fatalf("renamed reading = %+v", got)
	}
	if source["q"].Readings[0].Pod != "store-0" {
		t.Fatal("rename mutated the source snapshot")
	}
}
