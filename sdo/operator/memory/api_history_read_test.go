package memory

import (
	"os"
	"testing"
	"time"
)

func TestMissingAPIHistoryReadsDoNotCreateRecordingEvidence(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		root := t.TempDir()
		start := time.Unix(1, 0)
		var err error
		if recovery {
			_, err = IncidentAPIRecoveryReceipts(root, "synthetic", nil, start, start.Add(time.Second))
		} else {
			_, err = IncidentAPIRepairReceipts(root, "synthetic", nil, start, start.Add(time.Second))
		}
		if err == nil {
			t.Fatal("missing recording was accepted as complete")
		}
		entries, readErr := os.ReadDir(root)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatal("read manufactured session/generation evidence for a proxy that never ran")
		}
	}
}
