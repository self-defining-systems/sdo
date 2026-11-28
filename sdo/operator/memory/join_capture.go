package memory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"sdo.dev/operator/sdk/sdktest"
)

// JoinCaptureContext is what Go knew when it took a join capture; the
// format lives with the other capture formats in sdktest.
type JoinCaptureContext = sdktest.JoinCaptureContext

func joinCaptureContextFile(n int) string { return "join-" + strconv.Itoa(n) + "-context.json" }

// CaptureJoinContext stores, once, what Go knew at the nth join capture. Its
// name is outside open@*, so replays never read it as a snapshot.
func CaptureJoinContext(repositoryMount, incidentID string, n int, record JoinCaptureContext) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

	if n < 1 {
		return fmt.Errorf("invalid join capture %d", n)
	}
	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return CreateCaptureFile(repositoryMount, filepath.Join(dir, joinCaptureContextFile(n)), data)
}

// ReadJoinCaptureContext reads what Go knew at the nth join capture of an
// incident capture directory.
func ReadJoinCaptureContext(dir string, n int) (JoinCaptureContext, error) {
	var record JoinCaptureContext
	data, err := os.ReadFile(filepath.Join(dir, joinCaptureContextFile(n)))
	if err != nil {
		return record, err
	}
	return record, json.Unmarshal(data, &record)
}
