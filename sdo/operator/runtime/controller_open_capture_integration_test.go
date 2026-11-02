//go:build integration

package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"


	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// awaitRoutingSweep waits for the background open@detect routing sweep of an
// incident to finish (its marker is the last file it writes).
func awaitRoutingSweep(t *testing.T, root, incidentID string) {
	t.Helper()
	dir, err := memory.IncidentCorpusDir(root, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "open@detect-routing.done")); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("routing sweep never completed")
}

func TestOpenDetectRoutingSweepNeverDelaysDispatch(t *testing.T) {
	root := t.TempDir()
	prober := newGatedProber()
	t.Cleanup(prober.Release)
	controller, dispatcher := openCaptureController(t, root, func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(s), Prober: prober}
	})
	openIncident(t, controller)
	// The responder is dispatched while the sweep is still blocked on its first probe.
	request := awaitRequest(t, dispatcher.requests)
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "open@detect.json")); err != nil {
		t.Fatalf("open@detect snapshot must exist when the responder starts: %v", err)
	}
	marker := filepath.Join(dir, "open@detect-routing.done")
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("routing sweep finished although every probe was blocked")
	}
	prober.Release()
	awaitRoutingSweep(t, root, request.IncidentID)
}

