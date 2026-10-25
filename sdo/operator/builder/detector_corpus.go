package builder

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	programvalidation "sdo.dev/operator/memory/validate/program"
)

// generateDetectorCorpusTest evaluates the whole locator scope, independently
// of the label bindings used to prove an individual program's mitigation.
func generateDetectorCorpusTest(root string, programs []programvalidation.Spec) error {
	if len(programs) == 0 {
		return nil
	}
	var source strings.Builder
	source.WriteString(`package generated
import (
 "context"
 "path/filepath"
 "testing"
 "time"
 "sdo.dev/operator/sdk"
 "sdo.dev/operator/sdk/sdktest"
)
func TestProgramDetectorCorpusObservations(t *testing.T) {
 programIDs := map[string]bool{
`)
	for _, item := range programs {
		fmt.Fprintf(&source, "%q:true,\n", item.ID)
	}
	source.WriteString(` }
 paths, err := filepath.Glob(filepath.Join("..", "corpus", "*"))
 if err != nil { t.Fatal(err) }
 for _, path := range paths {
  corpus, err := sdktest.LoadCorpusCase(path)
  if err != nil { t.Fatal(err) }
  for _, detector := range All() {
   if detector.Spec().Class != sdk.DetectorClassIncident { continue }
   linked := false
   for _, id := range detector.Spec().Playbooks { linked = linked || programIDs[id] }
   if !linked { continue }
   for _, phase := range []struct{name string; snapshot sdktest.Snapshot}{
    {"open", corpus.Open}, {"fixed", corpus.Fixed}, {"healthy", corpus.Healthy},
   } {
    // Snapshot.ObservedView uses the same checked SDK
    // ReadView as production, including phase-bound recorded primitive reads.
    snapshot := phase.snapshot
    snapshot.Readings = append(append([]sdktest.PrimitiveCall(nil), snapshot.Readings...), corpus.PhaseRecordings(phase.name)...)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    _, err := sdk.DetectObserved(ctx, detector, snapshot)
    cancel()
    if err != nil {
     t.Errorf("detector.corpus-observation: detector=%s case=%s phase=%s: %v", detector.Spec().ID, filepath.Base(path), phase.name, err)
    }
   }
  }
 }
}
`)
	return os.WriteFile(filepath.Join(root, "generated", "detector_corpus_test.go"), []byte(source.String()), 0644)
}
